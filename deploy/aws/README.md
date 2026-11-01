# Igloo on AWS

This OpenTofu configuration deploys a complete Igloo installation into an AWS
account with one `tofu apply`. It creates the network, an EKS cluster, an S3
bucket for table data and an RDS PostgreSQL database, then installs the Igloo
Helm chart (`deploy/helm/igloo`) with the right settings for AWS.

It is a root configuration: run it directly rather than calling it as a
module from other code, because it configures its own providers.

## What gets created

| Component | Details |
|---|---|
| VPC | New VPC (`10.0.0.0/16` by default) across up to three availability zones: public subnets (NAT gateway, load balancer), private subnets (nodes), database subnets (RDS). One NAT gateway by default. A free S3 gateway endpoint keeps table reads and writes off the NAT gateway. |
| EKS | Cluster with the VPC CNI (NetworkPolicy enforcement on), kube-proxy and CoreDNS add-ons. Kubernetes secrets are encrypted with a KMS key. One managed node group: 2 x `r7gd.xlarge` (Graviton, 4 vCPU, 32 GiB, 237 GB local NVMe SSD), 150 GiB encrypted gp3 root volume each, Amazon Linux 2023. The NVMe instance store holds kubelet's and containerd's directories, and with them DuckDB's spill files ([Spill on local NVMe](#spill-on-local-nvme)). |
| S3 | Private bucket for Iceberg data, tables under `s3://<bucket>/warehouse`. All public access blocked, SSE-S3 encryption (or your KMS key), a policy that rejects requests without TLS, a lifecycle rule that removes abandoned multipart uploads. Versioning optional. |
| RDS | PostgreSQL 17, `db.t4g.small`, 20 GiB gp3 (autoscaling to 100 GiB), encrypted, private, reachable only from the cluster, TLS required, 7 days of backups, deletion protection on. One database, `igloo`, holds the catalog (schema `public`) and Igloo's users (schema `igloo_auth`). |
| IAM | Two roles for service accounts (IRSA): one for the igloo pods and one for the Lakekeeper catalog. Each can be assumed only by its own Kubernetes service account and can only list, read, write and delete objects under `warehouse/` in the bucket. A third, `<name_prefix>-engines`, with the same access, can be assumed only by the igloo role: Igloo assumes it, with a session policy for one table, to hand [other engines](../../docs/external-engines.md) credentials for that table for an hour. |
| Kubernetes | Namespace `igloo`, Secrets `igloo-admin` (key `password`), `igloo-database` (key `url`), `igloo-catalog-encryption` (key `encryption-key`), and `igloo-tls` when TLS is on. |
| Helm | The Igloo chart: 2 igloo pods (1.5 CPU, 12 GiB each), the Lakekeeper catalog, and a ClusterIP SQL service (or a Network Load Balancer with `expose_sql`). |
| Workers (optional) | With `workers_enabled`: node groups for [distributed-execution workers](#distributed-execution-workers), Spot with an on-demand fallback, from zero nodes, the Kubernetes cluster autoscaler to scale them, and an IAM role for it; with `large_worker_enabled`, a node group for one large worker too. Workers get an IAM role of their own: read the warehouse, write `_igloo_shuffle/` only. They write tables' files with credentials Igloo makes from the engines role for one task's directory ([security](../../docs/distributed-execution.md#security)). |

## Prerequisites

- An AWS account and credentials allowed to create VPC, EKS, EC2, IAM, KMS,
  S3, RDS and CloudWatch Logs resources.
- [OpenTofu](https://opentofu.org/docs/intro/install/) 1.9 or later.
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) v2.
  The Kubernetes and Helm providers call `aws eks get-token`, so the CLI must
  be on your `PATH` and resolve the same identity OpenTofu uses. Set
  `AWS_PROFILE` (or the usual `AWS_*` variables) in the shell rather than in a
  provider block.
- [kubectl](https://kubernetes.io/docs/tasks/tools/) to connect afterwards.
  [Helm](https://helm.sh/docs/intro/install/) is optional, for inspecting the
  release.
- The whole repository checked out: the chart is read from `../helm/igloo`.

## Quick start

```sh
cd deploy/aws
cp terraform.tfvars.example terraform.tfvars   # edit region, name_prefix, ...
tofu init
tofu apply
```

The first apply takes about 20 to 25 minutes, mostly waiting for EKS and RDS.
A minimal `terraform.tfvars`:

```hcl
region      = "eu-west-1"
name_prefix = "igloo"

# Your public address, so only you can reach the Kubernetes API.
cluster_endpoint_public_access_cidrs = ["203.0.113.10/32"]
```

When it finishes, `tofu output next_steps` prints the commands below with
your values filled in.

## Connecting

Point kubectl at the cluster:

```sh
aws eks update-kubeconfig --region <region> --name <name_prefix>
```

Other people need an access entry first; add their IAM role ARNs to
`cluster_admin_arns` and apply.

### Admin password

The administrator is `igloo` (change with `admin_user`). Its password is
generated and stored in a Kubernetes Secret:

```sh
kubectl -n igloo get secret igloo-admin -o jsonpath='{.data.password}' | base64 -d
```

The Secret is only read when Igloo first creates the account. After that,
change the password with `ALTER USER`; changing the Secret has no effect.

### Through kubectl port-forward (default)

By default the SQL port is not reachable from outside the cluster.

```sh
kubectl -n igloo port-forward svc/igloo 5433:5432
psql "host=127.0.0.1 port=5433 user=igloo dbname=igloo"
```

### Other engines

Spark, Trino, PyIceberg and other engines use the tables through Igloo's
[Iceberg REST catalog](../../docs/external-engines.md) on port 8181 of the same
Service: `http://igloo.igloo.svc:8181` inside the cluster, or through
`kubectl -n igloo port-forward svc/igloo 8181:8181`, or on the load balancer
below (over TLS). They sign in as Igloo users and get credentials for one table
at a time from the `<name_prefix>-engines` role; nothing else is needed on
AWS.

### Through a load balancer (`expose_sql = true`)

```hcl
expose_sql        = true
sql_allowed_cidrs = ["203.0.113.0/24"]   # required; there is no default
```

This makes the SQL service a Network Load Balancer created by EKS's built-in
AWS cloud controller. Only `sql_allowed_cidrs` can connect; the controller
enforces this with security group rules on the nodes. Set
`sql_load_balancer_internal = true` for a load balancer reachable only from
the VPC and networks connected to it.

Exposing SQL also turns on TLS. Without your own certificate the module
generates a self-signed one: traffic is encrypted, but clients cannot verify
the server, so use `sslmode=require` (not `verify-full`). To use a real
certificate for a DNS name you point at the load balancer, set
`tls_certificate_pem` and `tls_private_key_pem`.

```sh
psql "host=$(tofu output -raw sql_endpoint) port=5432 user=igloo dbname=igloo sslmode=require"
```

The load balancer's DNS name can take a few minutes to resolve after the
first apply.

## Sizing and cost

Approximate monthly cost of the defaults in us-east-1 at on-demand list
prices, before data transfer and S3 usage. Other regions differ; check the
[AWS Pricing Calculator](https://calculator.aws/) before relying on these.

| Item | Rate | Per month |
|---|---|---|
| EKS control plane | $0.10/hour | $73 |
| 2 x r7gd.xlarge nodes | $0.2722/hour each | $397 |
| 2 x 150 GiB gp3 node volumes | $0.08/GB-month | $24 |
| NAT gateway + its public IPv4 address | $0.045 + $0.005/hour | $37, plus $0.045/GB through it |
| RDS db.t4g.small, single-AZ, 20 GiB gp3 | $0.032/hour + $0.115/GB-month | $26 |
| KMS key, CloudWatch control-plane logs | | $2 to $10 |
| **Total** | | **about $560** |
| S3 storage and requests | $0.023/GB-month, $0.005 per 1,000 writes | depends on data |
| With `expose_sql`: NLB + 3 public IPv4 addresses | $0.0225/hour + capacity units | about $30 more |

Ways to spend less:

- `node_use_spot = true` usually cuts the node line by 60 to 70%. Spot nodes
  can be reclaimed with two minutes' notice, which ends queries running on
  them. List several types, e.g.
  `node_instance_types = ["r7gd.xlarge", "r6gd.xlarge", "r8gd.xlarge"]`.
- `node_instance_types = ["r7g.xlarge"]`, the same machine without the
  instance store, costs about $85 less, and DuckDB then spills to the gp3
  root volume, which is far slower (see below).
- `node_instance_types = ["m7gd.xlarge"]` (16 GiB) with `igloo_memory = "6Gi"`
  also costs about $85 less, at half the memory per query engine.
- With instance store, `node_disk_size_gib = 30` saves about $19: the root
  volume then holds only the operating system.
- Keep `db_multi_az = false` (the default) unless you need a database standby.
- For what runs around the clock, the nodes of a pool that does not scale to
  zero and the RDS database, a one- or three-year commitment lowers the
  price: a [Compute Savings Plan](https://aws.amazon.com/savingsplans/compute-pricing/)
  (any instance type, region and size; AWS quotes up to 66% off) or an EC2
  Instance Savings Plan (one instance family in one region, up to 72% off)
  for the nodes, and an RDS reserved instance for the database. Commit to
  the steady part only, the replicas that always run, and leave what
  scales with load, workers and burst nodes, on-demand or Spot.

Why these defaults: DuckDB's speed depends mostly on memory, so the nodes are
memory-optimized Graviton instances, which cost less than x86 for the same
work. Each node fits two igloo pods (1.5 CPU requested, 12 GiB), so two nodes
run the two replicas with room for a rolling update. Pods have no CPU limit
and use idle cores. DuckDB gets about 80% of the pod's memory and spills
larger queries to the node's disk (`duckdb_spill_size`, 50 GiB per pod),
which is why the nodes are the `d` variant with a local NVMe disk.

The default node group is sized by hand: the cluster autoscaler that
`workers_enabled` installs scales only the worker node groups. To add
capacity, raise `node_min_size` (`node_desired_size` is ignored after the
node group exists) or scale the node group in the EKS console, and raise
`igloo_replicas`.

### Spill on local NVMe

Queries larger than memory (big joins, aggregations and sorts) write and read
back DuckDB's spill files, tens to hundreds of GiB for heavy reports, so the
disk under the spill directory sets their speed. The gp3 root volume gives
3,000 IOPS and 125 MB/s unless you pay for more; the instance store of an
`r7gd.xlarge` is a 237 GB NVMe SSD on the host, rated at about 67,000 read
and 33,000 write IOPS, and costs nothing extra beyond the instance.

When any type in `node_instance_types` has an instance store (the `d` types
such as `r7gd`, `m7gd`, `c7gd`, `r8gd`, and storage-optimized types such as
`i4i`), the module adds a nodeadm `NodeConfig` to the node group's user data:

```yaml
apiVersion: node.eks.aws/v1alpha1
kind: NodeConfig
spec:
  instance:
    localStorage:
      strategy: RAID0
```

At boot, nodeadm puts the instance store disks in a RAID0 array, mounts it
under `/mnt/k8s-disks/0`, and bind-mounts `/var/lib/kubelet`,
`/var/lib/containerd` and `/var/log/pods` onto it. Every `emptyDir`,
including the chart's spill volume at `/var/lib/igloo/tmp`, is then on NVMe,
and the node's allocatable ephemeral storage is the array's size. Nodes of a
listed type without instance store skip this, and spill to the root volume.
`tofu output node_group` shows `local_storage = "RAID0"` when it is on.

The instance store is emptied when an instance stops or is replaced; spill
files only live for a query, so nothing is lost. It cannot be resized: the
igloo pods on a node share it with the container images. On an
`r7gd.xlarge` (237 GB), two pods with the default 50 GiB spill limit fit with
room to spare. For reports that spill around 100 GiB, set
`duckdb_spill_size = "120Gi"` and use `r7gd.2xlarge` (474 GB, 64 GiB of
memory), which holds two such pods. DuckDB stops at 90% of
`duckdb_spill_size`, so a query that needs more fails instead of filling the
disk.

Types without instance store (`node_instance_types = ["r7g.xlarge"]`) keep
the node group's user data empty, as before, and spill to the gp3 root
volume: leave `node_disk_size_gib` room for `duckdb_spill_size` times the
igloo pods per node.

## Distributed-execution workers

`workers_enabled = true` runs statements that read very large tables in
parts on worker pods ([distributed execution](../../docs/distributed-execution.md)).
It is off by default, so applying this version to an existing deployment
changes nothing. With it on, the module adds:

| Node group | Capacity | Size | Runs |
|---|---|---|---|
| `workers-spot` (unless `workers_spot = false`) | Spot, any of `workers_instance_types` (`m7gd.4xlarge`, `m6gd.4xlarge`: 16 Graviton vCPUs, 64 GiB, 950 GB NVMe) | 0 to `workers_max` (16) | worker pods |
| `workers-ondemand` | On-demand, the first type | 0 to `workers_max` | worker pods, when Spot has no capacity |
| `large-worker-ondemand` (with `large_worker_enabled`) | On-demand, `r7gd.16xlarge` (64 vCPUs, 512 GiB, 2 x 1900 GB NVMe) | 0 to 1 | the large worker |
| `large-worker-spot` (with `large_worker_spot` too) | Spot | 0 to 1 | the large worker, when Spot has capacity |

The worker groups carry the node label `igloo.dev/node=workers` (the large
worker's, `large-worker`) and a `NoSchedule` taint, `igloo.dev/workers`
(`igloo.dev/large-worker`), so only worker pods run there; the chart's
worker pods select the label and tolerate the taint, which both groups of a
kind share. Igloo pods, Lakekeeper and system pods stay on the default node
group, on-demand unless `node_use_spot`: igloo pods hold client sessions.
Sixteen workers take 256 vCPUs at once where a statement is worth it, and nothing
when idle: check your account's quotas for Standard Spot and On-Demand instances (in
Service Quotas, `L-34B43A08` and `L-1216C47A`), or lower `workers_max`. A node
AWS refuses leaves its worker unstarted, and the statement runs on fewer, or on
an igloo pod when `workers.startTimeout` runs out.

One worker fills a node: its pod requests 90% of the first type's vCPUs and
85% of its memory (`workers_cpu`, `workers_memory` override that), and its
spill and read cache volumes take 55% and 25% of the instance store, which
the module RAID0s under kubelet as on the default nodes; without instance
store, half and a fifth of the root volume (`workers_disk_size_gib`).

Igloo scales the worker Deployment up when a statement needs workers and to
zero when they are idle. The [Kubernetes cluster
autoscaler](https://github.com/kubernetes/autoscaler/tree/master/cluster-autoscaler)
(Helm chart `cluster-autoscaler`, `cluster_autoscaler_chart_version`, in
`kube-system`, with an IAM role for its service account that may resize
only the worker groups' autoscaling groups) adds nodes for waiting worker
pods and removes them five minutes after they are empty. Its priority
expander tries the Spot groups first. When AWS has no Spot capacity for the
listed types, the autoscaler usually sees the failed launch in the
autoscaling group's activities and backs the Spot group off at once; at the
latest it does so when no node has joined within
`--max-node-provision-time` (5 minutes). The on-demand group then gets the
pod, and the Spot group is tried again after the back-off. A statement
waits for workers at most `workers.startTimeout` (3 minutes) and then runs
on its igloo pod, so a statement that meets a Spot shortage may run without
workers; the next one gets the on-demand nodes. EKS managed node groups
drain a Spot node once AWS sends a rebalance recommendation for it, which
usually comes before the two-minute notice, and a stopping worker finishes
its tasks for up to 30 seconds; what it has not finished runs again on
other workers. (A worker reads the two-minute notice from instance metadata
itself only with a hop limit of 2, which these node groups keep at 1, so
pods cannot take the node's credentials.)

Why the cluster autoscaler rather than Karpenter: it scales the managed node
groups this module already uses, needs only a Helm chart and one IAM role,
and no custom resources, which the Kubernetes provider cannot plan before
the cluster exists, so one `tofu apply` still creates everything. Karpenter
falls back from Spot sooner and picks instance types more freely; install it
yourself with `workers_enabled = false` and the chart's `workers.*` values
through `helm_values_override` if you want that.

The large worker (`large_worker_enabled = true`) is one pod on a 512 GiB
node, which auto mode may run a whole statement on instead of on many small
workers (see the chart's [large worker](../helm/igloo/README.md#a-large-worker)).
It is on-demand by default (`large_worker_spot = true` tries Spot first),
and costs nothing while stopped.

Set `workers_price_per_hour` and `large_worker_price_per_hour` to what a
node costs, so auto mode can weigh price: in us-east-1, an `r7gd.16xlarge`
lists at about $4.36 an hour on-demand, an `m7gd.4xlarge` at about $0.85,
and Spot runs 50 to 70% below on-demand, varying by zone and hour. Left at
0, the price is unknown.

## Common settings

| Variable | Default | Purpose |
|---|---|---|
| `region`, `name_prefix` | `us-east-1`, `igloo` | Where, and the prefix for every resource name |
| `igloo_image_tag` | chart appVersion | Igloo version |
| `igloo_image_repository` | chart default | Igloo image repository, for a mirror or private registry |
| `igloo_replicas`, `igloo_cpu_request`, `igloo_memory` | `2`, `1500m`, `12Gi` | Query engine pods |
| `duckdb_memory_limit`, `duckdb_threads`, `duckdb_spill_size` | 80% of pod, 2 per core, `50Gi` | DuckDB tuning |
| `node_instance_types`, `node_use_spot`, `node_min_size`/`node_max_size`/`node_desired_size`, `node_disk_size_gib` | `["r7gd.xlarge"]`, `false`, 2/4/2, 150 | Nodes; spill goes to the instance store of types that have one |
| `db_instance_class`, `db_multi_az`, `db_backup_retention_days`, `db_deletion_protection` | `db.t4g.small`, `false`, 7, `true` | Database |
| `bucket_name`, `bucket_versioning`, `bucket_kms_key_arn`, `bucket_force_destroy` | generated, `false`, SSE-S3, `false` | Table bucket |
| `expose_sql`, `sql_allowed_cidrs`, `sql_load_balancer_internal` | `false`, none, `false` | SQL access from outside the cluster |
| `cluster_endpoint_public_access_cidrs`, `cluster_admin_arns` | `["0.0.0.0/0"]`, none | Kubernetes API access |
| `allowed_locations`, `igloo_additional_policy_arns` | none | Let users `COPY` from or to other buckets: list the `s3://` prefixes and attach an IAM policy granting the igloo role access to them |
| `workers_enabled`, `workers_spot`, `workers_instance_types`, `workers_max` | `false`, `true`, `["m7gd.4xlarge", "m6gd.4xlarge"]`, 16 | [Distributed-execution workers](#distributed-execution-workers): Spot with an on-demand fallback |
| `workers_cpu`, `workers_memory`, `workers_disk_size_gib`, `workers_price_per_hour` | 90% of vCPUs, 85% of memory, 100, 0 | Worker pod size, node root volume, price for auto mode |
| `large_worker_enabled`, `large_worker_spot`, `large_worker_instance_types`, `large_worker_price_per_hour` | `false`, `false`, `["r7gd.16xlarge"]`, 0 | One large worker, on-demand by default |
| `cluster_autoscaler_chart_version` | `9.59.0` | Cluster autoscaler 1.35, for `kubernetes_version` 1.35; keep the two minor versions together |
| `install_chart` | `true` | `false` creates everything except the Helm release, for installing the chart another way (the namespace and Secrets are still created) |
| `helm_values_override` | empty | Extra chart values as YAML, e.g. node selectors or autoscaling |

See `variables.tf` for every variable and its validation rules.

## Upgrading

- Igloo: change `igloo_image_tag` and apply. The chart rolls the pods.
- Kubernetes: raise `kubernetes_version` one minor version at a time and
  apply; EKS upgrades the control plane, then the node group. With the default
  `eks_support_type = "STANDARD"`, EKS upgrades the control plane itself when
  standard support for the version ends (14 months after release); `EXTENDED`
  postpones that for up to 12 months at $0.50 per cluster-hour more.
- PostgreSQL minor versions upgrade automatically in the maintenance window.

## Tearing down

1. Turn off database deletion protection and apply:
   `tofu apply -var db_deletion_protection=false`
2. Empty the bucket, or apply with `bucket_force_destroy = true` if you mean
   to delete every table.
3. `tofu destroy`

RDS takes a final snapshot named `<name_prefix>-db-final-<suffix>` unless
`db_skip_final_snapshot = true`; delete it yourself when you no longer need
it. The EKS secrets KMS key is scheduled for deletion, not deleted at once.

With `expose_sql`, OpenTofu waits two minutes after removing the Helm release
so EKS can delete the load balancer. If `tofu destroy` still fails deleting
subnets or the VPC because of a leftover load balancer or network interface,
delete the load balancer in the EC2 console and run `tofu destroy` again.

## Security notes

- **State contains secrets.** The RDS password, the Igloo admin password, the
  catalog encryption key and (with TLS) a private key are stored in OpenTofu
  state. Keep state in an encrypted, access-controlled remote backend, not on
  a laptop. A commented S3 backend example is at the bottom of
  `versions.tf`:

  ```hcl
  terraform {
    backend "s3" {
      bucket       = "my-org-tofu-state"
      key          = "igloo/aws/terraform.tfstate"
      region       = "us-east-1"
      encrypt      = true
      kms_key_id   = "arn:aws:kms:us-east-1:111122223333:key/..."
      use_lockfile = true
    }
  }
  ```

  OpenTofu can also encrypt state client-side with its `encryption` block.
- **Losing the catalog encryption key** makes the catalog's stored storage
  credentials unreadable. It is kept in state and in the
  `igloo-catalog-encryption` Secret; do not rotate it by replacing the
  `random_password` resource.
- **Kubernetes API endpoint.** It is public so OpenTofu and kubectl can reach
  it from outside the VPC, and open to `0.0.0.0/0` by default, which is EKS's
  own default. Every request still needs IAM credentials with an EKS access
  entry. Narrow `cluster_endpoint_public_access_cidrs` to your network and
  CI runners.
- **Storage access.** No static AWS keys exist. The igloo pods and the
  catalog get short-lived credentials through IRSA, scoped to the bucket's
  `warehouse/` prefix. Node instance metadata needs IMDSv2 with a hop limit of
  1, so pods cannot borrow the node's role.
- **SQL traffic.** The SQL service is internal unless `expose_sql` is set, and
  exposing it requires an allow-list and turns on TLS. Passwords are checked
  with SCRAM-SHA-256.
- **Database.** RDS is in subnets with no inbound route from the internet,
  accepts connections only from the cluster's security groups, requires TLS
  (`sslmode=require`), and is encrypted at rest. Igloo and the catalog connect
  as the RDS master user; separate least-privilege database users are not set
  up yet.
- **Pod security.** The namespace enforces the Kubernetes `baseline` Pod
  Security Standard and warns on `restricted`. NetworkPolicy enforcement is on
  in the VPC CNI, so the chart's policies apply.

Findings from `trivy config` that are deliberate:

| Check | Why it stays |
|---|---|
| AWS-0040, AWS-0041: public EKS endpoint, open CIDR | Needed for a single `tofu apply` from outside the VPC; restrict with `cluster_endpoint_public_access_cidrs`. |
| AWS-0104: node security group allows all egress | Nodes pull images from ghcr.io and quay.io and call AWS APIs; there is no fixed address list to allow. |
| AWS-0038: controller manager and scheduler logs off | Audit, API and authenticator logs are on; the other two add CloudWatch cost with little security value. |
| AWS-0178: VPC flow logs off | Set `enable_vpc_flow_logs = true`; off by default because ingestion is billed per GB. |
| AWS-0090: bucket versioning off | Set `bucket_versioning = true`. Iceberg keeps its own snapshots; versioning also keeps every deleted data file. |
| AWS-0089: bucket access logging off | Needs a second bucket; use CloudTrail S3 data events if you need an audit trail. |
| AWS-0176: RDS IAM authentication off | Lakekeeper and Igloo authenticate with a password over TLS; IAM tokens would need support in both. |
| AWS-0133: RDS Performance Insights off | Optional monitoring with its own cost and instance-class limits; turn it on in the RDS console if you need it. |

## Testing

The tests in `tests/` plan the configuration with every provider mocked, so
they need no AWS account or credentials:

```sh
tofu init -backend=false
tofu test
```

They check, among other things, that the bucket blocks public access, RDS is
private and encrypted, each IAM role trusts only its own service account and
reaches only the warehouse prefix, only the igloo role may assume the engines
role (whose ARN the chart gets as `storage.s3.stsRoleArn`), the Helm values carry the warehouse
location and `systemCredentials: aws`, the SQL service stays ClusterIP unless
exposed, node types with instance store get the RAID0 `NodeConfig` and others
none, workers get a Spot and an on-demand node group from zero, sized pods
and volumes, and a cluster autoscaler that prefers Spot and may resize only
their autoscaling groups, nothing of that exists without `workers_enabled`,
the large worker gets an on-demand group of its own, and bad input is
rejected.
