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
| EKS | Cluster with the VPC CNI (NetworkPolicy enforcement on), kube-proxy and CoreDNS add-ons. Kubernetes secrets are encrypted with a KMS key. One managed node group: 2 x `r7g.xlarge` (Graviton, 4 vCPU, 32 GiB), 150 GiB encrypted gp3 root volume each, Amazon Linux 2023. |
| S3 | Private bucket for Iceberg data, tables under `s3://<bucket>/warehouse`. All public access blocked, SSE-S3 encryption (or your KMS key), a policy that rejects requests without TLS, a lifecycle rule that removes abandoned multipart uploads. Versioning optional. |
| RDS | PostgreSQL 17, `db.t4g.small`, 20 GiB gp3 (autoscaling to 100 GiB), encrypted, private, reachable only from the cluster, TLS required, 7 days of backups, deletion protection on. One database, `igloo`, holds the catalog (schema `public`) and Igloo's users (schema `igloo_auth`). |
| IAM | Two roles for service accounts (IRSA): one for the igloo pods and one for the Lakekeeper catalog. Each can be assumed only by its own Kubernetes service account and can only list, read, write and delete objects under `warehouse/` in the bucket. A third, `<name_prefix>-engines`, with the same access, can be assumed only by the igloo role: Igloo assumes it, with a session policy for one table, to hand [other engines](../../docs/external-engines.md) credentials for that table for an hour. |
| Kubernetes | Namespace `igloo`, Secrets `igloo-admin` (key `password`), `igloo-database` (key `url`), `igloo-catalog-encryption` (key `encryption-key`), and `igloo-tls` when TLS is on. |
| Helm | The Igloo chart: 2 igloo pods (1.5 CPU, 12 GiB each), the Lakekeeper catalog, and a ClusterIP SQL service (or a Network Load Balancer with `expose_sql`). |

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
| 2 x r7g.xlarge nodes | $0.2142/hour each | $313 |
| 2 x 150 GiB gp3 node volumes | $0.08/GB-month | $24 |
| NAT gateway + its public IPv4 address | $0.045 + $0.005/hour | $37, plus $0.045/GB through it |
| RDS db.t4g.small, single-AZ, 20 GiB gp3 | $0.032/hour + $0.115/GB-month | $26 |
| KMS key, CloudWatch control-plane logs | | $2 to $10 |
| **Total** | | **about $475** |
| S3 storage and requests | $0.023/GB-month, $0.005 per 1,000 writes | depends on data |
| With `expose_sql`: NLB + 3 public IPv4 addresses | $0.0225/hour + capacity units | about $30 more |

Ways to spend less:

- `node_use_spot = true` usually cuts the node line by 60 to 70%. Spot nodes
  can be reclaimed with two minutes' notice, which ends queries running on
  them. List several types, e.g.
  `node_instance_types = ["r7g.xlarge", "r6g.xlarge", "r8g.xlarge"]`.
- `node_instance_types = ["m7g.xlarge"]` (16 GiB) with `igloo_memory = "6Gi"`
  costs about $75 less, at half the memory per query engine.
- Keep `db_multi_az = false` (the default) unless you need a database standby.

Why these defaults: DuckDB's speed depends mostly on memory, so the nodes are
memory-optimized Graviton instances, which cost less than x86 for the same
work. Each node fits two igloo pods (1.5 CPU requested, 12 GiB), so two nodes
run the two replicas with room for a rolling update. Pods have no CPU limit
and use idle cores. DuckDB gets about 80% of the pod's memory and spills
larger queries to the node's disk (`duckdb_spill_size`, 50 GiB per pod).

No cluster autoscaler is installed. To add capacity, raise `node_min_size`
(`node_desired_size` is ignored after the node group exists) or scale the node
group in the EKS console, and raise `igloo_replicas`.

## Common settings

| Variable | Default | Purpose |
|---|---|---|
| `region`, `name_prefix` | `us-east-1`, `igloo` | Where, and the prefix for every resource name |
| `igloo_image_tag` | chart appVersion | Igloo version |
| `igloo_image_repository` | chart default | Igloo image repository, for a mirror or private registry |
| `igloo_replicas`, `igloo_cpu_request`, `igloo_memory` | `2`, `1500m`, `12Gi` | Query engine pods |
| `duckdb_memory_limit`, `duckdb_threads`, `duckdb_spill_size` | 80% of pod, cores, `50Gi` | DuckDB tuning |
| `node_instance_types`, `node_use_spot`, `node_min_size`/`node_max_size`/`node_desired_size`, `node_disk_size_gib` | `["r7g.xlarge"]`, `false`, 2/4/2, 150 | Nodes |
| `db_instance_class`, `db_multi_az`, `db_backup_retention_days`, `db_deletion_protection` | `db.t4g.small`, `false`, 7, `true` | Database |
| `bucket_name`, `bucket_versioning`, `bucket_kms_key_arn`, `bucket_force_destroy` | generated, `false`, SSE-S3, `false` | Table bucket |
| `expose_sql`, `sql_allowed_cidrs`, `sql_load_balancer_internal` | `false`, none, `false` | SQL access from outside the cluster |
| `cluster_endpoint_public_access_cidrs`, `cluster_admin_arns` | `["0.0.0.0/0"]`, none | Kubernetes API access |
| `allowed_locations`, `igloo_additional_policy_arns` | none | Let users `COPY` from or to other buckets: list the `s3://` prefixes and attach an IAM policy granting the igloo role access to them |
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
exposed, and bad input is rejected.
