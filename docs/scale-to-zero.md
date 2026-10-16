# Scale to zero

Igloo can stop its compute when nobody uses it and start it again when a
client connects, so idle hours cost only a disk and a very small machine.
This is the same trade Snowflake's `AUTO_SUSPEND` and `AUTO_RESUME` make: the
first query after a quiet period waits while the compute starts.

```
 psql / JDBC / BI tool
          │
          ▼
 ┌──────────────────┐   start / stop    ┌──────────────────────────────┐
 │  igloo gateway   │ ────────────────▶ │ EC2 instance, VM or pods     │
 │  (always on,     │   SQL bytes       │ running Igloo (and, on one   │
 │   tiny)          │ ◀───────────────▶ │ machine, its catalog)        │
 └──────────────────┘                   └──────────────────────────────┘
```

The gateway is the `igloo gateway` command of the usual Igloo image.
Clients connect to it instead of to Igloo, and so can browsers using the
[web console](console.md):

- When a client connects and Igloo is stopped, the gateway starts the
  compute and holds the connection until Igloo accepts it. The client sees a
  slow login, nothing else.
- Once connected, the gateway copies bytes in both directions. TLS and
  passwords stay end to end between the client and Igloo; the gateway never
  sees them.
- When no client has sent or received anything for `IGLOO_GATEWAY_SUSPEND_AFTER`
  (10 minutes by default) and Igloo reports no running statements or table
  maintenance, the gateway stops the compute.
- With `IGLOO_GATEWAY_CONSOLE_BACKEND` set, the gateway forwards the web
  console's port the same way, on a second listener: opening the console
  starts Igloo, and its requests count as traffic. The console sends nothing
  while nobody uses it, so a browser tab left open does not keep Igloo
  running.

**Open sessions are closed when Igloo stops**, as they would be by a database
restart. Interactive clients reconnect on their next query, which starts
Igloo again. Connection pools should check connections before use
(`pool_pre_ping` in SQLAlchemy, `check` in psycopg's pool,
`testOnBorrow` or a validation query in JDBC pools).

## When to use it

Use it when Igloo sits idle for long stretches: overnight, on weekends, or
between scheduled jobs. A machine that is busy most of the day costs less
running all the time, and its first query of the day is not slowed by a
start.

How long a start takes depends on the platform. In the
[benchmark](../bench/README.md), a stopped c7g.4xlarge with Igloo's
single-machine stack (Igloo, its catalog and local storage under Docker
Compose) took about 16 seconds from the first connection to the first
query. Kubernetes pods on a node that is already running start in a few
seconds; a cluster that must first add a node takes a minute or two.

The first queries after a start are slower for a second reason: memory
starts empty, and table files fetched from object storage are slow to read
(31 to 59 seconds for TPC-H at scale factor 10 from Amazon S3, against 5.5
seconds from memory). Igloo keeps the files it has read in a
[read cache](operations.md#read-cache) on local disk, which survives the
stop, so after a start it reads them from the machine's disk instead of
object storage. That works only on storage that outlives the compute: the
EBS volume of a stopped EC2 instance (Docker Compose keeps the cache in the
`igloo-cache` volume there), or a PersistentVolumeClaim on Kubernetes.
Instance-store disks, `emptyDir` volumes and the disks of nodes the
autoscaler removes start empty. The [read cache](operations.md#read-cache)
section says how to watch and clear it.

## On one AWS machine

This is the setup the benchmark measures: Igloo's single-machine stack on
one EC2 instance, and the gateway on a `t4g.nano` (about $3 a month) in the
same VPC.

1. **Let the gateway start and stop the Igloo instance.** Give the gateway
   machine an instance profile with this policy, with your instance's ARN:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {"Effect": "Allow", "Action": ["ec2:StartInstances", "ec2:StopInstances"],
        "Resource": "arn:aws:ec2:REGION:ACCOUNT:instance/INSTANCE_ID"},
       {"Effect": "Allow", "Action": "ec2:DescribeInstances", "Resource": "*"}
     ]
   }
   ```

2. **Let the gateway reach Igloo.** In `deploy/single-vm/.env` on the Igloo
   instance, publish the SQL and HTTP ports on the private network, then
   restart the stack (`make up`):

   ```sh
   IGLOO_BIND=0.0.0.0
   IGLOO_HTTP_BIND=0.0.0.0
   IGLOO_CONSOLE_BIND=0.0.0.0   # to use the web console through the gateway
   ```

   Allow the gateway's security group to reach the instance on
   `IGLOO_PORT` (5433), `IGLOO_HTTP_PORT` (8432) and, for the console,
   `IGLOO_CONSOLE_PORT` (8443), and nothing else. The stack restarts by
   itself when the instance boots. The console accepts sign-ins from other
   machines only over TLS, so set up TLS (see `.env`) to use it through the
   gateway; the gateway passes TLS through untouched.

3. **Run the gateway** on the small machine, with the Igloo image:

   ```sh
   docker run -d --name igloo-gateway --restart unless-stopped --network host \
     -e IGLOO_GATEWAY_BACKEND=10.0.1.23:5433 \
     -e IGLOO_GATEWAY_ACTIVITY_URL=http://10.0.1.23:8432/activity \
     -e IGLOO_GATEWAY_COMPUTE=ec2 \
     -e IGLOO_GATEWAY_EC2_INSTANCE_IDS=i-0123456789abcdef0 \
     -e IGLOO_GATEWAY_CONSOLE_BACKEND=10.0.1.23:8443 \
     -e AWS_REGION=us-east-1 \
     ghcr.io/akkio-inc/igloo:VERSION gateway
   ```

   Use the instance's private IP address, which survives stops and starts.
   Point clients at the gateway machine's port 5432, and browsers at its
   port 8443. Leave out `IGLOO_GATEWAY_CONSOLE_BACKEND` to forward SQL only.

On a machine with 512 MB of memory, package installs can run out of memory;
add a swap file first, or use a `t4g.micro`.

## On Google Cloud or Azure VMs

The `command` driver runs a shell command to start or stop the compute. Put
the cloud's CLI in an image based on the Igloo image, or run the gateway
binary next to it, and give the gateway machine an identity that may start
and stop the Igloo VM:

```sh
# Google Cloud
IGLOO_GATEWAY_COMPUTE=command
IGLOO_GATEWAY_START_COMMAND='gcloud compute instances start igloo --zone us-central1-a'
IGLOO_GATEWAY_STOP_COMMAND='gcloud compute instances stop igloo --zone us-central1-a --async'

# Azure: deallocate, or the VM is still billed while stopped
IGLOO_GATEWAY_COMPUTE=command
IGLOO_GATEWAY_START_COMMAND='az vm start -g igloo -n igloo --no-wait'
IGLOO_GATEWAY_STOP_COMMAND='az vm deallocate -g igloo -n igloo --no-wait'
```

Commands must succeed when the VM is already in the state they ask for. The
gateway waits for Igloo's port itself, so start commands need not wait.

## On Kubernetes

The [Helm chart](../deploy/helm/igloo/README.md#scale-to-zero) sets up the
gateway with one setting:

```yaml
scaleToZero:
  enabled: true
  suspendAfter: 10m   # the default
```

The gateway then runs as its own Deployment with one replica, and the SQL
Service clients already use leads to it. Its `kubernetes` driver scales
Igloo's Deployment between zero and `scaleToZero.replicas` (by default
`replicaCount`) through the Kubernetes API, with a service account that may
only get and patch that Deployment's `scale` subresource. The cluster
autoscaler (or Karpenter) then removes nodes that only ran Igloo, which is
where the saving comes from: give Igloo its own node pool with a minimum
size of zero (`nodeSelector`, `tolerations`), and keep the gateway on an
always-on one (`scaleToZero.nodeSelector`, `scaleToZero.tolerations`).

The OpenTofu modules for AWS, Google Cloud and Azure do not set this node
pool up yet: they run Igloo on their general node pool, so with
`scaleToZero.enabled` the Igloo pods stop but the nodes keep running (the AWS
module installs no cluster autoscaler at all). Until they do, the single-VM
setup above is the one where scale to zero saves the machine's cost.

The gateway reaches Igloo through a second Service, and asks every Igloo
pod for running work through a headless Service on port 8080, so a
statement running on any replica keeps them all up. If Igloo also has a
HorizontalPodAutoscaler (`autoscaling.enabled`), it pauses at zero replicas
and takes over again after a start.

Pods scaled to zero lose whatever their containers held, including the
[read cache](operations.md#read-cache) unless `/var/lib/igloo/cache` is on a
PersistentVolumeClaim, so the first queries after a start read every table
file from object storage again.

Without the chart, set up the same pieces yourself, following its
`gateway.yaml` and `service.yaml` templates: `IGLOO_GATEWAY_BACKEND` pointing
at a Service for the Igloo pods (and `IGLOO_GATEWAY_CONSOLE_BACKEND` at their
console port, 8443, to forward the [web console](console.md) too),
`IGLOO_GATEWAY_ACTIVITY_URL` at a headless one (`clusterIP: None`), and those
permissions for the gateway's service account. Leave `replicas` out of Igloo's Deployment, so that applying it
again does not undo the gateway's scaling.

## Settings

| Variable | Default | Meaning |
|---|---|---|
| `IGLOO_GATEWAY_BACKEND` | (required) | Igloo's SQL address, `host:port` |
| `IGLOO_GATEWAY_COMPUTE` | (required) | `ec2`, `kubernetes` or `command` |
| `IGLOO_GATEWAY_SUSPEND_AFTER` | `10m` | Stop Igloo after this long without client traffic or running work |
| `IGLOO_GATEWAY_RESUME_TIMEOUT` | `5m` | How long a connecting client waits for Igloo to start before the gateway gives up and closes the connection |
| `IGLOO_GATEWAY_ACTIVITY_URL` | `http://BACKEND_HOST:8080/activity` | Igloo's activity endpoint. Set it empty to judge idleness by traffic alone, which can stop Igloo during a long statement that sends nothing. |
| `IGLOO_GATEWAY_LISTEN_ADDR` | `:5432` | Where clients connect |
| `IGLOO_GATEWAY_CONSOLE_BACKEND` | | Igloo's web console address, `host:port` (port 8443). Set it to forward the console too. |
| `IGLOO_GATEWAY_CONSOLE_LISTEN_ADDR` | `:8443` | Where browsers connect for the console, when it is forwarded |
| `IGLOO_GATEWAY_HTTP_ADDR` | `:8080` | `GET /healthz` and `GET /metrics`. Empty disables it. |
| `IGLOO_GATEWAY_EC2_INSTANCE_IDS` | | `ec2`: the instances to start and stop, comma separated. Credentials and region come from the usual AWS sources. |
| `IGLOO_GATEWAY_K8S_DEPLOYMENT` | | `kubernetes`: Igloo's Deployment |
| `IGLOO_GATEWAY_K8S_NAMESPACE` | the gateway's own | `kubernetes`: its namespace |
| `IGLOO_GATEWAY_K8S_REPLICAS` | `1` | `kubernetes`: replicas to start |
| `IGLOO_GATEWAY_START_COMMAND`, `IGLOO_GATEWAY_STOP_COMMAND` | | `command`: shell commands that start and stop the compute |
| `IGLOO_LOG_FORMAT`, `IGLOO_LOG_LEVEL` | `text`, `info` | As for the server |

Igloo's `GET /activity` on port 8080 answers
`{"running": 0, "idle_seconds": 312.5}`: the statements and maintenance
passes running now, and the time since one last started or ended.

## Metrics

| Metric | Labels | Meaning |
|---|---|---|
| `igloo_gateway_compute_running` | | 1 while Igloo's compute is known to be running |
| `igloo_gateway_connections` | | Open client connections, SQL and console |
| `igloo_gateway_connections_total` | | Client connections accepted, SQL and console |
| `igloo_gateway_resumes_total` | `result` = `success`, `failure` | Starts of the compute |
| `igloo_gateway_resume_seconds` | | Time from asking the compute to start until Igloo accepted connections |
| `igloo_gateway_suspends_total` | | Stops for idleness |

Alert on `igloo_gateway_resumes_total{result="failure"}`: clients cannot
connect while starts fail.

## Table maintenance

Igloo maintains tables (compaction, snapshot expiry, orphan cleanup) while it
runs, and the gateway does not stop it during a maintenance pass. Replicas
record each pass in the metadata database, so after a start Igloo knows
whether one is due: if no pass has finished within
`IGLOO_MAINTENANCE_INTERVAL` (an hour by default), one begins within about
ten seconds and keeps Igloo up until it finishes. A warehouse used in short
bursts is therefore maintained about once an interval, at the cost of
keeping one burst an interval a little longer.
