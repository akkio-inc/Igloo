{{/* ---------------------------------------------------------------------
Names and labels
--------------------------------------------------------------------- */}}

{{- define "igloo.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Release fullname. Release "igloo" gives "igloo": the OpenTofu modules expect
the SQL Service to be called exactly that.
*/}}
{{- define "igloo.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* "<fullname>-<suffix>", kept within 63 characters. Call with (list . "suffix"). */}}
{{- define "igloo.componentName" -}}
{{- $root := index . 0 -}}
{{- $suffix := index . 1 -}}
{{- $base := include "igloo.fullname" $root | trunc (int (sub 62 (len $suffix))) | trimSuffix "-" -}}
{{- printf "%s-%s" $base $suffix -}}
{{- end -}}

{{- define "igloo.catalogName" -}}{{ include "igloo.componentName" (list . "catalog") }}{{- end -}}
{{- define "igloo.metricsServiceName" -}}{{ include "igloo.poolComponentName" (list . "metrics") }}{{- end -}}
{{- define "igloo.postgresqlName" -}}{{ include "igloo.componentName" (list . "postgresql") }}{{- end -}}
{{- define "igloo.objectStorageName" -}}{{ include "igloo.componentName" (list . "object-storage") }}{{- end -}}
{{/* Scale to zero: the gateway, its metrics, and the two Services it reaches
the igloo pods through (SQL, and every pod's /activity). Each compute pool
has its own (see _pools.tpl). */}}
{{- define "igloo.gatewayName" -}}{{ include "igloo.poolComponentName" (list . "gateway") }}{{- end -}}
{{- define "igloo.gatewayMetricsServiceName" -}}{{ include "igloo.poolComponentName" (list . "gateway-metrics") }}{{- end -}}
{{- define "igloo.serverServiceName" -}}{{ include "igloo.poolComponentName" (list . "server") }}{{- end -}}
{{- define "igloo.headlessServiceName" -}}{{ include "igloo.poolComponentName" (list . "headless") }}{{- end -}}
{{/* The Secret holding everything the chart generates or is given inline. */}}
{{- define "igloo.secretName" -}}{{ include "igloo.fullname" . }}{{- end -}}

{{- define "igloo.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Labels on every object. Call with (dict "root" . "component" "server"). */}}
{{- define "igloo.labels" -}}
helm.sh/chart: {{ include "igloo.chart" .root }}
{{ include "igloo.selectorLabels" . }}
app.kubernetes.io/version: {{ .root.Chart.AppVersion | quote }}
app.kubernetes.io/part-of: igloo
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
{{- end -}}

{{/* Labels that select one component's pods. Immutable once installed. */}}
{{- define "igloo.selectorLabels" -}}
app.kubernetes.io/name: {{ include "igloo.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/* Pod template labels: selector labels plus part-of (not the chart version,
so upgrading the chart alone does not restart pods). */}}
{{- define "igloo.podLabels" -}}
{{ include "igloo.selectorLabels" . }}
app.kubernetes.io/part-of: igloo
{{- end -}}

{{/* ---------------------------------------------------------------------
Images and service accounts
--------------------------------------------------------------------- */}}

{{- define "igloo.image" -}}
{{- $tag := toString (default .Chart.AppVersion .Values.image.tag) -}}
{{- if hasPrefix "sha256:" $tag -}}
{{- printf "%s@%s" .Values.image.repository $tag -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}
{{- end -}}

{{- define "igloo.catalogImage" -}}
{{- printf "%s:%s" .Values.catalog.image.repository (toString .Values.catalog.image.tag) -}}
{{- end -}}

{{/* The workers' service account: their own, or the igloo pods'. */}}
{{- define "igloo.workerServiceAccountName" -}}
{{- if .Values.workers.serviceAccount.create -}}
{{- default (printf "%s-worker" (include "igloo.fullname" .)) .Values.workers.serviceAccount.name -}}
{{- else if .Values.workers.serviceAccount.name -}}
{{- .Values.workers.serviceAccount.name -}}
{{- else -}}
{{- include "igloo.serviceAccountName" . -}}
{{- end -}}
{{- end -}}

{{/* on or off, for IGLOO_WORKER_WRITES: see workers.writes in values.yaml. */}}
{{- define "igloo.workerWrites" -}}
{{- $w := .Values.workers.writes | toString | lower -}}
{{- $own := or .Values.workers.serviceAccount.create .Values.workers.serviceAccount.name .Values.workers.storage.s3.existingSecret .Values.workers.storage.gcs.existingSecret .Values.workers.storage.azure.existingSecret -}}
{{- if or (eq $w "true") (eq $w "on") (and (eq $w "auto") $own) -}}on{{- else if or (eq $w "false") (eq $w "off") (eq $w "auto") -}}off{{- else -}}
{{- fail (printf "workers.writes must be auto, true or false, got %q" $w) -}}
{{- end -}}
{{- end -}}

{{- define "igloo.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "igloo.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "igloo.catalogServiceAccountName" -}}
{{- if .Values.catalog.serviceAccount.create -}}
{{- default (include "igloo.catalogName" .) .Values.catalog.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.catalog.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* Use as: {{- include "igloo.imagePullSecrets" . | nindent 6 }} */}}
{{- define "igloo.imagePullSecrets" -}}
{{- with .Values.imagePullSecrets -}}
imagePullSecrets:
{{- toYaml . | nindent 2 }}
{{- end -}}
{{- end -}}

{{/* ---------------------------------------------------------------------
Storage and databases
--------------------------------------------------------------------- */}}

{{/* IGLOO_WAREHOUSE_LOCATION; empty when nothing is configured. */}}
{{- define "igloo.warehouseLocation" -}}
{{- if .Values.storage.warehouseLocation -}}
{{- .Values.storage.warehouseLocation -}}
{{- else if .Values.objectStorage.enabled -}}
{{- printf "s3://%s/warehouse" .Values.objectStorage.bucket -}}
{{- end -}}
{{- end -}}

{{/* "s3", "gs", "abfss" or "" */}}
{{- define "igloo.warehouseScheme" -}}
{{- $location := include "igloo.warehouseLocation" . -}}
{{- if contains "://" $location -}}
{{- index (splitList "://" $location) 0 -}}
{{- end -}}
{{- end -}}

{{/*
Where each component's database URL comes from, as YAML {name, key} for a
secretKeyRef, or empty when no database is configured. A component's own
setting wins (existingSecret before url); then the bundled PostgreSQL; then the
other component's setting, since both usually share one database.
*/}}
{{- define "igloo.metadataDatabaseRef" -}}
{{- include "igloo.databaseRef" (list . .Values.metadataDatabase "metadata-database-url" .Values.catalog.database "catalog-database-url") -}}
{{- end -}}

{{- define "igloo.catalogDatabaseRef" -}}
{{- include "igloo.databaseRef" (list . .Values.catalog.database "catalog-database-url" .Values.metadataDatabase "metadata-database-url") -}}
{{- end -}}

{{- define "igloo.databaseRef" -}}
{{- $root := index . 0 -}}
{{- $own := index . 1 -}}
{{- $ownKey := index . 2 -}}
{{- $other := index . 3 -}}
{{- $otherKey := index . 4 -}}
{{- $secret := include "igloo.secretName" $root -}}
{{- if $own.existingSecret -}}
name: {{ $own.existingSecret }}
key: url
{{- else if $own.url -}}
name: {{ $secret }}
key: {{ $ownKey }}
{{- else if $root.Values.postgresql.enabled -}}
name: {{ $secret }}
key: database-url
{{- else if $other.existingSecret -}}
name: {{ $other.existingSecret }}
key: url
{{- else if $other.url -}}
name: {{ $secret }}
key: {{ $otherKey }}
{{- end -}}
{{- end -}}

{{/*
DuckDB's memory limit in MiB, of a container's memory limit in MiB (a
float), where none is set, as Igloo gives it (memlimit.DuckDBLimit): what the
memory leaves after an eighth of it (at least 512Mi), which Igloo keeps free
before it lowers DuckDB's limit, and a twentieth in reserve (at least 256Mi),
less an eighth of the limit itself for what DuckDB holds beyond it as it
spills, but at least half of the memory.
*/}}
{{- define "igloo.duckdbShareMiB" -}}
{{- $total := float64 . -}}
{{- $high := maxf (divf $total 8.0) 512.0 -}}
{{- $reserve := maxf (divf $total 20.0) 256.0 -}}
{{- floor (maxf (mulf (subf (subf $total $high) $reserve) (divf 8.0 9.0)) (divf $total 2.0)) -}}
{{- end -}}

{{/*
IGLOO_DUCKDB_MEMORY_LIMIT: duckdb.memoryLimit, or what igloo.duckdbShareMiB
leaves of resources.limits.memory, so DuckDB spills to disk before the
container reaches its limit. (DuckDB allocates beyond its limit for
per-thread state, and the Go runtime, mapped extensions and spill-file page
cache share the container's memory; on a 1Gi pod with 18 cores a large
aggregation peaked at about 190Mi of anonymous memory above DuckDB's limit.)
Empty when neither is set.
*/}}
{{- define "igloo.duckdbMemoryLimit" -}}
{{- if .Values.duckdb.memoryLimit -}}
{{- .Values.duckdb.memoryLimit -}}
{{- else -}}
{{- $limit := "" -}}
{{- with .Values.resources -}}{{- with .limits -}}{{- with .memory -}}
{{- $limit = toString . -}}
{{- end -}}{{- end -}}{{- end -}}
{{- if $limit -}}
{{- $re := "^([0-9]+(?:\\.[0-9]+)?)(|k|M|G|T|Ki|Mi|Gi|Ti)$" -}}
{{- if not (regexMatch $re $limit) -}}
{{- fail (printf "igloo: cannot derive the DuckDB memory limit from resources.limits.memory %q; set duckdb.memoryLimit (e.g. \"6GB\")" $limit) -}}
{{- end -}}
{{- $unitBytes := dict "" "1" "k" "1000" "M" "1000000" "G" "1000000000" "T" "1000000000000" "Ki" "1024" "Mi" "1048576" "Gi" "1073741824" "Ti" "1099511627776" -}}
{{- $number := float64 (regexReplaceAll $re $limit "${1}") -}}
{{- $bytes := mulf $number (float64 (get $unitBytes (regexReplaceAll $re $limit "${2}"))) -}}
{{- $total := divf $bytes 1048576.0 -}}
{{- $mib := float64 (include "igloo.duckdbShareMiB" $total) -}}
{{- if lt $mib 256.0 -}}
{{- fail (printf "igloo: resources.limits.memory %q leaves DuckDB less than 256Mi; use at least 1Gi, or set duckdb.memoryLimit" $limit) -}}
{{- end -}}
{{- printf "%dMiB" (int64 $mib) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
DuckDB's spill cap (IGLOO_DUCKDB_MAX_TEMP_SIZE): duckdb.maxTempSize, or 90% of
the spill volume's sizeLimit so a query that needs more fails instead of the
kubelet evicting the pod. Empty when neither is set.
*/}}
{{- define "igloo.duckdbMaxTempSize" -}}
{{- if .Values.duckdb.maxTempSize -}}
{{- .Values.duckdb.maxTempSize -}}
{{- else -}}
{{- with .Values.duckdb.spillVolume.sizeLimit -}}
{{- $size := toString . -}}
{{- $re := "^([0-9]+(?:\\.[0-9]+)?)(|k|M|G|T|Ki|Mi|Gi|Ti)$" -}}
{{- if not (regexMatch $re $size) -}}
{{- fail (printf "igloo: cannot read duckdb.spillVolume.sizeLimit %q; set duckdb.maxTempSize (e.g. \"40GiB\")" $size) -}}
{{- end -}}
{{- $unitBytes := dict "" "1" "k" "1000" "M" "1000000" "G" "1000000000" "T" "1000000000000" "Ki" "1024" "Mi" "1048576" "Gi" "1073741824" "Ti" "1099511627776" -}}
{{- $bytes := mulf (float64 (regexReplaceAll $re $size "${1}")) (float64 (get $unitBytes (regexReplaceAll $re $size "${2}"))) -}}
{{- printf "%dMiB" (int64 (floor (mulf (divf $bytes 1048576.0) 0.9))) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* ---------------------------------------------------------------------
Scale to zero
--------------------------------------------------------------------- */}}

{{/*
Igloo pods the gateway starts (IGLOO_GATEWAY_K8S_REPLICAS):
scaleToZero.replicas, else autoscaling.minReplicas with autoscaling on, else
replicaCount.
*/}}
{{- define "igloo.scaleToZeroReplicas" -}}
{{- if not (kindIs "invalid" .Values.scaleToZero.replicas) -}}
{{- int .Values.scaleToZero.replicas -}}
{{- else if .Values.autoscaling.enabled -}}
{{- int .Values.autoscaling.minReplicas -}}
{{- else -}}
{{- int .Values.replicaCount -}}
{{- end -}}
{{- end -}}

{{/* ---------------------------------------------------------------------
Generated secrets
--------------------------------------------------------------------- */}}

{{/*
Data of the chart-managed Secret, base64-encoded, as YAML. Generated values are
read back from the live Secret (lookup), so they survive upgrades; values given
inline (admin.password, *.url) always win, and nothing is stored for settings
that come from an existingSecret.

The result is computed once per render and cached, so the Secret and the
checksum/secret pod annotations see the same generated values even on first
install (otherwise the first upgrade would restart every pod).
*/}}
{{- define "igloo.secretData" -}}
{{- if not (hasKey .Values "__iglooSecretDataCache") -}}
{{- $_ := set .Values "__iglooSecretDataCache" (include "igloo.generateSecretData" .) -}}
{{- end -}}
{{- get .Values "__iglooSecretDataCache" -}}
{{- end -}}

{{- define "igloo.generateSecretData" -}}
{{- $existing := dict -}}
{{- with lookup "v1" "Secret" .Release.Namespace (include "igloo.secretName" .) -}}
{{- $existing = default (dict) .data -}}
{{- end -}}
{{- $data := dict -}}
{{- if not .Values.admin.existingSecret -}}
{{- if .Values.admin.password -}}
{{- $_ := set $data "admin-password" (.Values.admin.password | b64enc) -}}
{{- else -}}
{{- $_ := set $data "admin-password" (get $existing "admin-password" | default (randAlphaNum 32 | b64enc)) -}}
{{- end -}}
{{- end -}}
{{- if not .Values.erasure.existingSecret -}}
{{- $_ := set $data "erasure-key" (get $existing "erasure-key" | default (randAlphaNum 48 | b64enc)) -}}
{{- end -}}
{{- if not .Values.catalog.encryptionKey.existingSecret -}}
{{- $_ := set $data "encryption-key" (get $existing "encryption-key" | default (randAlphaNum 48 | b64enc)) -}}
{{- end -}}
{{- if and .Values.metadataDatabase.url (not .Values.metadataDatabase.existingSecret) -}}
{{- $_ := set $data "metadata-database-url" (.Values.metadataDatabase.url | b64enc) -}}
{{- end -}}
{{- if and .Values.catalog.database.url (not .Values.catalog.database.existingSecret) -}}
{{- $_ := set $data "catalog-database-url" (.Values.catalog.database.url | b64enc) -}}
{{- end -}}
{{- if .Values.postgresql.enabled -}}
{{- $password := get $existing "postgres-password" | default (randAlphaNum 32 | b64enc) -}}
{{- $_ := set $data "postgres-password" $password -}}
{{- $url := printf "postgresql://igloo:%s@%s:5432/igloo" (b64dec $password) (include "igloo.postgresqlName" .) -}}
{{- $_ := set $data "database-url" ($url | b64enc) -}}
{{- end -}}
{{- if and .Values.workers.enabled (not .Values.workers.existingSecret) -}}
{{- $_ := set $data "worker-secret" (get $existing "worker-secret" | default (randAlphaNum 48 | b64enc)) -}}
{{- end -}}
{{- range .Values.pools -}}
{{- if and (include "igloo.poolRunsWorkers" (list $ .)) (not (dig "workers" "existingSecret" "" .)) -}}
{{- $key := printf "worker-secret-%s" .name -}}
{{- $_ := set $data $key (get $existing $key | default (randAlphaNum 48 | b64enc)) -}}
{{- end -}}
{{- end -}}
{{- if .Values.objectStorage.enabled -}}
{{- $_ := set $data "object-storage-access-key-id" (get $existing "object-storage-access-key-id" | default (randAlphaNum 20 | b64enc)) -}}
{{- $_ := set $data "object-storage-secret-access-key" (get $existing "object-storage-secret-access-key" | default (randAlphaNum 40 | b64enc)) -}}
{{- end -}}
{{- if $data -}}
{{- toYaml $data -}}
{{- end -}}
{{- end -}}

{{/* ---------------------------------------------------------------------
Validation
--------------------------------------------------------------------- */}}

{{/*
Whether templates are being rendered for a real cluster (install, upgrade,
--dry-run=server) rather than offline (helm template, helm lint), where lookup
always returns nothing. Only evaluated when the values are incomplete.
*/}}
{{- define "igloo.renderingForCluster" -}}
{{- if or (lookup "v1" "ServiceAccount" .Release.Namespace "default") (lookup "v1" "Namespace" "" "kube-system") -}}
true
{{- end -}}
{{- end -}}

{{/*
Fails on settings that cannot work. Missing required settings fail too, with
one exception: the chart's defaults alone (no warehouse location and no
database at all) still render offline, so `helm lint` and `helm template`
work without values; installing them fails.
Outputs YAML comments describing what is missing in that case.
*/}}
{{- define "igloo.validate" -}}
{{- $v := .Values -}}
{{- $location := include "igloo.warehouseLocation" . -}}
{{- $scheme := include "igloo.warehouseScheme" . -}}
{{- $prefix := "igloo: " -}}

{{- if $location -}}
{{- if not (has $scheme (list "s3" "gs" "abfss")) -}}
{{- fail (printf "%sstorage.warehouseLocation %q must start with s3://, gs:// or abfss://" $prefix $location) -}}
{{- end -}}
{{- end -}}

{{- if $v.objectStorage.enabled -}}
{{- $bucketRoot := printf "s3://%s" $v.objectStorage.bucket -}}
{{- if and $v.storage.warehouseLocation (ne $v.storage.warehouseLocation $bucketRoot) (not (hasPrefix (printf "%s/" $bucketRoot) $v.storage.warehouseLocation)) -}}
{{- fail (printf "%sobjectStorage.enabled stores tables in the bundled bucket %q, but storage.warehouseLocation is %q. Leave storage.warehouseLocation empty (it defaults to %s/warehouse), point it inside that bucket, or disable objectStorage." $prefix $v.objectStorage.bucket $v.storage.warehouseLocation $bucketRoot) -}}
{{- end -}}
{{- if $v.storage.s3.existingSecret -}}
{{- fail (printf "%sobjectStorage.enabled uses keys generated for the bundled object store; storage.s3.existingSecret must be empty" $prefix) -}}
{{- end -}}
{{- end -}}

{{- if and (eq $scheme "s3") (not $v.objectStorage.enabled) (not $v.storage.s3.existingSecret) (ne $v.catalog.systemCredentials "aws") -}}
{{- fail (printf "%sstorage.warehouseLocation is on S3 without static keys (storage.s3.existingSecret), so Lakekeeper must use its own AWS identity: set catalog.systemCredentials=aws (and give catalog.serviceAccount an IAM role), or set storage.s3.existingSecret" $prefix) -}}
{{- end -}}
{{- if eq $scheme "gs" -}}
{{- if not $v.storage.gcs.existingSecret -}}
{{- fail (printf "%sgs:// warehouse locations need an HMAC key: set storage.gcs.existingSecret to a Secret with keys hmac-key-id and hmac-secret" $prefix) -}}
{{- end -}}
{{- if ne $v.catalog.systemCredentials "gcp" -}}
{{- fail (printf "%sgs:// warehouse locations need Lakekeeper to use its own GCP identity: set catalog.systemCredentials=gcp (and bind catalog.serviceAccount with Workload Identity)" $prefix) -}}
{{- end -}}
{{- end -}}
{{- if and (eq $scheme "abfss") (not $v.storage.azure.existingSecret) (ne $v.catalog.systemCredentials "azure") -}}
{{- fail (printf "%sabfss:// without an account key (storage.azure.existingSecret) needs Lakekeeper to use its own Azure identity: set catalog.systemCredentials=azure (and catalog.serviceAccount / catalog.podLabels for workload identity)" $prefix) -}}
{{- end -}}

{{- $metadataConfigured := or $v.metadataDatabase.existingSecret $v.metadataDatabase.url -}}
{{- $catalogConfigured := or $v.catalog.database.existingSecret $v.catalog.database.url -}}
{{- if and $v.postgresql.enabled $metadataConfigured $catalogConfigured -}}
{{- fail (printf "%spostgresql.enabled is true, but metadataDatabase and catalog.database both point at another database, so the bundled PostgreSQL would be unused. Disable postgresql or clear one of them." $prefix) -}}
{{- end -}}

{{- if and $v.autoscaling.enabled (gt (int $v.autoscaling.minReplicas) (int $v.autoscaling.maxReplicas)) -}}
{{- fail (printf "%sautoscaling.minReplicas (%v) is larger than autoscaling.maxReplicas (%v)" $prefix $v.autoscaling.minReplicas $v.autoscaling.maxReplicas) -}}
{{- end -}}

{{- if $v.scaleToZero.enabled -}}
{{- $replicas := int (include "igloo.scaleToZeroReplicas" .) -}}
{{- if lt $replicas 1 -}}
{{- fail (printf "%sscaleToZero.enabled starts scaleToZero.replicas igloo pods (default: replicaCount), which must be at least 1" $prefix) -}}
{{- end -}}
{{- if and $v.autoscaling.enabled (or (lt $replicas (int $v.autoscaling.minReplicas)) (gt $replicas (int $v.autoscaling.maxReplicas))) -}}
{{- fail (printf "%sscaleToZero.replicas (%d) must lie between autoscaling.minReplicas (%v) and autoscaling.maxReplicas (%v), or the HorizontalPodAutoscaler resizes the Deployment as soon as the gateway starts it. Leave it empty to use autoscaling.minReplicas." $prefix $replicas $v.autoscaling.minReplicas $v.autoscaling.maxReplicas) -}}
{{- end -}}
{{- end -}}

{{- if and $v.sso.issuer (not $v.sso.clientId) -}}
{{- fail (printf "%ssso.issuer is set, so sso.clientId must be set too: the client ID Igloo is registered under at the identity provider" $prefix) -}}
{{- end -}}

{{- /* Igloo's own check (config.CheckAdminPassword) less the length, which
the schema enforces. The message never includes the password. */ -}}
{{- with $v.admin.password -}}
{{- $word := lower (trim .) -}}
{{- $guessable := list "password" "admin" "igloo" "secret" "default" (lower $v.admin.user) -}}
{{- if or (contains "changeme" (regexReplaceAll `[^\p{L}\p{N}]` $word "")) (has $word $guessable) (eq (len (uniq (splitList "" .))) 1) -}}
{{- fail (printf "%sadmin.password is a placeholder or easily guessed, and Igloo will not create the admin account with it. Leave it empty for the chart to generate one, or set a long random value, e.g. from `openssl rand -base64 24`" $prefix) -}}
{{- end -}}
{{- end -}}

{{- if and $v.workers.enabled (lt (int $v.workers.maxReplicas) 1) -}}
{{- fail (printf "%sworkers.maxReplicas must be at least 1 with workers.enabled" $prefix) -}}
{{- end -}}
{{- if and $v.workers.large.enabled (not (include "igloo.anyWorkers" .)) -}}
{{- fail (printf "%sworkers.large.enabled needs workers.enabled too: the large worker shares the workers' secret and settings" $prefix) -}}
{{- end -}}

{{- $missing := list -}}
{{- if not $location -}}
{{- $missing = append $missing "a warehouse location: set storage.warehouseLocation (s3://, gs:// or abfss://), or objectStorage.enabled=true for the bundled object store" -}}
{{- end -}}
{{- if not (include "igloo.catalogDatabaseRef" .) -}}
{{- $missing = append $missing "a PostgreSQL database: set metadataDatabase.existingSecret / metadataDatabase.url (catalog.database defaults to it), or postgresql.enabled=true for the bundled one" -}}
{{- end -}}
{{- if $missing -}}
{{- if and (eq (len $missing) 2) (not (include "igloo.renderingForCluster" .)) -}}
# WARNING: these values cannot be installed. Igloo needs:
{{- range $missing }}
#   - {{ . }}
{{- end }}
# For a self-contained development install:
#   --set postgresql.enabled=true --set objectStorage.enabled=true
{{- else -}}
{{- $lines := list -}}
{{- range $missing -}}{{- $lines = append $lines (printf "  - %s" .) -}}{{- end -}}
{{- fail (printf "\n\nigloo: missing required values:\n%s\nFor a self-contained development install: --set postgresql.enabled=true --set objectStorage.enabled=true\n" (join "\n" $lines)) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Refuses a read cache volume that cannot work, for the release's igloo pods
or a pool's (called from the Deployment, in either's context): an ephemeral
volume needs a size, and a claim, which one pod at a time can use, needs a
Deployment of at most one pod that never runs two at once.
*/}}
{{- define "igloo.validateCacheVolume" -}}
{{- $c := .Values.cacheVolume -}}
{{- $what := "cacheVolume" -}}
{{- with (get . "Pool") -}}{{- $what = printf "pool %q's cacheVolume" .name -}}{{- end -}}
{{- if and (eq $c.type "ephemeral") (not $c.sizeLimit) -}}
{{- fail (printf "igloo: %s.type ephemeral needs cacheVolume.sizeLimit, the size of each pod's volume (e.g. 100Gi)" $what) -}}
{{- end -}}
{{- if eq $c.type "persistentVolumeClaim" -}}
{{- if not $c.existingClaim -}}
{{- fail (printf "igloo: %s.type persistentVolumeClaim needs cacheVolume.existingClaim, the claim to keep the cache on" $what) -}}
{{- end -}}
{{- $pods := int .Values.replicaCount -}}
{{- if .Values.scaleToZero.enabled -}}{{- $pods = int (include "igloo.scaleToZeroReplicas" .) -}}{{- end -}}
{{- if or .Values.autoscaling.enabled (gt $pods 1) (ne (toString (dig "type" "RollingUpdate" (.Values.strategy | default dict))) "Recreate") -}}
{{- fail (printf "igloo: %s.type persistentVolumeClaim keeps one igloo pod's cache on claim %q, which no two pods may use at once: set replicaCount (and scaleToZero.replicas) to 1, autoscaling.enabled=false and strategy.type=Recreate, or use type emptyDir or ephemeral" $what $c.existingClaim) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* ---------------------------------------------------------------------
Lakekeeper
--------------------------------------------------------------------- */}}

{{/* Environment shared by the catalog containers. Call with (list . $databaseRef). */}}
{{- define "igloo.catalogEnv" -}}
{{- $root := index . 0 -}}
{{- $db := index . 1 -}}
# Lakekeeper's tables are unqualified and belong in schema "public". With the
# default search_path ("$user", public) and a database role named "igloo",
# they would land in, and later read from, Igloo's own schema "igloo".
# Override with catalog.extraEnv if a pooler rejects startup options.
- name: PGOPTIONS
  value: "-c search_path=public"
- name: LAKEKEEPER__PG_ENCRYPTION_KEY
  valueFrom:
    secretKeyRef:
      {{- if $root.Values.catalog.encryptionKey.existingSecret }}
      name: {{ $root.Values.catalog.encryptionKey.existingSecret }}
      {{- else }}
      name: {{ include "igloo.secretName" $root }}
      {{- end }}
      key: encryption-key
{{- with $db }}
- name: LAKEKEEPER__PG_DATABASE_URL_READ
  valueFrom:
    secretKeyRef:
      name: {{ .name }}
      key: {{ .key }}
- name: LAKEKEEPER__PG_DATABASE_URL_WRITE
  valueFrom:
    secretKeyRef:
      name: {{ .name }}
      key: {{ .key }}
{{- end }}
{{- if eq $root.Values.catalog.systemCredentials "aws" }}
- name: LAKEKEEPER__ENABLE_AWS_SYSTEM_CREDENTIALS
  value: "true"
- name: LAKEKEEPER__S3_ENABLE_DIRECT_SYSTEM_CREDENTIALS
  value: "true"
{{- else if eq $root.Values.catalog.systemCredentials "gcp" }}
- name: LAKEKEEPER__ENABLE_GCP_SYSTEM_CREDENTIALS
  value: "true"
{{- else if eq $root.Values.catalog.systemCredentials "azure" }}
- name: LAKEKEEPER__ENABLE_AZURE_SYSTEM_CREDENTIALS
  value: "true"
{{- end }}
{{- with $root.Values.catalog.extraEnv }}
{{ toYaml . }}
{{- end }}
{{- end -}}
{{- define "igloo.catalogSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: ["ALL"]
{{- end -}}
