{{/* ---------------------------------------------------------------------
Distributed-execution workers (docs/distributed-execution.md)
--------------------------------------------------------------------- */}}

{{/* The workers' Deployment and Service: <fullname>-workers, or a compute
pool's own, <fullname>-pool-<name>-workers. Each pool has workers of its
own, which only its igloo pods use. In the large worker's context (.Large,
see "igloo.largeWorker"), the large worker's: <fullname>-workers-large or
<fullname>-pool-<name>-workers-large. */}}
{{- define "igloo.workersName" -}}
{{- if get . "Large" -}}{{ include "igloo.largeWorkerName" . }}{{- else -}}{{ include "igloo.poolComponentName" (list . "workers") }}{{- end -}}
{{- end -}}

{{/* The large worker's Deployment and Service, from any context. */}}
{{- define "igloo.largeWorkerName" -}}{{ include "igloo.poolComponentName" (list . "workers-large") }}{{- end -}}

{{/* The workers' component label: "worker", or "pool-<name>-worker"; the
large worker's ends in "-large". */}}
{{- define "igloo.workerComponent" -}}
{{- include "igloo.workerBaseComponent" . -}}{{- if get . "Large" -}}-large{{- end -}}
{{- end -}}

{{- define "igloo.workerBaseComponent" -}}
{{- with (get . "Pool") -}}pool-{{ .name }}-worker{{- else -}}worker{{- end -}}
{{- end -}}

{{/* Where the workers' values are, for messages: "workers." or
"workers.large.". */}}
{{- define "igloo.workerValuesPrefix" -}}
{{- if get . "Large" -}}workers.large.{{- else -}}workers.{{- end -}}
{{- end -}}

{{/* Whether this context (the release's or a pool's) runs a large worker:
it needs workers.enabled too. */}}
{{- define "igloo.runsLargeWorker" -}}
{{- if and .Values.workers.enabled .Values.workers.large.enabled -}}true{{- end -}}
{{- end -}}

{{/* The components that may reach a worker's task port to exchange rows
(workers.exchanges local): the workers, and the large worker if there is
one. */}}
{{- define "igloo.workerPeerComponents" -}}
{{- $base := include "igloo.workerBaseComponent" . -}}
{{- $peers := list $base -}}
{{- if or (get . "Large") (include "igloo.runsLargeWorker" .) -}}{{- $peers = append $peers (printf "%s-large" $base) -}}{{- end -}}
{{- toJson $peers -}}
{{- end -}}

{{/*
The large worker's values, as YAML: workers.large's own (resources, duckdb,
cacheVolume, scheduling, pricePerHour, ...) in place of the workers', and
the workers' secret, exchanges and timeouts. Read with fromYaml.
*/}}
{{- define "igloo.largeWorkerValues" -}}
{{- $w := omit .Values.workers "large" -}}
{{- range $k, $v := .Values.workers.large -}}
{{- if ne $k "enabled" -}}{{- $_ := set $w $k $v -}}{{- end -}}
{{- end -}}
{{- toYaml $w -}}
{{- end -}}

{{/*
The large worker ("scale up", docs/distributed-execution.md): a second
worker Deployment of one pod on a large machine, rendered by "igloo.workers"
in a context whose .Values.workers are the large worker's values and whose
.Large is true.
*/}}
{{- define "igloo.largeWorker" -}}
{{- /* Generated secrets are cached in .Values; fill the cache before the
copy, so the large worker's pods see the same checksum. */ -}}
{{- $_ := include "igloo.secretData" . -}}
{{- $values := deepCopy .Values -}}
{{- $_ := set $values "workers" (include "igloo.largeWorkerValues" . | fromYaml) -}}
{{- $ctx := dict "Values" $values "Release" .Release "Chart" .Chart "Capabilities" .Capabilities "Template" .Template "Files" .Files "Large" true -}}
{{- with (get . "Pool") -}}{{- $_ := set $ctx "Pool" . -}}{{- end -}}
{{- include "igloo.workers" $ctx -}}
{{- end -}}

{{/* "true" when the release or any compute pool runs workers. */}}
{{- define "igloo.anyWorkers" -}}
{{- $any := .Values.workers.enabled -}}
{{- range .Values.pools -}}
{{- if include "igloo.poolRunsWorkers" (list $ .) -}}{{- $any = true -}}{{- end -}}
{{- end -}}
{{- if $any -}}true{{- end -}}
{{- end -}}

{{/* Worker port: TLS keyed by the worker secret. */}}
{{- define "igloo.workerPort" -}}7433{{- end -}}

{{/* Where the worker secret comes from, as YAML {name, key}. */}}
{{/* Each compute pool's workers have a secret of their own, so a pool's
igloo pods cannot use another pool's workers even where the network would
let them: its own workers.existingSecret, or the generated
worker-secret-<pool>. The release's existingSecret is the release's only. */}}
{{- define "igloo.workerSecretRef" -}}
{{- $pool := get . "Pool" -}}
{{- $own := "" -}}
{{- if $pool -}}{{- $own = dig "workers" "existingSecret" "" $pool -}}{{- end -}}
{{- if $own -}}
name: {{ $own }}
key: secret
{{- else if $pool -}}
name: {{ include "igloo.secretName" . }}
key: {{ printf "worker-secret-%s" $pool.name }}
{{- else if .Values.workers.existingSecret -}}
name: {{ .Values.workers.existingSecret }}
key: secret
{{- else -}}
name: {{ include "igloo.secretName" . }}
key: worker-secret
{{- end -}}
{{- end -}}

{{/* Whether a compute pool runs workers: its own workers.enabled, or the
release's. Call with (list $root $pool). */}}
{{- define "igloo.poolRunsWorkers" -}}
{{- $root := index . 0 -}}
{{- $pool := index . 1 -}}
{{- if and $pool.workers (hasKey $pool.workers "enabled") -}}
{{- if $pool.workers.enabled -}}true{{- end -}}
{{- else if $root.Values.workers.enabled -}}true{{- end -}}
{{- end -}}

{{/*
A size in MiB from a Kubernetes quantity, for the DuckDB settings derived
from it. Call with (list "<setting name>" "<quantity>").
*/}}
{{- define "igloo.quantityMiB" -}}
{{- $what := index . 0 -}}
{{- $q := toString (index . 1) -}}
{{- $re := "^([0-9]+(?:\\.[0-9]+)?)(|k|M|G|T|Ki|Mi|Gi|Ti)$" -}}
{{- if not (regexMatch $re $q) -}}
{{- fail (printf "igloo: cannot read %s %q" $what $q) -}}
{{- end -}}
{{- $unitBytes := dict "" "1" "k" "1000" "M" "1000000" "G" "1000000000" "T" "1000000000000" "Ki" "1024" "Mi" "1048576" "Gi" "1073741824" "Ti" "1099511627776" -}}
{{- $bytes := mulf (float64 (regexReplaceAll $re $q "${1}")) (float64 (get $unitBytes (regexReplaceAll $re $q "${2}"))) -}}
{{- divf $bytes 1048576.0 -}}
{{- end -}}

{{/*
A worker's IGLOO_DUCKDB_MEMORY_LIMIT: workers.duckdb.memoryLimit, or what
igloo.duckdbShareMiB leaves of workers.resources.limits.memory, as for igloo
pods.
The large worker's comes from workers.large the same way.
*/}}
{{- define "igloo.workerMemoryLimit" -}}
{{- include "igloo.workerMemoryLimitOf" (list .Values.workers (include "igloo.workerValuesPrefix" .)) -}}
{{- end -}}

{{/* The same from a workers dict. Call with (list <workers values>
"<prefix for messages>"). */}}
{{- define "igloo.workerMemoryLimitOf" -}}
{{- $w := index . 0 -}}
{{- $prefix := index . 1 -}}
{{- if $w.duckdb.memoryLimit -}}
{{- $w.duckdb.memoryLimit -}}
{{- else -}}
{{- with $w.resources -}}{{- with .limits -}}{{- with .memory -}}
{{- $total := float64 (include "igloo.quantityMiB" (list (printf "%sresources.limits.memory" $prefix) .)) -}}
{{- $mib := float64 (include "igloo.duckdbShareMiB" $total) -}}
{{- if lt $mib 256.0 -}}
{{- fail (printf "igloo: %sresources.limits.memory leaves DuckDB less than 256Mi; use at least 1Gi, or set %sduckdb.memoryLimit" $prefix $prefix) -}}
{{- end -}}
{{- printf "%dMiB" (int64 $mib) -}}
{{- end -}}{{- end -}}{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Whole cores from a container's resources: requests.cpu, else limits.cpu,
rounded down and at least 1; empty when neither is set. Call with
(list <resources> "<name for messages>").
*/}}
{{- define "igloo.resourceCores" -}}
{{- $r := index . 0 | default dict -}}
{{- $what := index . 1 -}}
{{- $cpu := "" -}}
{{- with $r.requests -}}{{- with .cpu -}}{{- $cpu = toString . -}}{{- end -}}{{- end -}}
{{- if not $cpu -}}{{- with $r.limits -}}{{- with .cpu -}}{{- $cpu = toString . -}}{{- end -}}{{- end -}}{{- end -}}
{{- if $cpu -}}
{{- if not (regexMatch "^[0-9]+(\\.[0-9]+)?m?$" $cpu) -}}
{{- fail (printf "igloo: cannot read %s %q" $what $cpu) -}}
{{- end -}}
{{- $n := float64 (trimSuffix "m" $cpu) -}}
{{- if hasSuffix "m" $cpu -}}{{- $n = divf $n 1000.0 -}}{{- end -}}
{{- max 1 (int64 (floor $n)) -}}
{{- end -}}
{{- end -}}

{{/* Environment of the igloo pods for distributed execution. */}}
{{- define "igloo.coordinatorEnv" -}}
- name: IGLOO_WORKERS
  value: {{ printf "%s:%s" (include "igloo.workersName" .) (include "igloo.workerPort" .) | quote }}
- name: IGLOO_WORKER_SECRET
  valueFrom:
    secretKeyRef:
      {{- include "igloo.workerSecretRef" . | nindent 6 }}
- name: IGLOO_WORKER_COMPUTE
  value: kubernetes
- name: IGLOO_WORKER_K8S_DEPLOYMENT
  value: {{ include "igloo.workersName" . | quote }}
- name: IGLOO_WORKER_MAX
  value: {{ int .Values.workers.maxReplicas | quote }}
- name: IGLOO_WORKER_IDLE_AFTER
  value: {{ .Values.workers.idleAfter | quote }}
- name: IGLOO_WORKER_START_TIMEOUT
  value: {{ .Values.workers.startTimeout | quote }}
- name: IGLOO_EXCHANGES
  value: {{ .Values.workers.exchanges | default "object" | quote }}
{{- with .Values.workers.pricePerHour }}
- name: IGLOO_WORKER_PRICE_PER_HOUR
  value: {{ . | toString | quote }}
{{- end }}
{{- with .Values.workers.instanceType }}
- name: IGLOO_WORKER_INSTANCE_TYPE
  value: {{ . | quote }}
{{- end }}
{{- if .Values.workers.spot }}
- name: IGLOO_WORKER_SPOT
  value: "true"
{{- end }}
{{- with .Values.workers.region }}
- name: IGLOO_CLOUD_REGION
  value: {{ . | quote }}
{{- end }}
{{- if include "igloo.runsLargeWorker" . }}
{{- $large := include "igloo.largeWorkerValues" . | fromYaml }}
{{- $name := include "igloo.largeWorkerName" . }}
# The large worker, for statements auto mode runs whole on one machine.
- name: IGLOO_LARGE_WORKER
  value: {{ printf "%s:%s" $name (include "igloo.workerPort" .) | quote }}
- name: IGLOO_LARGE_WORKER_COMPUTE
  value: kubernetes
- name: IGLOO_LARGE_WORKER_K8S_DEPLOYMENT
  value: {{ $name | quote }}
{{- with include "igloo.resourceCores" (list $large.resources "workers.large.resources cpu") }}
- name: IGLOO_LARGE_WORKER_CORES
  value: {{ . | quote }}
{{- end }}
{{- with include "igloo.workerMemoryLimitOf" (list $large "workers.large.") }}
- name: IGLOO_LARGE_WORKER_MEMORY
  value: {{ . | quote }}
{{- end }}
{{- with .Values.workers.large.pricePerHour }}
- name: IGLOO_LARGE_WORKER_PRICE_PER_HOUR
  value: {{ . | toString | quote }}
{{- end }}
{{- with .Values.workers.large.instanceType }}
- name: IGLOO_LARGE_WORKER_INSTANCE_TYPE
  value: {{ . | quote }}
{{- end }}
{{- if .Values.workers.large.spot }}
- name: IGLOO_LARGE_WORKER_SPOT
  value: "true"
{{- end }}
{{- end }}
{{- end -}}

{{/* Object storage settings, for workers: as the igloo pods have them. */}}
{{- define "igloo.workerStorageEnv" -}}
{{- $secretName := include "igloo.secretName" . -}}
{{- $objectStorage := include "igloo.objectStorageName" . -}}
- name: IGLOO_WAREHOUSE_LOCATION
  value: {{ include "igloo.warehouseLocation" . | quote }}
{{- with .Values.storage.allowedLocations }}
- name: IGLOO_ALLOWED_LOCATIONS
  value: {{ join "," . | quote }}
{{- end }}
{{- if .Values.objectStorage.enabled }}
- name: IGLOO_S3_ENDPOINT
  value: {{ printf "%s:9000" $objectStorage | quote }}
- name: IGLOO_S3_REGION
  value: {{ .Values.storage.s3.region | quote }}
- name: IGLOO_S3_URL_STYLE
  value: path
- name: IGLOO_S3_USE_SSL
  value: "false"
- name: IGLOO_S3_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ $secretName }}
      key: object-storage-access-key-id
- name: IGLOO_S3_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ $secretName }}
      key: object-storage-secret-access-key
{{- else }}
{{- with .Values.storage.s3.endpoint }}
- name: IGLOO_S3_ENDPOINT
  value: {{ . | quote }}
{{- end }}
- name: IGLOO_S3_REGION
  value: {{ .Values.storage.s3.region | quote }}
- name: IGLOO_S3_URL_STYLE
  value: {{ .Values.storage.s3.urlStyle | quote }}
- name: IGLOO_S3_USE_SSL
  value: {{ .Values.storage.s3.useSSL | quote }}
{{- with .Values.storage.s3.existingSecret }}
- name: IGLOO_S3_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ . }}
      key: access-key-id
- name: IGLOO_S3_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ . }}
      key: secret-access-key
{{- end }}
{{- end }}
{{- with .Values.storage.gcs.existingSecret }}
- name: IGLOO_GCS_HMAC_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ . }}
      key: hmac-key-id
- name: IGLOO_GCS_HMAC_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ . }}
      key: hmac-secret
{{- end }}
{{- with .Values.storage.azure.existingSecret }}
- name: IGLOO_AZURE_ACCOUNT_KEY
  valueFrom:
    secretKeyRef:
      name: {{ . }}
      key: account-key
{{- end }}
{{- end -}}
