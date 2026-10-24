{{/* ---------------------------------------------------------------------
Distributed-execution workers (docs/distributed-execution.md)
--------------------------------------------------------------------- */}}

{{/* The workers' Deployment and Service: <fullname>-workers, or a compute
pool's own, <fullname>-pool-<name>-workers. Each pool has workers of its
own, which only its igloo pods use. */}}
{{- define "igloo.workersName" -}}{{ include "igloo.poolComponentName" (list . "workers") }}{{- end -}}

{{/* The workers' component label: "worker", or "pool-<name>-worker". */}}
{{- define "igloo.workerComponent" -}}
{{- with (get . "Pool") -}}pool-{{ .name }}-worker{{- else -}}worker{{- end -}}
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
A worker's IGLOO_DUCKDB_MEMORY_LIMIT: workers.duckdb.memoryLimit, or 80% of
workers.resources.limits.memory leaving at least 512Mi, as for igloo pods.
*/}}
{{- define "igloo.workerMemoryLimit" -}}
{{- if .Values.workers.duckdb.memoryLimit -}}
{{- .Values.workers.duckdb.memoryLimit -}}
{{- else -}}
{{- with .Values.workers.resources -}}{{- with .limits -}}{{- with .memory -}}
{{- $total := float64 (include "igloo.quantityMiB" (list "workers.resources.limits.memory" .)) -}}
{{- $mib := floor (min (mulf $total 0.8) (subf $total 512.0)) -}}
{{- if lt $mib 256.0 -}}
{{- fail "igloo: workers.resources.limits.memory leaves DuckDB less than 256Mi after a 512Mi reserve; use at least 1Gi, or set workers.duckdb.memoryLimit" -}}
{{- end -}}
{{- printf "%dMiB" (int64 $mib) -}}
{{- end -}}{{- end -}}{{- end -}}
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
