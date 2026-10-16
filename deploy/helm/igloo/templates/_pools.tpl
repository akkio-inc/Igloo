{{/* ---------------------------------------------------------------------
Compute pools

Each entry of .Values.pools is a "virtual warehouse": an igloo Deployment
with its own size, replica count and SQL Service, and its own scale-to-zero
gateway if it asks for one, sharing the release's catalog, metadata database
and object storage. pools.yaml renders it with the same templates as the
release's own Deployment, in a context whose .Values are the release's with
the pool's on top and whose .Pool is the pool. In the release's own context
.Pool is unset, and these helpers give the names the chart always had.
--------------------------------------------------------------------- */}}

{{/* Name of a Deployment and its SQL Service: the release's, or
<fullname>-pool-<name>. */}}
{{- define "igloo.serverName" -}}
{{- with (get . "Pool") -}}
{{- include "igloo.componentName" (list $ (printf "pool-%s" .name)) -}}
{{- else -}}
{{- include "igloo.fullname" . -}}
{{- end -}}
{{- end -}}

{{/* "<fullname>-<suffix>", or "<fullname>-pool-<name>-<suffix>" for a pool.
Call with (list . "suffix"). */}}
{{- define "igloo.poolComponentName" -}}
{{- $root := index . 0 -}}
{{- $suffix := index . 1 -}}
{{- with (get $root "Pool") -}}
{{- include "igloo.componentName" (list $root (printf "pool-%s-%s" .name $suffix)) -}}
{{- else -}}
{{- include "igloo.componentName" (list $root $suffix) -}}
{{- end -}}
{{- end -}}

{{/* The component label of igloo pods: "server", or "pool-<name>", so no
Deployment or Service selects another's pods. */}}
{{- define "igloo.serverComponent" -}}
{{- with (get . "Pool") -}}pool-{{ .name }}{{- else -}}server{{- end -}}
{{- end -}}

{{- define "igloo.gatewayComponent" -}}
{{- with (get . "Pool") -}}pool-{{ .name }}-gateway{{- else -}}gateway{{- end -}}
{{- end -}}

{{/* NetworkPolicy peers for every igloo pod of the release, its pools'
included: what the catalog, PostgreSQL and the object store accept. */}}
{{- define "igloo.enginePeers" -}}
- podSelector:
    matchLabels:
      {{- include "igloo.selectorLabels" (dict "root" . "component" "server") | nindent 6 }}
{{- range .Values.pools }}
- podSelector:
    matchLabels:
      {{- include "igloo.selectorLabels" (dict "root" $ "component" (printf "pool-%s" .name)) | nindent 6 }}
{{- end }}
{{- end -}}

{{/*
Refuses pools that cannot work: names must be unique, fit in Kubernetes
names and differ from "default", the release's own pool.
*/}}
{{- define "igloo.validatePools" -}}
{{- $seen := dict -}}
{{- range .Values.pools -}}
{{- if not (regexMatch "^[a-z]([-a-z0-9]{0,18}[a-z0-9])?$" (toString .name)) -}}
{{- fail (printf "igloo: pool name %q must be 1 to 20 lowercase letters, digits and hyphens, starting with a letter" (toString .name)) -}}
{{- end -}}
{{- if eq .name "default" -}}
{{- fail "igloo: the pool name \"default\" is the release's own igloo Deployment; give the pool another name" -}}
{{- end -}}
{{- if hasKey $seen .name -}}
{{- fail (printf "igloo: pool %q is listed twice" .name) -}}
{{- end -}}
{{- $_ := set $seen .name true -}}
{{- with .scaleToZero -}}
{{- if and .enabled (not (kindIs "invalid" .replicas)) (lt (int .replicas) 1) -}}
{{- fail "igloo: a pool's scaleToZero.replicas must be at least 1" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
