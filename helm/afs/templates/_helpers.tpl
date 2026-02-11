{{- define "afs.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "afs.fullname" -}}
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

{{- define "afs.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "afs.labels" -}}
helm.sh/chart: {{ include "afs.chart" . }}
app.kubernetes.io/name: {{ include "afs.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{- define "afs.selectorLabels" -}}
app.kubernetes.io/name: {{ include "afs.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "afs.discoveryServiceName" -}}
{{- printf "%s-discovery" (include "afs.fullname" .) -}}
{{- end -}}

{{- define "afs.discoveryHeadlessServiceName" -}}
{{- printf "%s-discovery-headless" (include "afs.fullname" .) -}}
{{- end -}}

{{- define "afs.afsletName" -}}
{{- printf "%s-afslet" (include "afs.fullname" .) -}}
{{- end -}}

{{- define "afs.afsletHeadlessServiceName" -}}
{{- printf "%s-afslet-headless" (include "afs.fullname" .) -}}
{{- end -}}

{{- define "afs.afsProxyName" -}}
{{- printf "%s-afs-proxy" (include "afs.fullname" .) -}}
{{- end -}}

{{- define "afs.afsProxyHeadlessServiceName" -}}
{{- printf "%s-afs-proxy-headless" (include "afs.fullname" .) -}}
{{- end -}}
