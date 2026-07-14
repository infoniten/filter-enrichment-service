{{- define "fes.name" -}}
filter-enrichment-service
{{- end -}}

{{- define "fes.labels" -}}
app.kubernetes.io/name: {{ include "fes.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "fes.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fes.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
