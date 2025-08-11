{{/*
Expand the name of the chart.
*/}}
{{- define "test-app.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "test-app.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "test-app.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "test-app.labels" -}}
helm.sh/chart: {{ include "test-app.chart" . }}
{{ include "test-app.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "test-app.selectorLabels" -}}
app.kubernetes.io/name: {{ include "test-app.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "test-app.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "test-app.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Return the full config string used for hashing.
Useful for debugging purposes.
*/}}
{{- define "test-app.configString" -}}
{{- printf "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s"
    (default "" .Values.env.POSTGRES_HOST)
    (default "" .Values.env.POSTGRES_DB)
    (default "" .Values.env.POSTGRES_USER)
    (default "" .Values.env.KEYDB_HOST)
    (default "" .Values.env.NEO4J_HOST)
    (default "" .Values.env.NEO4J_USER)
    (default "" .Values.env.NEO4J_PASSWORD)
    (default "" .Values.env.CASSANDRA_HOST)
    (default "" .Values.env.CASSANDRA_KEYSPACE)
    (default "" .Values.env.CASSANDRA_USER)
    (default "" .Values.env.CASSANDRA_PASSWORD)
-}}
{{- end }}

{{/*
Return the short hash of the config string.
*/}}
{{- define "test-app.configHash" -}}
{{ include "test-app.configString" . | sha256sum | trunc 10 }}
{{- end }}
