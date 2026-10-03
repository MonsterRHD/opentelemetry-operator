// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-operator/apis/v1alpha1"
)

func TestLoadAgentCollectorConfigOpenShiftTLSOverride(t *testing.T) {
	loader := NewConfigLoader()

	base, err := loader.LoadCollectorConfig(AgentCollectorType, DistroProvider(""), v1alpha1.ClusterObservabilitySpec{}, nil)
	require.NoError(t, err)
	openshift, err := loader.LoadCollectorConfig(AgentCollectorType, OpenShift, v1alpha1.ClusterObservabilitySpec{}, nil)
	require.NoError(t, err)

	assert.Equal(t, true, requireMap(t, base.Receivers.Object, "kubelet_stats")["insecure_skip_verify"])

	kubeletstats := requireMap(t, openshift.Receivers.Object, "kubelet_stats")
	assert.Equal(t, "/etc/kubelet-serving-ca/ca-bundle.crt", kubeletstats["ca_file"])
	assert.Equal(t, false, kubeletstats["insecure_skip_verify"])
	assert.Contains(t, openshift.Receivers.Object, "host_metrics")
	assert.Contains(t, openshift.Receivers.Object, "file_log")
}

func tlsExporterSpec(tls *v1alpha1.TLSConfig) v1alpha1.ClusterObservabilitySpec {
	return v1alpha1.ClusterObservabilitySpec{
		Exporter: v1alpha1.OTLPHTTPExporter{
			Endpoint: "https://backend.example.com:4318",
			TLS:      tls,
		},
	}
}

func otlpHTTPTLS(t *testing.T, spec v1alpha1.ClusterObservabilitySpec, files *ExporterTLSFiles) map[string]any {
	t.Helper()
	loader := NewConfigLoader()
	cfg, err := loader.LoadCollectorConfig(ClusterCollectorType, DistroProvider(""), spec, files)
	require.NoError(t, err)
	otlp := requireMap(t, cfg.Exporters.Object, "otlp_http")
	tls, ok := otlp["tls"].(map[string]any)
	require.True(t, ok)
	return tls
}

func TestBuildExportersConfigKeepsSystemTrustAndInsecureBehavior(t *testing.T) {
	// No TLS settings at all renders no tls block.
	loader := NewConfigLoader()
	cfg, err := loader.LoadCollectorConfig(ClusterCollectorType, DistroProvider(""),
		v1alpha1.ClusterObservabilitySpec{Exporter: v1alpha1.OTLPHTTPExporter{Endpoint: "https://backend.example.com:4318"}}, nil)
	require.NoError(t, err)
	otlp := requireMap(t, cfg.Exporters.Object, "otlp_http")
	assert.NotContains(t, otlp, "tls")

	// Explicit insecure keeps its previous rendering.
	tls := otlpHTTPTLS(t, tlsExporterSpec(&v1alpha1.TLSConfig{Insecure: true}), nil)
	assert.Equal(t, true, tls["insecure"])
	assert.NotContains(t, tls, "ca_file")
}

func TestBuildExportersConfigPlainFilePathsArePassedThrough(t *testing.T) {
	tls := otlpHTTPTLS(t, tlsExporterSpec(&v1alpha1.TLSConfig{
		CAFile:     "/custom/ca.crt",
		CertFile:   "/custom/client.crt",
		KeyFile:    "/custom/client.key",
		ServerName: "backend.example.com",
	}), nil)
	assert.Equal(t, "/custom/ca.crt", tls["ca_file"])
	assert.Equal(t, "/custom/client.crt", tls["cert_file"])
	assert.Equal(t, "/custom/client.key", tls["key_file"])
	assert.Equal(t, "backend.example.com", tls["server_name"])
}

func TestBuildExportersConfigMountedFilesOverridePlainPaths(t *testing.T) {
	files := &ExporterTLSFiles{
		CAFile:   "/otel-exporter-tls/ca.crt",
		CertFile: "/otel-exporter-tls/tls.crt",
		KeyFile:  "/otel-exporter-tls/tls.key",
	}
	tls := otlpHTTPTLS(t, tlsExporterSpec(&v1alpha1.TLSConfig{
		CAFile:     "/custom/ca.crt",
		ServerName: "backend.example.com",
	}), files)
	assert.Equal(t, "/otel-exporter-tls/ca.crt", tls["ca_file"])
	assert.Equal(t, "/otel-exporter-tls/tls.crt", tls["cert_file"])
	assert.Equal(t, "/otel-exporter-tls/tls.key", tls["key_file"])
	assert.Equal(t, "backend.example.com", tls["server_name"])
}

func requireMap(t *testing.T, parent map[string]any, key string) map[string]any {
	t.Helper()

	value, ok := parent[key]
	require.Truef(t, ok, "key %q not found", key)
	result, ok := value.(map[string]any)
	require.Truef(t, ok, "key %q has type %T", key, value)
	return result
}
