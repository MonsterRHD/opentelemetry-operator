// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clusterobservability

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/open-telemetry/opentelemetry-operator/apis/v1alpha1"
	operatorcfg "github.com/open-telemetry/opentelemetry-operator/internal/config"
	"github.com/open-telemetry/opentelemetry-operator/internal/manifests"
)

func tlsSpec(tls *v1alpha1.TLSConfig) v1alpha1.ClusterObservabilitySpec {
	return v1alpha1.ClusterObservabilitySpec{
		Exporter: v1alpha1.OTLPHTTPExporter{
			Endpoint: "https://backend.example.com:4318",
			TLS:      tls,
		},
	}
}

func secretRef(name, key string) *v1alpha1.TLSReference {
	return &v1alpha1.TLSReference{Secret: &v1alpha1.TLSKeySelector{Name: name, Key: key}}
}

func configMapRef(name, key string) *v1alpha1.TLSReference {
	return &v1alpha1.TLSReference{ConfigMap: &v1alpha1.TLSKeySelector{Name: name, Key: key}}
}

func TestValidateExporterTLSReferences(t *testing.T) {
	t.Run("insecure without references is allowed", func(t *testing.T) {
		require.NoError(t, validateExporterTLSReferences(&v1alpha1.TLSConfig{Insecure: true}))
	})

	t.Run("insecure combined with references is invalid", func(t *testing.T) {
		err := validateExporterTLSReferences(&v1alpha1.TLSConfig{
			Insecure: true,
			CA:       configMapRef("ca", ""),
		})
		var refErr *ExporterTLSReferenceError
		require.ErrorAs(t, err, &refErr)
		assert.Equal(t, ReasonExporterTLSInvalid, refErr.Reason)
	})

	t.Run("client certificate and key must be set together", func(t *testing.T) {
		err := validateExporterTLSReferences(&v1alpha1.TLSConfig{
			ClientCertificate: secretRef("cert", ""),
		})
		var refErr *ExporterTLSReferenceError
		require.ErrorAs(t, err, &refErr)
		assert.Equal(t, ReasonExporterTLSInvalid, refErr.Reason)
	})

	t.Run("exactly one source must be set", func(t *testing.T) {
		err := validateExporterTLSReferences(&v1alpha1.TLSConfig{
			CA: &v1alpha1.TLSReference{
				Secret:    &v1alpha1.TLSKeySelector{Name: "ca"},
				ConfigMap: &v1alpha1.TLSKeySelector{Name: "ca"},
			},
		})
		var refErr *ExporterTLSReferenceError
		require.ErrorAs(t, err, &refErr)
		assert.Equal(t, ReasonExporterTLSInvalid, refErr.Reason)
	})

	t.Run("reference name must not be empty", func(t *testing.T) {
		err := validateExporterTLSReferences(&v1alpha1.TLSConfig{
			CA: &v1alpha1.TLSReference{Secret: &v1alpha1.TLSKeySelector{}},
		})
		var refErr *ExporterTLSReferenceError
		require.ErrorAs(t, err, &refErr)
		assert.Equal(t, ReasonExporterTLSInvalid, refErr.Reason)
	})

	t.Run("complete CA and client pair is valid", func(t *testing.T) {
		require.NoError(t, validateExporterTLSReferences(&v1alpha1.TLSConfig{
			CA:                configMapRef("ca", ""),
			ClientCertificate: secretRef("mtls", ""),
			ClientKey:         secretRef("mtls", ""),
		}))
	})
}

func newTLSFakeClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
}

func resolveParams(t *testing.T, cli client.Client, spec v1alpha1.ClusterObservabilitySpec) manifests.Params {
	t.Helper()
	return manifests.Params{
		Client: cli,
		Config: operatorcfg.New(),
		ClusterObservability: v1alpha1.ClusterObservability{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster-obs", Namespace: "observability"},
			Spec:       spec,
		},
	}
}

func TestResolveExporterTLSWithoutReferences(t *testing.T) {
	cli := newTLSFakeClient(t)
	resolved, err := ResolveExporterTLS(context.Background(), resolveParams(t, cli, tlsSpec(nil)))
	require.NoError(t, err)
	assert.Nil(t, resolved)
	assert.False(t, HasExporterTLSReferences(tlsSpec(&v1alpha1.TLSConfig{Insecure: true})))
}

func TestResolveExporterTLSMissingResourceAndKey(t *testing.T) {
	t.Run("referenced secret does not exist", func(t *testing.T) {
		cli := newTLSFakeClient(t)
		spec := tlsSpec(&v1alpha1.TLSConfig{CA: secretRef("ca", "ca.crt")})
		_, err := ResolveExporterTLS(context.Background(), resolveParams(t, cli, spec))
		var refErr *ExporterTLSReferenceError
		require.ErrorAs(t, err, &refErr)
		assert.Equal(t, ReasonExporterTLSReferenceMissing, refErr.Reason)
		assert.Contains(t, refErr.Message, "Secret")
	})

	t.Run("referenced key is missing", func(t *testing.T) {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "ca", Namespace: "observability"},
			Data:       map[string][]byte{"other.crt": []byte("ca")},
		}
		cli := newTLSFakeClient(t, secret)
		spec := tlsSpec(&v1alpha1.TLSConfig{CA: secretRef("ca", "ca.crt")})
		_, err := ResolveExporterTLS(context.Background(), resolveParams(t, cli, spec))
		var refErr *ExporterTLSReferenceError
		require.ErrorAs(t, err, &refErr)
		assert.Equal(t, ReasonExporterTLSReferenceMissing, refErr.Reason)
		assert.Contains(t, refErr.Message, "ca.crt")
	})

	t.Run("empty key value is treated as missing", func(t *testing.T) {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "ca", Namespace: "observability"},
			Data:       map[string][]byte{"ca.crt": {}},
		}
		cli := newTLSFakeClient(t, secret)
		spec := tlsSpec(&v1alpha1.TLSConfig{CA: secretRef("ca", "ca.crt")})
		_, err := ResolveExporterTLS(context.Background(), resolveParams(t, cli, spec))
		var refErr *ExporterTLSReferenceError
		require.ErrorAs(t, err, &refErr)
		assert.Equal(t, ReasonExporterTLSReferenceMissing, refErr.Reason)
	})
}

func TestResolveExporterTLSProjectsMaterial(t *testing.T) {
	caConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "ca-bundle", Namespace: "observability"},
		Data:       map[string]string{"ca.crt": "CA-CERT"},
	}
	mtlsSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mtls", Namespace: "observability"},
		Data: map[string][]byte{
			"tls.crt": []byte("CLIENT-CERT"),
			"tls.key": []byte("CLIENT-KEY"),
		},
	}
	cli := newTLSFakeClient(t, caConfigMap, mtlsSecret)
	spec := tlsSpec(&v1alpha1.TLSConfig{
		CA:                configMapRef("ca-bundle", ""),
		ClientCertificate: secretRef("mtls", ""),
		ClientKey:         secretRef("mtls", ""),
	})

	resolved, err := ResolveExporterTLS(context.Background(), resolveParams(t, cli, spec))
	require.NoError(t, err)
	require.NotNil(t, resolved)

	assert.Equal(t, "/otel-exporter-tls/ca.crt", resolved.Files.CAFile)
	assert.Equal(t, "/otel-exporter-tls/tls.crt", resolved.Files.CertFile)
	assert.Equal(t, "/otel-exporter-tls/tls.key", resolved.Files.KeyFile)

	// One ConfigMap volume and one Secret volume, both sources deduped.
	require.Len(t, resolved.Volumes, 2)
	require.Len(t, resolved.VolumeMounts, 3)

	cmVolume := findVolumeByName(t, resolved.Volumes, 0)
	require.NotNil(t, cmVolume.ConfigMap)
	assert.Equal(t, "ca-bundle", cmVolume.ConfigMap.Name)
	secretVolume := findVolumeByName(t, resolved.Volumes, 1)
	require.NotNil(t, secretVolume.Secret)
	assert.Equal(t, "mtls", secretVolume.Secret.SecretName)

	mounts := mountsByPath(resolved.VolumeMounts)
	assert.Equal(t, cmVolume.Name, mounts["/otel-exporter-tls/ca.crt"].Name)
	assert.Equal(t, "ca.crt", mounts["/otel-exporter-tls/ca.crt"].SubPath)
	assert.True(t, mounts["/otel-exporter-tls/ca.crt"].ReadOnly)

	// The certificate and key come from the same source and share one volume.
	assert.Equal(t, secretVolume.Name, mounts["/otel-exporter-tls/tls.crt"].Name)
	assert.Equal(t, secretVolume.Name, mounts["/otel-exporter-tls/tls.key"].Name)
	assert.Equal(t, "tls.crt", mounts["/otel-exporter-tls/tls.crt"].SubPath)
	assert.Equal(t, "tls.key", mounts["/otel-exporter-tls/tls.key"].SubPath)
}

func TestResolveExporterTLSExplicitKeysAndBinaryData(t *testing.T) {
	caConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "ca-bundle", Namespace: "observability"},
		BinaryData: map[string][]byte{"custom/ca.pem": []byte("CA-CERT")},
	}
	certSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cert", Namespace: "observability"},
		Data:       map[string][]byte{"client.pem": []byte("CLIENT-CERT")},
	}
	keySecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "key", Namespace: "observability"},
		Data:       map[string][]byte{"client-key.pem": []byte("CLIENT-KEY")},
	}
	cli := newTLSFakeClient(t, caConfigMap, certSecret, keySecret)
	spec := tlsSpec(&v1alpha1.TLSConfig{
		ServerName:        "backend.example.com",
		CA:                configMapRef("ca-bundle", "custom/ca.pem"),
		ClientCertificate: secretRef("cert", "client.pem"),
		ClientKey:         secretRef("key", "client-key.pem"),
	})

	resolved, err := ResolveExporterTLS(context.Background(), resolveParams(t, cli, spec))
	require.NoError(t, err)
	require.Len(t, resolved.Volumes, 3)
	require.Len(t, resolved.VolumeMounts, 3)

	mounts := mountsByPath(resolved.VolumeMounts)
	assert.Equal(t, "custom/ca.pem", mounts["/otel-exporter-tls/ca.crt"].SubPath)
	assert.Equal(t, "client.pem", mounts["/otel-exporter-tls/tls.crt"].SubPath)
	assert.Equal(t, "client-key.pem", mounts["/otel-exporter-tls/tls.key"].SubPath)
}

func TestTLSReferenceMatches(t *testing.T) {
	spec := tlsSpec(&v1alpha1.TLSConfig{
		CA:                configMapRef("ca-bundle", ""),
		ClientCertificate: secretRef("mtls", ""),
		ClientKey:         secretRef("mtls", ""),
	})
	co := &v1alpha1.ClusterObservability{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster-obs", Namespace: "observability"},
		Spec:       spec,
	}

	assert.True(t, TLSReferenceMatches(co, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "mtls", Namespace: "observability"}}))
	assert.True(t, TLSReferenceMatches(co, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "ca-bundle", Namespace: "observability"}}))
	assert.False(t, TLSReferenceMatches(co, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "observability"}}))
	assert.False(t, TLSReferenceMatches(co, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "mtls", Namespace: "other"}}))
}

func TestBuildCollectorsWithExporterTLS(t *testing.T) {
	caConfigMap := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "ca-bundle", Namespace: "observability"},
		Data:       map[string]string{"ca.crt": "CA-CERT"},
	}
	mtlsSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mtls", Namespace: "observability"},
		Data: map[string][]byte{
			"tls.crt": []byte("CLIENT-CERT"),
			"tls.key": []byte("CLIENT-KEY"),
		},
	}
	cli := newTLSFakeClient(t, caConfigMap, mtlsSecret)
	spec := tlsSpec(&v1alpha1.TLSConfig{
		CA:                configMapRef("ca-bundle", ""),
		ClientCertificate: secretRef("mtls", ""),
		ClientKey:         secretRef("mtls", ""),
	})
	params := resolveParams(t, cli, spec)

	resolved, err := ResolveExporterTLS(context.Background(), params)
	require.NoError(t, err)

	agent, err := buildAgentCollector(params, "", resolved)
	require.NoError(t, err)
	cluster, err := buildClusterCollector(params, resolved)
	require.NoError(t, err)

	// Both collectors carry the same volumes, mounts and in-container paths.
	assert.Equal(t, resolved.Volumes, agent.Spec.Volumes[len(agent.Spec.Volumes)-len(resolved.Volumes):])
	assert.Equal(t, resolved.Volumes, cluster.Spec.Volumes)
	assert.Equal(t, resolved.VolumeMounts, agent.Spec.VolumeMounts[len(agent.Spec.VolumeMounts)-len(resolved.VolumeMounts):])
	assert.Equal(t, resolved.VolumeMounts, cluster.Spec.VolumeMounts)

	for _, cfg := range []struct {
		name string
		tls  map[string]any
	}{
		{"agent", exporterTLSConfig(t, agent.Spec.Config.Exporters.Object)},
		{"cluster", exporterTLSConfig(t, cluster.Spec.Config.Exporters.Object)},
	} {
		assert.Equal(t, "/otel-exporter-tls/ca.crt", cfg.tls["ca_file"], cfg.name)
		assert.Equal(t, "/otel-exporter-tls/tls.crt", cfg.tls["cert_file"], cfg.name)
		assert.Equal(t, "/otel-exporter-tls/tls.key", cfg.tls["key_file"], cfg.name)
	}

	// Without resolved material the collectors keep the previous volume set
	// and no TLS block is rendered.
	agentWithoutTLS, err := buildAgentCollector(params, "", nil)
	require.NoError(t, err)
	for _, v := range agentWithoutTLS.Spec.Volumes {
		assert.NotEqual(t, exporterTLSVolumeName, v.Name)
	}
	otlpWithoutTLS, ok := agentWithoutTLS.Spec.Config.Exporters.Object["otlp_http"].(map[string]any)
	require.True(t, ok)
	assert.NotContains(t, otlpWithoutTLS, "tls")
}

func exporterTLSConfig(t *testing.T, exporters map[string]any) map[string]any {
	t.Helper()
	otlp, ok := exporters["otlp_http"].(map[string]any)
	require.True(t, ok)
	tls, ok := otlp["tls"].(map[string]any)
	require.True(t, ok)
	return tls
}

func findVolumeByName(t *testing.T, volumes []corev1.Volume, index int) corev1.Volume {
	t.Helper()
	require.Greater(t, len(volumes), index)
	return volumes[index]
}

func mountsByPath(mounts []corev1.VolumeMount) map[string]corev1.VolumeMount {
	byPath := make(map[string]corev1.VolumeMount, len(mounts))
	for _, mount := range mounts {
		byPath[mount.MountPath] = mount
	}
	return byPath
}
