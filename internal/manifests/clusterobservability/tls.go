// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package clusterobservability

import (
	"context"
	"fmt"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/open-telemetry/opentelemetry-operator/apis/v1alpha1"
	"github.com/open-telemetry/opentelemetry-operator/internal/manifests"
	"github.com/open-telemetry/opentelemetry-operator/internal/manifests/clusterobservability/config"
	"github.com/open-telemetry/opentelemetry-operator/internal/naming"
)

const (
	// exporterTLSMountDir is the directory the exporter TLS material is projected
	// into in both managed collectors, so they share the same mount paths.
	exporterTLSMountDir = "/otel-exporter-tls"

	exporterTLSCAFileName   = "ca.crt"
	exporterTLSCertFileName = "tls.crt"
	exporterTLSKeyFileName  = "tls.key"

	exporterTLSVolumeName = "exporter-tls"

	// Status reasons reported on the ExporterTLSReady condition.
	ReasonExporterTLSInvalid          = "ExporterTLSInvalid"
	ReasonExporterTLSReferenceMissing = "ExporterTLSReferenceMissing"
)

// ExporterTLSReferenceError reports that the exporter TLS configuration is
// invalid or that referenced Secret/ConfigMap material is missing or
// incomplete. The error is recoverable: correcting the spec or creating the
// referenced material lets the following reconciliation converge.
type ExporterTLSReferenceError struct {
	// Reason is the short, machine-readable reason used for the status condition.
	Reason string
	// Message is the human-readable detail of what must be fixed.
	Message string
}

func (e *ExporterTLSReferenceError) Error() string {
	return e.Message
}

// ResolvedExporterTLS is the validated, mountable TLS material of the exporter.
type ResolvedExporterTLS struct {
	Volumes      []corev1.Volume
	VolumeMounts []corev1.VolumeMount
	Files        config.ExporterTLSFiles
}

// HasExporterTLSReferences reports whether the exporter references TLS
// material from a Secret or a ConfigMap.
func HasExporterTLSReferences(spec v1alpha1.ClusterObservabilitySpec) bool {
	tls := spec.Exporter.TLS
	return tls != nil && (tls.CA != nil || tls.ClientCertificate != nil || tls.ClientKey != nil)
}

// TLSReferenceMatches reports whether the given Secret or ConfigMap is
// referenced by the exporter TLS configuration, in the matching namespace.
func TLSReferenceMatches(co *v1alpha1.ClusterObservability, obj client.Object) bool {
	if co.Namespace != obj.GetNamespace() {
		return false
	}

	name := obj.GetName()
	for _, ref := range exporterTLSReferences(co.Spec) {
		var selector *v1alpha1.TLSKeySelector
		switch obj.(type) {
		case *corev1.Secret:
			selector = ref.Secret
		case *corev1.ConfigMap:
			selector = ref.ConfigMap
		default:
			return false
		}
		if selector != nil && selector.Name == name {
			return true
		}
	}
	return false
}

// ResolveExporterTLS verifies every Secret/ConfigMap referenced by the exporter
// TLS configuration and builds the volumes, volume mounts and in-container file
// paths used by both managed collectors. It returns nil without an error when
// the exporter references no TLS material, preserving the previous behavior.
func ResolveExporterTLS(ctx context.Context, params manifests.Params) (*ResolvedExporterTLS, error) {
	co := params.ClusterObservability
	if !HasExporterTLSReferences(co.Spec) {
		return nil, nil
	}

	tls := co.Spec.Exporter.TLS
	if err := validateExporterTLSReferences(tls); err != nil {
		return nil, err
	}

	files := make([]projectedTLSFile, 0, 3)
	resolved := &ResolvedExporterTLS{}

	addSource := func(role string, ref *v1alpha1.TLSReference, defaultKey, mountFileName string, setPath func(string)) error {
		key, err := resolveExporterTLSKey(ctx, params.Client, co.Namespace, role, ref, defaultKey)
		if err != nil {
			return err
		}
		kind := sourceKindSecret
		if ref.ConfigMap != nil {
			kind = sourceKindConfigMap
		}
		files = append(files, projectedTLSFile{
			kind: kind,
			name: exporterTLSReferenceName(ref),
			key:  key,
			path: mountFileName,
		})
		setPath(filepath.Join(exporterTLSMountDir, mountFileName))
		return nil
	}

	var err error
	if tls.CA != nil {
		err = addSource("CA", tls.CA, exporterTLSCAFileName, exporterTLSCAFileName, func(p string) { resolved.Files.CAFile = p })
	}
	if err == nil && tls.ClientCertificate != nil {
		err = addSource("client certificate", tls.ClientCertificate, exporterTLSCertFileName, exporterTLSCertFileName, func(p string) { resolved.Files.CertFile = p })
	}
	if err == nil && tls.ClientKey != nil {
		err = addSource("client key", tls.ClientKey, exporterTLSKeyFileName, exporterTLSKeyFileName, func(p string) { resolved.Files.KeyFile = p })
	}
	if err != nil {
		return nil, err
	}

	resolved.Volumes, resolved.VolumeMounts = exporterTLSVolumes(files)
	return resolved, nil
}

type sourceKind string

const (
	sourceKindSecret    sourceKind = "Secret"
	sourceKindConfigMap sourceKind = "ConfigMap"
)

// projectedTLSFile is one referenced key projected under the exporter TLS directory.
type projectedTLSFile struct {
	kind sourceKind
	name string // Secret or ConfigMap name
	key  string // source data key, mounted via subPath
	path string // fixed file name under exporterTLSMountDir
}

type exporterTLSVolumeIdentity struct {
	kind sourceKind
	name string
}

// exporterTLSVolumes projects each referenced key onto its file under
// exporterTLSMountDir. Distinct sources become distinct volumes; keys from the
// same source share one volume and are mounted individually via subPath.
func exporterTLSVolumes(files []projectedTLSFile) ([]corev1.Volume, []corev1.VolumeMount) {
	var volumes []corev1.Volume
	var mounts []corev1.VolumeMount
	seen := map[exporterTLSVolumeIdentity]string{}
	for _, f := range files {
		id := exporterTLSVolumeIdentity{kind: f.kind, name: f.name}
		volumeName, ok := seen[id]
		if !ok {
			volumeName = naming.DNSName(naming.Truncate("%s-%d", 63, exporterTLSVolumeName, len(volumes)))
			seen[id] = volumeName
			var source corev1.VolumeSource
			switch f.kind {
			case sourceKindConfigMap:
				source.ConfigMap = &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: f.name},
				}
			default:
				source.Secret = &corev1.SecretVolumeSource{SecretName: f.name}
			}
			volumes = append(volumes, corev1.Volume{Name: volumeName, VolumeSource: source})
		}
		mounts = append(mounts, corev1.VolumeMount{
			Name:      volumeName,
			MountPath: filepath.Join(exporterTLSMountDir, f.path),
			SubPath:   f.key,
			ReadOnly:  true,
		})
	}
	return volumes, mounts
}

// validateExporterTLSReferences checks the parts of the TLS configuration that
// can be verified without reading the referenced resources.
func validateExporterTLSReferences(tls *v1alpha1.TLSConfig) error {
	if tls.Insecure && (tls.CA != nil || tls.ClientCertificate != nil || tls.ClientKey != nil) {
		return &ExporterTLSReferenceError{
			Reason:  ReasonExporterTLSInvalid,
			Message: "exporter tls: insecure must not be combined with ca, client_certificate or client_key references",
		}
	}
	if (tls.ClientCertificate == nil) != (tls.ClientKey == nil) {
		return &ExporterTLSReferenceError{
			Reason:  ReasonExporterTLSInvalid,
			Message: "exporter tls: client_certificate and client_key must both be set for mutual TLS",
		}
	}
	for role, ref := range map[string]*v1alpha1.TLSReference{
		"ca":                 tls.CA,
		"client_certificate": tls.ClientCertificate,
		"client_key":         tls.ClientKey,
	} {
		if ref == nil {
			continue
		}
		if (ref.Secret == nil) == (ref.ConfigMap == nil) {
			return &ExporterTLSReferenceError{
				Reason:  ReasonExporterTLSInvalid,
				Message: fmt.Sprintf("exporter tls: %s must reference exactly one of secret or configMap", role),
			}
		}
		if exporterTLSReferenceName(ref) == "" {
			return &ExporterTLSReferenceError{
				Reason:  ReasonExporterTLSInvalid,
				Message: fmt.Sprintf("exporter tls: %s reference must name a secret or configMap", role),
			}
		}
	}
	return nil
}

// resolveExporterTLSKey fetches the referenced resource and confirms the
// selected key exists and is not empty.
func resolveExporterTLSKey(ctx context.Context, cli client.Client, namespace, role string, ref *v1alpha1.TLSReference, defaultKey string) (string, error) {
	if ref.Secret != nil {
		key := exporterTLSKey(ref.Secret.Key, defaultKey)
		var secret corev1.Secret
		err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref.Secret.Name}, &secret)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return "", missingExporterTLSMaterial(role, "Secret", namespace, ref.Secret.Name, "")
			}
			return "", err
		}
		value, ok := secret.Data[key]
		if !ok || len(value) == 0 {
			return "", missingExporterTLSMaterial(role, "Secret", namespace, ref.Secret.Name, key)
		}
		return key, nil
	}

	key := exporterTLSKey(ref.ConfigMap.Key, defaultKey)
	var configMap corev1.ConfigMap
	err := cli.Get(ctx, client.ObjectKey{Namespace: namespace, Name: ref.ConfigMap.Name}, &configMap)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", missingExporterTLSMaterial(role, "ConfigMap", namespace, ref.ConfigMap.Name, "")
		}
		return "", err
	}
	value, ok := configMap.Data[key]
	if !ok {
		binaryValue, binaryOK := configMap.BinaryData[key]
		value, ok = string(binaryValue), binaryOK
	}
	if !ok || value == "" {
		return "", missingExporterTLSMaterial(role, "ConfigMap", namespace, ref.ConfigMap.Name, key)
	}
	return key, nil
}

func missingExporterTLSMaterial(role, kind, namespace, name, key string) *ExporterTLSReferenceError {
	if key == "" {
		return &ExporterTLSReferenceError{
			Reason: ReasonExporterTLSReferenceMissing,
			Message: fmt.Sprintf("exporter tls: %s %s referenced by %s not found in namespace %s; the previous collector configuration is kept",
				kind, name, role, namespace),
		}
	}
	return &ExporterTLSReferenceError{
		Reason: ReasonExporterTLSReferenceMissing,
		Message: fmt.Sprintf("exporter tls: key %q of %s %s referenced by %s is missing or empty in namespace %s; the previous collector configuration is kept",
			key, kind, name, role, namespace),
	}
}

func exporterTLSReferences(spec v1alpha1.ClusterObservabilitySpec) []*v1alpha1.TLSReference {
	tls := spec.Exporter.TLS
	if tls == nil {
		return nil
	}
	refs := make([]*v1alpha1.TLSReference, 0, 3)
	for _, ref := range []*v1alpha1.TLSReference{tls.CA, tls.ClientCertificate, tls.ClientKey} {
		if ref != nil {
			refs = append(refs, ref)
		}
	}
	return refs
}

func exporterTLSReferenceName(ref *v1alpha1.TLSReference) string {
	if ref == nil {
		return ""
	}
	if ref.Secret != nil {
		return ref.Secret.Name
	}
	if ref.ConfigMap != nil {
		return ref.ConfigMap.Name
	}
	return ""
}

func exporterTLSKey(key, defaultKey string) string {
	if key != "" {
		return key
	}
	return defaultKey
}
