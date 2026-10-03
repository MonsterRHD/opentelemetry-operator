// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package opampbridge

import (
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"

	"github.com/open-telemetry/opentelemetry-operator/apis/v1alpha1"
)

const (
	// OpAMPBridgeTLSMountPath is the mount path of the Secret holding the TLS material
	// for the OpAMP backend connection.
	OpAMPBridgeTLSMountPath = "/tls"

	tlsCAFileKey   = "ca.crt"
	tlsCertFileKey = "tls.crt"
	tlsKeyFileKey  = "tls.key"
)

// ValidateTLSSecret verifies that the referenced Secret contains usable TLS material:
// a CA certificate and/or a matching client certificate and private key.
func ValidateTLSSecret(secret *corev1.Secret) error {
	if secret == nil {
		return errors.New("the TLS Secret is not available")
	}
	_, hasCA := secret.Data[tlsCAFileKey]
	_, hasCert := secret.Data[tlsCertFileKey]
	_, hasKey := secret.Data[tlsKeyFileKey]
	if hasCert != hasKey {
		return fmt.Errorf("the TLS Secret %q must contain both %q and %q for mutual TLS", secret.Name, tlsCertFileKey, tlsKeyFileKey)
	}
	if !hasCA && !hasCert {
		return fmt.Errorf("the TLS Secret %q must contain %q and/or the %q and %q pair", secret.Name, tlsCAFileKey, tlsCertFileKey, tlsKeyFileKey)
	}
	return nil
}

// renderTLSConfig builds the tls block written to the OpAMPBridge configuration file. File paths
// point at the mounted Secret; the SecretName itself is never written to the configuration.
func renderTLSConfig(tls v1alpha1.OpAMPBridgeTLSConfig, secret *corev1.Secret) (map[any]any, error) {
	tlsConfig := map[any]any{}
	if tls.Insecure {
		tlsConfig["insecure"] = true
	}
	if tls.InsecureSkipVerify {
		tlsConfig["insecure_skip_verify"] = true
	}
	if tls.SecretName == "" {
		return tlsConfig, nil
	}
	if err := ValidateTLSSecret(secret); err != nil {
		return nil, err
	}
	if _, ok := secret.Data[tlsCAFileKey]; ok {
		tlsConfig["ca_file"] = fmt.Sprintf("%s/%s", OpAMPBridgeTLSMountPath, tlsCAFileKey)
	}
	if _, ok := secret.Data[tlsCertFileKey]; ok {
		tlsConfig["cert_file"] = fmt.Sprintf("%s/%s", OpAMPBridgeTLSMountPath, tlsCertFileKey)
	}
	if _, ok := secret.Data[tlsKeyFileKey]; ok {
		tlsConfig["key_file"] = fmt.Sprintf("%s/%s", OpAMPBridgeTLSMountPath, tlsKeyFileKey)
	}
	return tlsConfig, nil
}
