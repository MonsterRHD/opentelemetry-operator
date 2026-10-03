// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package operator

import (
	"context"
	stderrors "errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/open-telemetry/opamp-go/protobufs"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"github.com/open-telemetry/opentelemetry-operator/apis/v1beta1"
	"github.com/open-telemetry/opentelemetry-operator/cmd/operator-opamp-bridge/internal/rollout"
	"github.com/open-telemetry/opentelemetry-operator/internal/naming"
)

const (
	CollectorResource       = "OpenTelemetryCollector"
	ResourceIdentifierKey   = "created-by"
	ResourceIdentifierValue = "operator-opamp-bridge"
	ReportingLabelKey       = "opentelemetry.io/opamp-reporting"
	ManagedLabelKey         = "opentelemetry.io/opamp-managed"
)

type ConfigApplier interface {
	// ApplyConfig applies one complete remote configuration snapshot. entries contains
	// every config object in the OpAMP AgentConfigMap; deletedKeys lists keys of the
	// previously applied snapshot that are absent from entries. All entries are validated
	// before any Kubernetes write, and a failed write rolls back every write made during
	// the call, so managed resources stay at the previously applied snapshot.
	ApplyConfig(entries map[string]*protobufs.AgentConfigObject, deletedKeys []string) error

	// Restart triggers a rolling restart of the managed collector workload(s) in response to
	// an OpAMP ServerToAgentCommand with type CommandType_Restart.
	Restart(ctx context.Context) error

	// ListInstances retrieves all collector instances managed by the bridge.
	ListInstances() ([]CollectorInstance, error)

	// GetHealth retrieves the health of resources managed by the bridge.
	GetHealth() (Health, error)
}

type Client struct {
	log               logr.Logger
	componentsAllowed map[string]map[string]bool
	k8sClient         client.Client
	close             chan bool
	name              string
}

var _ ConfigApplier = &Client{}

func NewClient(name string, log logr.Logger, c client.Client, componentsAllowed map[string]map[string]bool) *Client {
	return &Client{
		log:               log,
		componentsAllowed: componentsAllowed,
		k8sClient:         c,
		close:             make(chan bool, 1),
		name:              name,
	}
}

// preparedChange is one validated remote config entry together with the collector currently
// in the cluster, which is nil when no collector with the key exists yet.
type preparedChange struct {
	name      string
	namespace string
	desired   *v1beta1.OpenTelemetryCollector
	existing  *v1beta1.OpenTelemetryCollector
}

func (c Client) prepareChange(key string, configmap *protobufs.AgentConfigObject) (preparedChange, error) {
	resource, err := kubeResourceFromKey(key)
	if err != nil {
		return preparedChange{}, err
	}
	name, namespace := resource.name, resource.namespace

	if len(configmap.Body) == 0 {
		return preparedChange{}, errors.NewBadRequest("invalid config to apply: config is empty")
	}

	var collector v1beta1.OpenTelemetryCollector
	err = yaml.Unmarshal(configmap.Body, &collector)
	if err != nil {
		return preparedChange{}, errors.NewBadRequest(fmt.Sprintf("failed to unmarshal config into v1beta1 API Version: %v", err))
	}

	err = c.validateComponents(&collector.Spec.Config)
	if err != nil {
		return preparedChange{}, err
	}

	instance, err := c.GetInstance(name, namespace)
	if err != nil {
		return preparedChange{}, err
	}

	err = c.validateLabels(instance)
	if err != nil {
		return preparedChange{}, err
	}
	err = c.validateLabels(&collector)
	if err != nil {
		return preparedChange{}, err
	}

	return preparedChange{
		name:      name,
		namespace: namespace,
		desired:   &collector,
		existing:  instance,
	}, nil
}

func (c Client) Apply(key string, configmap *protobufs.AgentConfigObject) error {
	change, err := c.prepareChange(key, configmap)
	if err != nil {
		return err
	}
	c.log.Info("Received new config", "name", change.name, "namespace", change.namespace)

	ctx := context.Background()
	if change.existing == nil {
		return c.create(ctx, change.name, change.namespace, change.desired)
	}
	return c.update(ctx, change.existing, change.desired)
}

// collectorDeletion pairs a key the snapshot removes with the collector that currently
// carries it, nil when the collector no longer exists and the delete is a no-op.
type collectorDeletion struct {
	key      string
	existing *v1beta1.OpenTelemetryCollector
}

// ApplyConfig applies the whole remote config snapshot. Every entry is validated and the
// collectors it would remove are captured before any write; a failed create, update, or
// delete restores all collectors changed during the call to their previous state.
func (c Client) ApplyConfig(entries map[string]*protobufs.AgentConfigObject, deletedKeys []string) error {
	entryKeys := make([]string, 0, len(entries))
	for key := range entries {
		entryKeys = append(entryKeys, key)
	}
	slices.Sort(entryKeys)

	changes := make([]preparedChange, 0, len(entryKeys))
	for _, key := range entryKeys {
		change, err := c.prepareChange(key, entries[key])
		if err != nil {
			return err
		}
		changes = append(changes, change)
	}

	sortedDeletedKeys := slices.Clone(deletedKeys)
	slices.Sort(sortedDeletedKeys)
	deletions := make([]collectorDeletion, 0, len(sortedDeletedKeys))
	for _, key := range sortedDeletedKeys {
		resource, err := kubeResourceFromKey(key)
		if err != nil {
			return err
		}
		existing, err := c.GetInstance(resource.name, resource.namespace)
		if err != nil {
			return err
		}
		deletions = append(deletions, collectorDeletion{key: key, existing: existing})
	}

	// All entries are validated and prior state captured. Any failure from here on undoes
	// the writes already made in this snapshot, in reverse order.
	var rollbacks []func() error
	rollbackAll := func(applyErr error) error {
		for i := len(rollbacks) - 1; i >= 0; i-- {
			if err := rollbacks[i](); err != nil {
				c.log.Error(err, "failed to roll back remote config snapshot")
			}
		}
		return applyErr
	}

	ctx := context.Background()
	for i := range changes {
		change := changes[i]
		if change.existing == nil {
			if err := c.create(ctx, change.name, change.namespace, change.desired); err != nil {
				return rollbackAll(err)
			}
			key := NewKubeResourceKey(change.namespace, change.name).String()
			rollbacks = append(rollbacks, func() error { return c.Delete(key) })
			continue
		}
		original := change.existing.DeepCopy()
		if err := c.update(ctx, change.existing, change.desired); err != nil {
			return rollbackAll(err)
		}
		rollbacks = append(rollbacks, func() error { return c.restoreCollector(original) })
	}
	for i := range deletions {
		deletion := deletions[i]
		if deletion.existing == nil {
			continue
		}
		original := deletion.existing.DeepCopy()
		if err := c.Delete(deletion.key); err != nil {
			return rollbackAll(err)
		}
		rollbacks = append(rollbacks, func() error { return c.recreateCollector(original) })
	}
	return nil
}

// restoreCollector overwrites a collector updated during a failed snapshot with its
// previous state. ResourceVersion is cleared because the snapshot write advanced it.
func (c Client) restoreCollector(original *v1beta1.OpenTelemetryCollector) error {
	restored := original.DeepCopy()
	restored.ResourceVersion = ""
	if err := c.k8sClient.Update(context.Background(), restored); err != nil {
		return fmt.Errorf("failed to restore collector %s during rollback: %w", NewKubeResourceKey(restored.Namespace, restored.Name).String(), err)
	}
	return nil
}

// recreateCollector creates a collector deleted during a failed snapshot. Server-assigned
// fields and status are cleared so Create accepts the object.
func (c Client) recreateCollector(original *v1beta1.OpenTelemetryCollector) error {
	restored := original.DeepCopy()
	restored.ResourceVersion = ""
	restored.UID = ""
	restored.Status = v1beta1.OpenTelemetryCollectorStatus{}
	if err := c.k8sClient.Create(context.Background(), restored); err != nil {
		return fmt.Errorf("failed to recreate collector %s during rollback: %w", NewKubeResourceKey(restored.Namespace, restored.Name).String(), err)
	}
	return nil
}

func (c Client) validateComponents(collectorConfig *v1beta1.Config) error {
	if len(c.componentsAllowed) == 0 {
		return nil
	}

	configuredComponents := map[string]map[string]any{
		"receivers": collectorConfig.Receivers.Object,
		"exporters": collectorConfig.Exporters.Object,
	}
	if collectorConfig.Processors != nil {
		configuredComponents["processors"] = collectorConfig.Processors.Object
	}

	var invalidComponents []string
	for component, componentMap := range configuredComponents {
		if _, ok := c.componentsAllowed[component]; !ok {
			invalidComponents = append(invalidComponents, component)
			continue
		}
		for componentName := range componentMap {
			if _, ok := c.componentsAllowed[component][componentName]; !ok {
				invalidComponents = append(invalidComponents, fmt.Sprintf("%s.%s", component, componentName))
			}
		}
	}

	if len(invalidComponents) > 0 {
		return errors.NewBadRequest(fmt.Sprintf("Items in config are not allowed: %v", invalidComponents))
	}

	return nil
}

func (c Client) validateLabels(collector *v1beta1.OpenTelemetryCollector) error {
	if collector == nil {
		return nil
	}

	resourceLabels := collector.GetLabels()

	// If either the received collector resource has labels indicating it should only report and is not managed,
	// disallow applying the new collector config
	if labelSetContainsLabel(resourceLabels, ReportingLabelKey, "true") {
		return errors.NewBadRequest(fmt.Sprintf("cannot modify a collector with `%s: true`", ReportingLabelKey))
	}

	// If either the collector doesn't have the managed label set to true, it should disallow applying the new collector
	// config
	if !labelSetContainsLabel(resourceLabels, ManagedLabelKey, "true") &&
		!labelSetContainsLabel(resourceLabels, ManagedLabelKey, c.name) {
		return errors.NewBadRequest(fmt.Sprintf("cannot modify a collector that doesn't have `%s: true | <bridge-name>` set", ManagedLabelKey))
	}

	return nil
}

func labelSetContainsLabel(resourceLabelSet map[string]string, label, value string) bool {
	if len(resourceLabelSet) == 0 {
		return false
	}
	return strings.EqualFold(resourceLabelSet[label], value)
}

func (c Client) create(ctx context.Context, name, namespace string, collector *v1beta1.OpenTelemetryCollector) error {
	// Set the defaults
	setTypedMeta(collector)
	collector.Name = name
	collector.Namespace = namespace

	if collector.Labels == nil {
		collector.Labels = map[string]string{}
	}
	collector.Labels[ResourceIdentifierKey] = ResourceIdentifierValue

	c.log.Info("Creating collector")
	return c.k8sClient.Create(ctx, collector)
}

func (c Client) update(ctx context.Context, o, n *v1beta1.OpenTelemetryCollector) error {
	n.ObjectMeta = o.ObjectMeta
	n.TypeMeta = o.TypeMeta

	c.log.Info("Updating collector")
	return c.k8sClient.Update(ctx, n)
}

func (c Client) Delete(key string) error {
	resource, err := kubeResourceFromKey(key)
	if err != nil {
		return err
	}
	ctx := context.Background()
	result := v1beta1.OpenTelemetryCollector{}
	err = c.k8sClient.Get(ctx, client.ObjectKey{
		Namespace: resource.namespace,
		Name:      resource.name,
	}, &result)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return c.k8sClient.Delete(ctx, &result)
}

// Restart triggers a rolling restart of all managed collector workloads.
// Sidecar mode collectors are skipped (they have no standalone workload).
// All other collectors are restarted via rollout.TriggerRollout; errors are joined and returned.
func (c Client) Restart(ctx context.Context) error {
	collectors, err := c.listOpenTelemetryCollectors()
	if err != nil {
		return fmt.Errorf("failed to list collectors for restart: %w", err)
	}
	var errs []error
	for i := range collectors {
		col := &collectors[i]
		mode := strings.ToLower(string(col.Col.Spec.Mode))
		if mode == "sidecar" {
			c.log.Info("Skipping restart for sidecar mode collector - no standalone workload",
				"name", col.GetName(), "namespace", col.GetNamespace())
			continue
		}
		workloadName := naming.Collector(col.GetName())
		if err := rollout.TriggerRollout(ctx, c.k8sClient, col.GetNamespace(), mode, workloadName); err != nil {
			errs = append(errs, err)
		} else {
			c.log.Info("Triggered workload rollout restart", "mode", mode, "name", workloadName, "namespace", col.GetNamespace())
		}
	}
	return stderrors.Join(errs...)
}

// ListInstances returns all collectors that are visible to OpAMP as effective config entries.
func (c Client) ListInstances() ([]CollectorInstance, error) {
	collectors, err := c.listOpenTelemetryCollectors()
	if err != nil {
		return nil, err
	}
	result := make([]CollectorInstance, len(collectors))
	for i := range collectors {
		result[i] = collectors[i]
	}
	return result, nil
}

// GetHealth reports bridge root health with managed collector health as children.
func (c Client) GetHealth() (Health, error) {
	collectors, err := c.listOpenTelemetryCollectors()
	if err != nil {
		return Health{}, err
	}
	healthMap := map[string]Health{}
	for _, col := range collectors {
		podMap, err := c.generateCollectorHealth(col.selectorLabels(), col.GetNamespace())
		if err != nil {
			return Health{}, err
		}
		isPoolHealthy := true
		for _, pod := range podMap {
			isPoolHealthy = isPoolHealthy && pod.Healthy
		}
		healthMap[NewKubeResourceKey(col.GetNamespace(), col.GetName()).String()] = Health{
			StartTime: col.Col.GetCreationTimestamp().Time,
			Status:    col.Col.Status.Scale.StatusReplicas,
			Children:  podMap,
			Healthy:   isPoolHealthy,
		}
	}
	return Health{
		Healthy:  true,
		Children: healthMap,
	}, nil
}

// generateCollectorHealth reports pod health for one collector selected by labels within a namespace.
func (c Client) generateCollectorHealth(selectorLabels map[string]string, namespace string) (map[string]Health, error) {
	pods, err := c.getCollectorPods(selectorLabels, namespace)
	if err != nil {
		return nil, err
	}
	healthMap := map[string]Health{}
	for _, item := range pods.Items {
		key := NewKubeResourceKey(item.GetNamespace(), item.GetName())
		healthy := true
		if item.Status.Phase != "Running" {
			healthy = false
		}
		var startTime time.Time
		if item.Status.StartTime != nil {
			startTime = item.Status.StartTime.Time
		} else {
			healthy = false
		}
		healthMap[key.String()] = Health{
			StartTime: startTime,
			Status:    string(item.Status.Phase),
			Healthy:   healthy,
			Children:  map[string]Health{},
		}
	}
	return healthMap, nil
}

// listOpenTelemetryCollectors returns collectors managed by this bridge plus reporting-only collectors.
func (c Client) listOpenTelemetryCollectors() ([]CRDInstance, error) {
	ctx := context.Background()

	labelSelector := labels.NewSelector()
	requirement, err := labels.NewRequirement(ManagedLabelKey, selection.In, []string{c.name, "true"})
	if err != nil {
		return nil, err
	}
	managedCollectorLabelSelector := client.MatchingLabelsSelector{Selector: labelSelector.Add(*requirement)}

	managedCollectors := v1beta1.OpenTelemetryCollectorList{}
	err = c.k8sClient.List(ctx, &managedCollectors, managedCollectorLabelSelector)
	if err != nil {
		return nil, err
	}

	reportingCollectorLabelMatcher := client.MatchingLabels{ReportingLabelKey: "true"}
	reportingCollectors := v1beta1.OpenTelemetryCollectorList{}
	err = c.k8sClient.List(ctx, &reportingCollectors, reportingCollectorLabelMatcher)
	if err != nil {
		return nil, err
	}

	result := make([]CRDInstance, 0, len(managedCollectors.Items)+len(reportingCollectors.Items))
	for i := range managedCollectors.Items {
		managedCollectors.Items[i].SetManagedFields(nil)
		setTypedMeta(&managedCollectors.Items[i])
		result = append(result, newCRDInstance(managedCollectors.Items[i], true))
	}
	for i := range reportingCollectors.Items {
		reportingCollectors.Items[i].SetManagedFields(nil)
		setTypedMeta(&reportingCollectors.Items[i])
		result = append(result, newCRDInstance(reportingCollectors.Items[i], false))
	}
	return result, nil
}

func (c Client) GetInstance(name, namespace string) (*v1beta1.OpenTelemetryCollector, error) {
	ctx := context.Background()
	result := v1beta1.OpenTelemetryCollector{}

	err := c.k8sClient.Get(ctx, client.ObjectKey{
		Namespace: namespace,
		Name:      name,
	}, &result)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	setTypedMeta(&result)
	return &result, nil
}

func (c Client) getCollectorPods(selectorLabels map[string]string, namespace string) (*v1.PodList, error) {
	ctx := context.Background()
	podList := &v1.PodList{}
	err := c.k8sClient.List(ctx, podList, client.MatchingLabels(selectorLabels), client.InNamespace(namespace))
	return podList, err
}

// setTypedMeta sets the TypeMeta of the given collector to the correct values. The controller-runtime
// client will not set the TypeMeta for us, so we need to do it manually.
func setTypedMeta(collector *v1beta1.OpenTelemetryCollector) {
	collector.Kind = CollectorResource
	collector.APIVersion = v1beta1.GroupVersion.String()
}
