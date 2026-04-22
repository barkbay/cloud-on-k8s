// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package fixtures

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	esv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
)

// ClusterSnapshot lists every Deployment, ConfigMap, and Secret in the given
// Elasticsearch's namespace that carries the cluster-name label, projects
// them to a stateless-driver-focused shape, and returns a stable JSON
// representation together with a summary of the reconciliation result.
//
// The projection keeps only the fields whose behaviour the stateless driver
// is responsible for (labels, owner refs, replicas, strategy, volume wiring,
// container image). Full Secret/ConfigMap data blobs are replaced with a
// short content hash — the immutable-config unit tests already prove that
// hashing is stable, so here we only care "same or different" across passes.
func ClusterSnapshot(t *testing.T, c k8s.Client, es esv1.Elasticsearch, res *reconciler.Results) []byte {
	t.Helper()
	ctx := context.Background()
	clusterSelector := label.NewLabelSelectorForElasticsearchClusterName(es.Name)

	var depList appsv1.DeploymentList
	require.NoError(t, c.List(ctx, &depList, client.InNamespace(es.Namespace), clusterSelector))
	var cmList corev1.ConfigMapList
	require.NoError(t, c.List(ctx, &cmList, client.InNamespace(es.Namespace), clusterSelector))
	var secretList corev1.SecretList
	require.NoError(t, c.List(ctx, &secretList, client.InNamespace(es.Namespace), clusterSelector))

	deployments := make([]deploymentSnapshot, 0, len(depList.Items))
	for i := range depList.Items {
		deployments = append(deployments, projectDeployment(&depList.Items[i]))
	}
	configMaps := make([]immutableResourceSnapshot, 0, len(cmList.Items))
	for i := range cmList.Items {
		cm := &cmList.Items[i]
		configMaps = append(configMaps, projectImmutable(&cm.ObjectMeta, stringMapKeys(cm.Data), cm.Immutable, hashStringMap(cm.Data)))
	}
	secrets := make([]immutableResourceSnapshot, 0, len(secretList.Items))
	for i := range secretList.Items {
		s := &secretList.Items[i]
		secrets = append(secrets, projectImmutable(&s.ObjectMeta, byteMapKeys(s.Data), s.Immutable, hashByteMap(s.Data)))
	}

	sortBy(deployments, func(d deploymentSnapshot) string { return d.Namespace + "/" + d.Name })
	sortBy(configMaps, func(x immutableResourceSnapshot) string { return x.Namespace + "/" + x.Name })
	sortBy(secrets, func(x immutableResourceSnapshot) string { return x.Namespace + "/" + x.Name })

	reconciled, reason := res.IsReconciled()
	snap := struct {
		Deployments []deploymentSnapshot        `json:"deployments"`
		ConfigMaps  []immutableResourceSnapshot `json:"configMaps"`
		Secrets     []immutableResourceSnapshot `json:"secrets"`
		Result      reconcileSummary            `json:"result"`
	}{
		Deployments: deployments,
		ConfigMaps:  configMaps,
		Secrets:     secrets,
		Result: reconcileSummary{
			Reconciled: reconciled,
			Reason:     reason,
			HasError:   res.HasError(),
			HasRequeue: res.HasRequeue(),
		},
	}

	raw, err := json.MarshalIndent(snap, "", "  ")
	require.NoError(t, err)
	return raw
}

// deploymentSnapshot is the stateless-driver-focused projection of an
// appsv1.Deployment. It captures the contract the driver is responsible for
// without dragging in pod-spec-level boilerplate (env vars, probes,
// resources) that belongs to other packages' tests.
type deploymentSnapshot struct {
	Namespace       string                    `json:"namespace"`
	Name            string                    `json:"name"`
	Labels          map[string]string         `json:"labels,omitempty"`
	Annotations     map[string]string         `json:"annotations,omitempty"`
	OwnerRefs       []ownerRefSummary         `json:"ownerReferences,omitempty"`
	Replicas        *int32                    `json:"replicas"`
	RevHistoryLimit *int32                    `json:"revisionHistoryLimit,omitempty"`
	Strategy        appsv1.DeploymentStrategy `json:"strategy"`
	Selector        map[string]string         `json:"selector,omitempty"`
	Template        podTemplateProjection     `json:"template"`
}

// podTemplateProjection keeps only the pod-template fields the stateless
// driver is responsible for. TopologySpreadConstraints are kept because
// zone-awareness is a visible driver feature; Affinity is kept for the same
// reason.
type podTemplateProjection struct {
	Labels                    map[string]string                 `json:"labels,omitempty"`
	Annotations               map[string]string                 `json:"annotations,omitempty"`
	Containers                []containerProjection             `json:"containers"`
	Volumes                   []volumeProjection                `json:"volumes,omitempty"`
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
}

// containerProjection keeps only what determines tier behaviour: name and
// image (so upgrade tests can prove the new image is rolled out). Volume
// mounts are kept because the immutable-config wiring is one of the central
// stateless responsibilities.
type containerProjection struct {
	Name         string               `json:"name"`
	Image        string               `json:"image"`
	VolumeMounts []corev1.VolumeMount `json:"volumeMounts,omitempty"`
}

// volumeProjection captures the source kind + referenced name of a Volume so
// snapshots can detect rotation of immutable Secrets/ConfigMaps without
// including the volume's low-level fields.
type volumeProjection struct {
	Name       string `json:"name"`
	Source     string `json:"source"`               // e.g. "secret", "configMap", "emptyDir"
	Referenced string `json:"referenced,omitempty"` // e.g. the Secret/ConfigMap name
}

// immutableResourceSnapshot is a projection shared by immutable Secrets and
// ConfigMaps. We keep metadata + the names of the data keys + a short hash
// of the content so a rotation shows up as "same name, different hash" or
// "new name, new hash".
type immutableResourceSnapshot struct {
	Namespace   string            `json:"namespace"`
	Name        string            `json:"name"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
	OwnerRefs   []ownerRefSummary `json:"ownerReferences,omitempty"`
	Immutable   *bool             `json:"immutable,omitempty"`
	DataKeys    []string          `json:"dataKeys,omitempty"`
	DataHash    string            `json:"dataHash,omitempty"`
}

// ownerRefSummary is a minimal OwnerReference projection without the UID the
// fake client generates.
type ownerRefSummary struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	Controller *bool  `json:"controller,omitempty"`
}

// reconcileSummary captures the public-API-visible parts of a
// *reconciler.Results: whether it errored, whether it requeues, and the
// reconciled-state reason.
type reconcileSummary struct {
	Reconciled bool   `json:"reconciled"`
	Reason     string `json:"reason,omitempty"`
	HasError   bool   `json:"hasError"`
	HasRequeue bool   `json:"hasRequeue"`
}

// projectDeployment returns a deployment-snapshot view of the given
// Deployment, sorting any list-typed subfield to keep the snapshot stable.
func projectDeployment(d *appsv1.Deployment) deploymentSnapshot {
	var selector map[string]string
	if d.Spec.Selector != nil {
		selector = d.Spec.Selector.MatchLabels
	}
	containers := make([]containerProjection, 0, len(d.Spec.Template.Spec.Containers))
	for _, c := range d.Spec.Template.Spec.Containers {
		mounts := append([]corev1.VolumeMount(nil), c.VolumeMounts...)
		sort.Slice(mounts, func(i, j int) bool { return mounts[i].Name < mounts[j].Name })
		containers = append(containers, containerProjection{
			Name:         c.Name,
			Image:        c.Image,
			VolumeMounts: mounts,
		})
	}
	sort.Slice(containers, func(i, j int) bool { return containers[i].Name < containers[j].Name })

	volumes := make([]volumeProjection, 0, len(d.Spec.Template.Spec.Volumes))
	for _, v := range d.Spec.Template.Spec.Volumes {
		volumes = append(volumes, projectVolume(v))
	}
	sort.Slice(volumes, func(i, j int) bool { return volumes[i].Name < volumes[j].Name })

	tsc := append([]corev1.TopologySpreadConstraint(nil), d.Spec.Template.Spec.TopologySpreadConstraints...)
	sort.Slice(tsc, func(i, j int) bool {
		return tsc[i].TopologyKey+string(tsc[i].WhenUnsatisfiable) < tsc[j].TopologyKey+string(tsc[j].WhenUnsatisfiable)
	})

	return deploymentSnapshot{
		Namespace:       d.Namespace,
		Name:            d.Name,
		Labels:          d.Labels,
		Annotations:     d.Annotations,
		OwnerRefs:       projectOwnerRefs(d.OwnerReferences),
		Replicas:        d.Spec.Replicas,
		RevHistoryLimit: d.Spec.RevisionHistoryLimit,
		Strategy:        d.Spec.Strategy,
		Selector:        selector,
		Template: podTemplateProjection{
			Labels:                    d.Spec.Template.Labels,
			Annotations:               d.Spec.Template.Annotations,
			Containers:                containers,
			Volumes:                   volumes,
			TopologySpreadConstraints: tsc,
		},
	}
}

// projectVolume extracts the source kind and referenced name from a Volume.
// Only the sources the stateless driver uses today are recognised; the
// default branch just records the source kind so unexpected additions are
// visible in the snapshot diff.
func projectVolume(v corev1.Volume) volumeProjection {
	switch {
	case v.Secret != nil:
		return volumeProjection{Name: v.Name, Source: "secret", Referenced: v.Secret.SecretName}
	case v.ConfigMap != nil:
		return volumeProjection{Name: v.Name, Source: "configMap", Referenced: v.ConfigMap.Name}
	case v.EmptyDir != nil:
		return volumeProjection{Name: v.Name, Source: "emptyDir"}
	case v.PersistentVolumeClaim != nil:
		return volumeProjection{Name: v.Name, Source: "persistentVolumeClaim", Referenced: v.PersistentVolumeClaim.ClaimName}
	case v.DownwardAPI != nil:
		return volumeProjection{Name: v.Name, Source: "downwardAPI"}
	case v.Projected != nil:
		return volumeProjection{Name: v.Name, Source: "projected"}
	case v.Ephemeral != nil:
		return volumeProjection{Name: v.Name, Source: "ephemeral"}
	default:
		return volumeProjection{Name: v.Name, Source: "other"}
	}
}

// projectImmutable builds an immutableResourceSnapshot from the three inputs
// pulled out of each Secret/ConfigMap: its metadata, its data keys, and a
// content hash to distinguish rotated revisions.
func projectImmutable(m *metav1.ObjectMeta, keys []string, immutable *bool, hash string) immutableResourceSnapshot {
	return immutableResourceSnapshot{
		Namespace:   m.Namespace,
		Name:        m.Name,
		Labels:      m.Labels,
		Annotations: m.Annotations,
		OwnerRefs:   projectOwnerRefs(m.OwnerReferences),
		Immutable:   immutable,
		DataKeys:    keys,
		DataHash:    hash,
	}
}

// projectOwnerRefs strips the UIDs the fake client generates on every
// OwnerReference while preserving the fields that reflect stateless-driver
// intent (owner kind, name, controller marker).
func projectOwnerRefs(refs []metav1.OwnerReference) []ownerRefSummary {
	out := make([]ownerRefSummary, 0, len(refs))
	for _, r := range refs {
		out = append(out, ownerRefSummary{
			APIVersion: r.APIVersion,
			Kind:       r.Kind,
			Name:       r.Name,
			Controller: r.Controller,
		})
	}
	return out
}

// hashStringMap returns a short, sorted-key hash of a string-keyed string
// map. Used to distinguish rotated immutable ConfigMaps.
func hashStringMap(m map[string]string) string {
	keys := stringMapKeys(m)
	h := sha256.New()
	for _, k := range keys {
		_, _ = h.Write([]byte(k))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(m[k]))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:12]
}

// hashByteMap is the byte-valued counterpart of hashStringMap for Secrets.
func hashByteMap(m map[string][]byte) string {
	keys := byteMapKeys(m)
	h := sha256.New()
	for _, k := range keys {
		_, _ = h.Write([]byte(k))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write(m[k])
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:12]
}

func stringMapKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func byteMapKeys(m map[string][]byte) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// sortBy sorts a slice in-place using the given string key accessor.
func sortBy[T any](items []T, key func(T) string) {
	sort.Slice(items, func(i, j int) bool { return key(items[i]) < key(items[j]) })
}
