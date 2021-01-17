package autoscaling

import (
	"context"
	"fmt"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/nodesets"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/status"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/certificates"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/tracing"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/version"
	esclient "github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/services"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/user"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/volume"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	"github.com/elastic/cloud-on-k8s/pkg/utils/net"
	"github.com/go-logr/logr"
	"go.elastic.co/apm"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// reconcileElasticsearch updates the resources in the NodeSets of an Elasticsearch spec according to the NamedTierResources
// computed by the autoscaling algorithm. It also updates the autoscaling status annotation.
func reconcileElasticsearch(
	log logr.Logger,
	es *esv1.Elasticsearch,
	statusBuilder *status.AutoscalingStatusBuilder,
	nextNodeSetsResources nodesets.ClusterResources,
	actualAutoscalingStatus status.Status,
) error {
	nextResourcesByNodeSet := nextNodeSetsResources.ByNodeSet()
	for i := range es.Spec.NodeSets {
		name := es.Spec.NodeSets[i].Name
		nodeSetResources, ok := nextResourcesByNodeSet[name]
		if !ok {
			log.V(1).Info("Skipping nodeset update", "nodeset", name)
			continue
		}

		container, containers := removeContainer(esv1.ElasticsearchContainerName, es.Spec.NodeSets[i].PodTemplate.Spec.Containers)
		// Create a copy to compare if some changes have been made.
		actualContainer := container.DeepCopy()
		if container == nil {
			container = &corev1.Container{
				Name: esv1.ElasticsearchContainerName,
			}
		}

		// Update desired count
		es.Spec.NodeSets[i].Count = nodeSetResources.NodeCount

		if container.Resources.Requests == nil {
			container.Resources.Requests = corev1.ResourceList{}
		}
		if container.Resources.Limits == nil {
			container.Resources.Limits = corev1.ResourceList{}
		}

		// Update memory requests and limits
		if nodeSetResources.HasRequest(corev1.ResourceMemory) {
			container.Resources.Requests[corev1.ResourceMemory] = nodeSetResources.GetRequest(corev1.ResourceMemory)
			//TODO: apply request/memory ratio
			container.Resources.Limits[corev1.ResourceMemory] = nodeSetResources.GetRequest(corev1.ResourceMemory)
		}
		if nodeSetResources.HasRequest(corev1.ResourceCPU) {
			container.Resources.Requests[corev1.ResourceCPU] = nodeSetResources.GetRequest(corev1.ResourceCPU)
			//TODO: apply request/memory ratio
		}

		if nodeSetResources.HasRequest(corev1.ResourceStorage) {
			// Update storage claim
			if len(es.Spec.NodeSets[i].VolumeClaimTemplates) == 0 {
				es.Spec.NodeSets[i].VolumeClaimTemplates = []corev1.PersistentVolumeClaim{*volume.DefaultDataVolumeClaim.DeepCopy()}
			}
			for _, claimTemplate := range es.Spec.NodeSets[i].VolumeClaimTemplates {
				if claimTemplate.Name == volume.ElasticsearchDataVolumeName {
					// Storage may have been managed outside of the scope of the autoscaler, we still want to ensure here that
					// we are not scaling down the storage capacity.
					nextStorage := nodeSetResources.GetRequest(corev1.ResourceStorage)
					actualStorage, hasStorage := claimTemplate.Spec.Resources.Requests[corev1.ResourceStorage]
					if hasStorage && actualStorage.Cmp(nextStorage) > 0 {
						continue
					}
					claimTemplate.Spec.Resources.Requests[corev1.ResourceStorage] = nextStorage
				}
			}
		}

		// Add the container to other containers
		containers = append(containers, *container)
		// Update the NodeSet
		es.Spec.NodeSets[i].PodTemplate.Spec.Containers = containers

		if !apiequality.Semantic.DeepEqual(actualContainer, container) {
			log.V(1).Info("Updating nodeset with resources", "nodeset", name, "resources", nextNodeSetsResources)
		}
	}

	// Update autoscaling status
	return status.UpdateAutoscalingStatus(es, statusBuilder, nextNodeSetsResources, actualAutoscalingStatus)
}

func (r *ReconcileElasticsearch) fetchElasticsearch(
	ctx context.Context,
	request reconcile.Request,
	es *esv1.Elasticsearch,
) (bool, error) {
	span, _ := apm.StartSpan(ctx, "fetch_elasticsearch", tracing.SpanTypeApp)
	defer span.End()

	err := r.Get(request.NamespacedName, es)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		// Error reading the object - requeue the request.
		return true, err
	}
	return false, nil
}

func newElasticsearchClient(
	c k8s.Client,
	dialer net.Dialer,
	es esv1.Elasticsearch,
) (esclient.Client, error) {
	url := services.ExternalServiceURL(es)
	v, err := version.Parse(es.Spec.Version)
	if err != nil {
		return nil, err
	}
	// Get user Secret
	var controllerUserSecret corev1.Secret
	key := types.NamespacedName{
		Namespace: es.Namespace,
		Name:      esv1.InternalUsersSecret(es.Name),
	}
	if err := c.Get(key, &controllerUserSecret); err != nil {
		return nil, err
	}
	password, ok := controllerUserSecret.Data[user.ControllerUserName]
	if !ok {
		return nil, fmt.Errorf("controller user %s not found in Secret %s/%s", user.ControllerUserName, key.Namespace, key.Name)
	}

	// Get public certs
	var caSecret corev1.Secret
	key = types.NamespacedName{
		Namespace: es.Namespace,
		Name:      certificates.PublicCertsSecretName(esv1.ESNamer, es.Name),
	}
	if err := c.Get(key, &caSecret); err != nil {
		return nil, err
	}
	trustedCerts, ok := caSecret.Data[certificates.CertFileName]
	if !ok {
		return nil, fmt.Errorf("%s not found in Secret %s/%s", certificates.CertFileName, key.Namespace, key.Name)
	}
	caCerts, err := certificates.ParsePEMCerts(trustedCerts)

	return esclient.NewElasticsearchClient(
		dialer,
		url,
		esclient.BasicAuth{
			Name:     user.ControllerUserName,
			Password: string(password),
		},
		*v,
		caCerts,
		esclient.Timeout(es),
	), nil
}

// removeContainer remove a container from a slice and return the removed container if found.
func removeContainer(name string, containers []corev1.Container) (*corev1.Container, []corev1.Container) {
	for i, container := range containers {
		if container.Name == name {
			// Remove the container
			return &container, append(containers[:i], containers[i+1:]...)
		}
	}
	return nil, containers
}
