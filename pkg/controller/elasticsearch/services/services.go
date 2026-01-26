// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package services

import (
	"fmt"
	"math/rand"
	"strconv"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/defaults"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/metadata"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/name"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/network"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/stringsutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	globalServiceSuffix = ".svc"

	RemoteClusterServicePortName = "rcs"
)

// NewTransportService returns the transport service associated with the given cluster.
// It is used by Elasticsearch nodes to talk to remote cluster nodes.
func NewTransportService(es escommon.ElasticsearchCluster, meta metadata.Metadata) *corev1.Service {
	nsn := k8s.ExtractNamespacedName(es)
	svc := corev1.Service{
		ObjectMeta: es.GetTransport().Service.ObjectMeta,
		Spec:       es.GetTransport().Service.Spec,
	}

	svc.ObjectMeta.Namespace = es.GetNamespace()
	svc.ObjectMeta.Name = escommon.TransportService(es)
	// Nodes need to discover themselves before the pod is considered ready,
	// otherwise minimum master nodes would never be reached
	svc.Spec.PublishNotReadyAddresses = true
	if svc.Spec.Type == "" {
		svc.Spec.Type = corev1.ServiceTypeClusterIP
		// We set ClusterIP to None in order to let the ES nodes discover all other node IPs at once.
		svc.Spec.ClusterIP = "None"
	}
	selector := label.NewLabels(nsn, es.IsStateless())
	ports := []corev1.ServicePort{
		{
			Name:     "tls-transport", // prefix with protocol for Istio compatibility
			Protocol: corev1.ProtocolTCP,
			Port:     network.TransportPort,
		},
	}

	return defaults.SetServiceDefaults(&svc, meta, selector, ports)
}

// ExternalTransportServiceHost returns the hostname and the port used to reach Elasticsearch's transport endpoint.
// Note: This function takes a NamespacedName (not ElasticsearchCluster) because it's used for remote cluster
// references where we only have the name/namespace of the target cluster.
func ExternalTransportServiceHost(es escommon.ElasticsearchCluster) string {
	namer := escommon.StatefulNamer
	if es.IsStateless() {
		namer = escommon.StatelessNamer
	}
	return stringsutil.Concat(namer.Suffix(es.GetName(), escommon.TransportServiceSuffix), ".", es.GetNamespace(), globalServiceSuffix, ":", strconv.Itoa(network.TransportPort))
}

// ExternalTransportServiceHostWithKind returns the hostname and the port used to reach Elasticsearch's transport endpoint,
// supporting both stateful Elasticsearch and stateless ElasticsearchStateless kinds based on the ref's Kind field.
func ExternalTransportServiceHostWithKind(ref commonv1.LocalElasticsearchRef) string {
	namer := namerForKind(ref.Kind)
	return stringsutil.Concat(namer.Suffix(ref.Name, escommon.TransportServiceSuffix), ".", ref.Namespace, globalServiceSuffix, ":", strconv.Itoa(network.TransportPort))
}

// RemoteClusterServerServiceHostWithKind returns the hostname and the port used to reach Elasticsearch's remote cluster server endpoint,
// supporting both stateful Elasticsearch and stateless ElasticsearchStateless kinds based on the ref's Kind field.
func RemoteClusterServerServiceHostWithKind(ref commonv1.LocalElasticsearchRef) string {
	namer := namerForKind(ref.Kind)
	return stringsutil.Concat(namer.Suffix(ref.Name, escommon.RemoteClusterServiceSuffix), ".", ref.Namespace, globalServiceSuffix, ":", strconv.Itoa(network.RemoteClusterPort))
}

// namerForKind returns the appropriate namer based on the Elasticsearch kind.
func namerForKind(kind string) name.Namer {
	if kind == commonv1.ElasticsearchStatelessKind {
		return escommon.StatelessNamer
	}
	return escommon.StatefulNamer
}

// ExternalServiceURL returns the URL used to reach Elasticsearch's external endpoint.
func ExternalServiceURL(es escommon.ElasticsearchCluster) string {
	return stringsutil.Concat(es.GetHTTP().Protocol(), "://", escommon.HTTPService(es), ".", es.GetNamespace(), globalServiceSuffix, ":", strconv.Itoa(network.HTTPPort))
}

// InternalServiceURL returns the URL used to reach Elasticsearch's internally managed service
func InternalServiceURL(es escommon.ElasticsearchCluster) string {
	return stringsutil.Concat(es.GetHTTP().Protocol(), "://", escommon.InternalHTTPService(es), ".", es.GetNamespace(), globalServiceSuffix, ":", strconv.Itoa(network.HTTPPort))
}

// NewExternalService returns the external service associated to the given cluster.
// It is used by users to perform requests against one of the cluster nodes.
func NewExternalService(es escommon.ElasticsearchCluster, meta metadata.Metadata) *corev1.Service {
	nsn := k8s.ExtractNamespacedName(es)

	svc := corev1.Service{
		ObjectMeta: es.GetHTTP().Service.ObjectMeta,
		Spec:       es.GetHTTP().Service.Spec,
	}

	svc.ObjectMeta.Namespace = es.GetNamespace()
	svc.ObjectMeta.Name = escommon.HTTPService(es)

	// defaults to ClusterIP if not set
	if svc.Spec.Type == "" {
		svc.Spec.Type = corev1.ServiceTypeClusterIP
	}
	selector := label.NewLabels(nsn, es.IsStateless())
	ports := []corev1.ServicePort{
		{
			Name:     es.GetHTTP().Protocol(),
			Protocol: corev1.ProtocolTCP,
			Port:     network.HTTPPort,
		},
	}

	return defaults.SetServiceDefaults(&svc, meta, selector, ports)
}

// NewInternalService returns the internal service associated to the given cluster.
// It is used by the operator to perform requests against the Elasticsearch cluster nodes,
// and does not inherit the spec defined within the Elasticsearch custom resource,
// to remove the possibility of the user misconfiguring access to the ES cluster.
func NewInternalService(es escommon.ElasticsearchCluster, meta metadata.Metadata) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        escommon.InternalHTTPService(es),
			Namespace:   es.GetNamespace(),
			Labels:      meta.Labels,
			Annotations: meta.Annotations,
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{
					Name:     es.GetHTTP().Protocol(),
					Protocol: corev1.ProtocolTCP,
					Port:     network.HTTPPort,
				},
			},
			Selector:                 label.NewLabels(k8s.ExtractNamespacedName(es), es.IsStateless()),
			PublishNotReadyAddresses: false,
		},
	}
}

// NewRemoteClusterService returns the service associated to the remote cluster service for the given cluster.
func NewRemoteClusterService(es escommon.ElasticsearchCluster, meta metadata.Metadata) *corev1.Service {
	nsn := k8s.ExtractNamespacedName(es)
	svc := corev1.Service{
		ObjectMeta: es.GetRemoteClusterServer().Service.ObjectMeta,
		Spec:       es.GetRemoteClusterServer().Service.Spec,
	}

	svc.ObjectMeta.Namespace = es.GetNamespace()
	svc.ObjectMeta.Name = escommon.RemoteClusterService(es)
	// Allow connections to pods that are not yet ready
	svc.Spec.PublishNotReadyAddresses = true
	if svc.Spec.Type == "" {
		svc.Spec.Type = corev1.ServiceTypeClusterIP
		// ClusterIP None creates a headless service, allowing direct access to all pods for remote cluster connections
		svc.Spec.ClusterIP = "None"
	}
	selector := label.NewLabels(nsn, es.IsStateless())
	ports := []corev1.ServicePort{
		{
			Name:     RemoteClusterServicePortName,
			Protocol: corev1.ProtocolTCP,
			Port:     network.RemoteClusterPort,
		},
	}

	return defaults.SetServiceDefaults(&svc, meta, selector, ports)
}

type urlProvider struct {
	pods   func() ([]corev1.Pod, error)
	svcURL string
}

// URL implements client.URLProvider.
func (u *urlProvider) URL() (string, error) {
	var ready, running []corev1.Pod
	pods, err := u.pods()
	if err != nil {
		return "", err
	}
	for _, p := range pods {
		if k8s.IsPodReady(p) {
			ready = append(ready, p)
		}
		if k8s.IsPodRunning(p) {
			running = append(running, p)
		}
	}
	switch {
	case len(ready) > 0:
		return randomESPodURL(ready), nil
	case len(running) > 0:
		return randomESPodURL(running), nil
	default:
		return u.svcURL, nil
	}
}

// Equals implements client.URLProvider.
func (u *urlProvider) Equals(other client.URLProvider) bool {
	otherImpl, ok := other.(*urlProvider)
	if !ok {
		return false
	}
	return u.svcURL == otherImpl.svcURL
}

// HasEndpoints implements client.URLProvider.
func (u *urlProvider) HasEndpoints() bool {
	pods, err := u.pods()
	return err == nil && len(k8s.RunningPods(pods)) > 0
}

// NewElasticsearchURLProvider returns a client.URLProvider that dynamically tries to find Pod URLs among the
// currently running Pods. Preferring ready Pods over running ones.
func NewElasticsearchURLProvider(es escommon.ElasticsearchCluster, client k8s.Client) client.URLProvider {
	return &urlProvider{
		pods: func() ([]corev1.Pod, error) {
			return k8s.PodsMatchingLabels(client, es.GetNamespace(), label.NewLabelSelectorForElasticsearch(es))
		},
		svcURL: InternalServiceURL(es),
	}
}

func randomESPodURL(pods []corev1.Pod) string {
	randomPod := pods[rand.Intn(len(pods))] //nolint:gosec
	return ElasticsearchPodURL(randomPod)
}

// ElasticsearchPodURL calculates the URL for the given Pod based on the Pods metadata.
func ElasticsearchPodURL(pod corev1.Pod) string {
	scheme, hasSchemeLabel := pod.Labels[label.HTTPSchemeLabelName]
	sset, hasSsetLabel := pod.Labels[label.StatefulSetNameLabelName]
	if hasSsetLabel && hasSchemeLabel {
		return fmt.Sprintf("%s://%s.%s.%s:%d", scheme, pod.Name, sset, pod.Namespace, network.HTTPPort)
	}
	return ""
}
