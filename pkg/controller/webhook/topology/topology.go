// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package topology

import (
	"context"
	"sort"
	"strings"

	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	"github.com/elastic/cloud-on-k8s/pkg/utils/set"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	w "sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// +kubebuilder:webhook:path=/pod-binding-k8s-elastic-co,mutating=true,failurePolicy=ignore,groups="",resources=pods/binding,verbs=create,versions=v1,name=pod-binding-k8s-elastic-co,sideEffects=None,admissionReviewVersions=v1;v1beta1,matchPolicy=Exact

const (
	webhookPath = "/pod-binding-k8s-elastic-co"
)

var log = logf.Log.WithName("topology")

type mutatingWebhook struct {
	client               k8s.Client
	decoder              *admission.Decoder
	validateStorageClass bool
}

func (wh *mutatingWebhook) Handle(ctx context.Context, request admission.Request) admission.Response {
	log.Info("binding request", "request", request)
	binding := &v1.Binding{}
	err := wh.decoder.DecodeRaw(request.Object, binding)
	if err != nil {
		log.Error(err, "cannot read binding request")
	}
	log.Info("parsed binding request", "pod", binding.ObjectMeta.Name, "node", binding.Target.Name)

	kv, err := getTopologyKeys(ctx, wh.client, binding.Target.Name)
	if err != nil {
		log.Error(err, "error while getting node labels")
		return admission.Allowed("")
	}

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		// 1. Read Pod specification
		key := client.ObjectKey{Namespace: binding.ObjectMeta.Namespace, Name: binding.ObjectMeta.Name}
		pod := &v1.Pod{}
		if err := wh.client.Get(ctx, key, pod); err != nil {
			log.Error(err, "error while getting Pod")
		}
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		// 2. Add topology keys
		for k, v := range kv {
			pod.Annotations[k] = v
		}
		// 3. Update
		return wh.client.Update(ctx, pod)
	}); err != nil {
		log.Error(err, "error while adding node labels")
		return admission.Allowed("")
	}

	return admission.Allowed("")
}

var (
	// singular -> plural
	topologyKeys = map[string]string{
		"topology.kubernetes.io/region": "topology.kubernetes.io/regions",
		"topology.kubernetes.io/zone":   "topology.kubernetes.io/zones",
	}
)

func getTopologyKeys(ctx context.Context, k8s k8s.Client, nodeName string) (map[string]string, error) {
	nodes := &v1.NodeList{}
	kv := make(map[string]string)
	if err := k8s.List(ctx, nodes); err != nil {
		return kv, err
	}
	allValues := make(map[string]set.StringSet)

	for _, node := range nodes.Items {
		for topologyKey, topologyValue := range node.Labels {
			plural, exist := topologyKeys[topologyKey]
			if !exist {
				continue
			}
			if node.Name == nodeName {
				kv[topologyKey] = topologyValue
			}
			if allValues[plural] == nil {
				allValues[plural] = set.Make(topologyValue)
			} else {
				allValues[plural].Add(topologyValue)
			}
		}
	}

	for k, v := range allValues {
		s := v.AsSlice()
		sort.Strings(s)
		kv[k] = strings.Join(s, ",")
	}

	return kv, nil
}

func RegisterPodBindingWebhook(mgr ctrl.Manager, validateStorageClass bool) {
	wh := &mutatingWebhook{
		client:               mgr.GetClient(),
		validateStorageClass: validateStorageClass,
	}
	log.Info("Registering pods/binding webhook", "path", webhookPath)
	mgr.GetWebhookServer().Register(webhookPath, &w.Admission{Handler: wh})
}

// InjectDecoder injects the decoder automatically.
func (wh *mutatingWebhook) InjectDecoder(d *admission.Decoder) error {
	wh.decoder = d
	return nil
}

var (
	_ admission.DecoderInjector = &mutatingWebhook{}
	_ admission.Handler         = &mutatingWebhook{}
)
