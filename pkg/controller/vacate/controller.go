// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package vacate

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/elastic/cloud-on-k8s/pkg/controller/vacate/webhook"

	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/volume"

	"k8s.io/apimachinery/pkg/types"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/operator"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/tracing"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/watches"
	esv1label "github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	ulog "github.com/elastic/cloud-on-k8s/pkg/utils/log"
	"github.com/elastic/cloud-on-k8s/pkg/utils/rbac"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	name = "vacate-controller"
	// LabelName used to specify that a PVC is expected to be vacated.
	LabelName = "elasticsearch.k8s.elastic.co/vacate"

	AnnotationName = "elasticsearch.k8s.elastic.co/vacated-pvc"

	// VacatedPVAnnotationName is the label used to mention that a PV is vacated
	VacatedPVAnnotationName = "elasticsearch.k8s.elastic.co/vacated"
	// VacatedFromPVAnnotationName is the name of the relocated PVC
	VacatedFromPVAnnotationName = "elasticsearch.k8s.elastic.co/vacated-from"
)

var (
	log            = ulog.Log.WithName("vacate")
	defaultRequeue = reconcile.Result{Requeue: true, RequeueAfter: 5 * time.Second}
)

// AddVacateES creates a new vacate Controller and adds it to the manager with default RBAC.
func AddVacateES(mgr manager.Manager, accessReviewer rbac.AccessReviewer, params operator.Parameters) error {
	r := NewReconciler(mgr, accessReviewer, params)
	c, err := common.NewController(mgr, name, r, params)
	if err != nil {
		return err
	}
	return AddWatches(c)
}

// NewReconciler returns a new reconcile.Reconciler
func NewReconciler(mgr manager.Manager, accessReviewer rbac.AccessReviewer, params operator.Parameters) *ReconcileVacate {
	c := mgr.GetClient()
	return &ReconcileVacate{
		Client:         c,
		accessReviewer: accessReviewer,
		watches:        watches.NewDynamicWatches(),
		recorder:       mgr.GetEventRecorderFor(name),
		Parameters:     params,
	}
}

var _ reconcile.Reconciler = &ReconcileVacate{}

// ReconcileVacate adds or remove temporary nodeSets in grow-and-shrink workflows.
type ReconcileVacate struct {
	k8s.Client
	operator.Parameters
	accessReviewer rbac.AccessReviewer
	recorder       record.EventRecorder
	watches        watches.DynamicWatches

	// iteration is the number of times this controller has run its Reconcile method
	iteration uint64
}

// Reconcile reads that state of the cluster for the expected remote clusters in this Kubernetes cluster.
// It copies the remote CA Secrets so they can be trusted by every peer Elasticsearch clusters.
func (r *ReconcileVacate) Reconcile(ctx context.Context, request reconcile.Request) (reconcile.Result, error) {
	defer common.LogReconciliationRun(log, request, "es_name", &r.iteration)()
	tx, ctx := tracing.NewTransaction(ctx, r.Tracer, request.NamespacedName, "vacate")
	defer tracing.EndTransaction(tx)

	// Fetch the local Elasticsearch spec
	es := esv1.Elasticsearch{}
	err := r.Get(context.Background(), request.NamespacedName, &es)
	if err != nil && !errors.IsNotFound(err) {
		return reconcile.Result{}, err
	}

	if common.IsUnmanaged(&es) {
		log.Info("Object is currently not managed by this controller. Skipping reconciliation", "namespace", es.Namespace, "es_name", es.Name)
		return reconcile.Result{}, nil
	}

	return doReconcile(ctx, r, &es)
}

func getNodeSetAndOrdinal(es string, pvc v1.PersistentVolumeClaim) (string, int64, error) {
	// Ordinal to be vacated
	ordinalPos := strings.LastIndex(pvc.Name, "-")
	ordinalAsString := pvc.Name[ordinalPos+1:]
	ordinalAsInt, err := strconv.ParseInt(ordinalAsString, 10, 32)
	if err != nil {
		return "", 0, err
	}
	// Get sset name
	sset, exists := pvc.Labels[esv1label.StatefulSetNameLabelName]
	if !exists {
		return "", 0, fmt.Errorf("cannot vacate because of missing annotation %s on PVC %s", esv1label.StatefulSetNameLabelName, pvc.Name)
	}
	// Get original nodeSet name, sored in the suffix (a bit hacky)
	prefix := fmt.Sprintf("%s-es-", es)
	nodeSetName := strings.TrimPrefix(sset, prefix)
	return nodeSetName, ordinalAsInt, nil
}

func doReconcile(
	ctx context.Context,
	r *ReconcileVacate,
	es *esv1.Elasticsearch,
) (reconcile.Result, error) {
	// Check if a PVC is annotated as to be vacated.
	matchingLabels := client.MatchingLabels(map[string]string{
		esv1label.ClusterNameLabelName: es.Name,
		LabelName:                      "true",
	})
	pvcList := &v1.PersistentVolumeClaimList{}
	err := r.Client.List(ctx, pvcList, client.InNamespace(es.Namespace), matchingLabels)
	if err != nil {
		return reconcile.Result{}, err
	}

	// Patch underlying PV
	for _, pvc := range pvcList.Items {
		// Get PV name
		err := patchPV(r.Client, pvc)
		if err != nil {
			return reconcile.Result{}, err
		}
	}

	// nodeSet -> pvcName
	actualTemporaryNodeSets := make(map[string]string)
	// Check if there is nodeSet created for a grow-and-shrink operation
	for i, nodeSet := range es.Spec.NodeSets {
		// Read annotations
		for annotation, pvcName := range nodeSet.PodTemplate.Annotations {
			if annotation == AnnotationName {
				actualTemporaryNodeSets[es.Spec.NodeSets[i].Name] = pvcName
			}
		}
	}

	// Calculate expected actualTemporaryNodeSets, tmpName -> sourceNodeSet
	expectedNodeSets := make(map[string]string)
	for _, pvc := range pvcList.Items {
		// Check if nodeSet exist
		if _, exists := actualTemporaryNodeSets[pvc.Name]; exists {
			continue
		}
		nodeSetName, ordinalAsInt, err := getNodeSetAndOrdinal(es.Name, pvc)
		if err != nil {
			return reconcile.Result{}, err
		}

		// Build expected nodeSet name
		tmpNodeSetname := fmt.Sprintf("vacate-%s-%d", nodeSetName, ordinalAsInt)
		expectedNodeSets[tmpNodeSetname] = nodeSetName
	}

	esUpdated := false
	// Add missing nodeSet
	for expectedNodeSet, sourceNodeSet := range expectedNodeSets {
		if _, alreadyExists := actualTemporaryNodeSets[expectedNodeSet]; alreadyExists {
			continue
		}
		// Create a new nodeSet
		tmpNodeSet, err := copyNodeSet(es, sourceNodeSet, expectedNodeSet)
		if err != nil {
			return reconcile.Result{}, err
		}
		es.Spec.NodeSets = append(es.Spec.NodeSets, tmpNodeSet)
		esUpdated = true
	}

	// Remove unnecessary temporary NodeSets
	for actualNodeSet := range actualTemporaryNodeSets {
		_, isExpected := expectedNodeSets[actualNodeSet]
		if !isExpected {
			esUpdated = true
			removeNodeSet(es, actualNodeSet)
		}
	}

	if esUpdated {
		if err := r.Client.Update(ctx, es); err != nil {
			return reconcile.Result{}, err
		}
	}

	// Phase 2 : delete PVC and POD
	for _, pvc := range pvcList.Items {
		pvc := pvc
		err := r.Client.Delete(context.TODO(), &pvc)
		if err != nil {
			return reconcile.Result{}, err
		}
		// Infer POD name
		nodeSet, ordinal, err := getNodeSetAndOrdinal(es.Name, pvc)
		if err != nil {
			return reconcile.Result{}, err
		}
		sset := esv1.StatefulSet(es.Name, nodeSet)
		podName := fmt.Sprintf("%s-%d", sset, ordinal)
		err = r.Client.Delete(context.TODO(), &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: pvc.Namespace,
			},
		})
		if err != nil {
			return reconcile.Result{}, err
		}
	}

	// Phase 3 attempt to recreate PVCs
	requeue, err := recreatePVC(r.Client)
	if err != nil {
		return reconcile.Result{}, err
	}

	if requeue {
		return defaultRequeue, nil
	}

	return reconcile.Result{}, nil
}

func recreatePVC(k k8s.Client) (requeue bool, err error) {
	// List PV waiting to be adopted
	pvs := &v1.PersistentVolumeList{}
	if err := k.List(context.TODO(), pvs); err != nil {
		return false, err
	}

	for _, pv := range pvs.Items {
		if len(pv.Annotations) == 0 {
			continue
		}
		if pvcAsJSON, hasFormerPVC := pv.Annotations[VacatedFromPVAnnotationName]; hasFormerPVC {
			log.Info("Check if new PVCs can be created")

			pvc := &v1.PersistentVolumeClaim{}
			err := json.Unmarshal([]byte(pvcAsJSON), pvc)
			if err != nil {
				log.Error(err, "cannot deserialize PVC")
				return false, err
			}

			pvcStillExists, err := pvcExists(k, pvc.Namespace, pvc.Name, string(pvc.UID))
			if err != nil {
				return true, err
			}
			if pvcStillExists {
				// Skip, but requeue later
				requeue = true
				continue
			}

			// Recreate the 2 PVCs
			// TODO: Check if we can also let the sst controller recreates them, it might be a bit slower though
			clusterName := pvc.Labels[esv1label.ClusterNameLabelName]
			nodeSetName, ordinalAsInt, err := getNodeSetAndOrdinal(clusterName, *pvc)
			if err != nil {
				return true, err
			}
			tmpNodeSetname := fmt.Sprintf("vacate-%s-%d", nodeSetName, ordinalAsInt)
			newPVCName := fmt.Sprintf("elasticsearch-data-elasticsearch-sample-es-%s-0", tmpNodeSetname)

			// 1. Create PVC for stunt double (the one which adopts the former volume)
			adoptPVC := &v1.PersistentVolumeClaim{}
			if err := k.Get(context.TODO(), types.NamespacedName{Name: newPVCName, Namespace: pvc.Namespace}, adoptPVC); errors.IsNotFound(err) {
				log.Info("Create PVC for stunt double")
				adoptPVC = &v1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:        newPVCName,
						Namespace:   pvc.Namespace,
						Annotations: map[string]string{webhook.TrustMeAnnotation: "true"},
					},
					Spec: pvc.Spec,
				}
				err = k.Create(context.TODO(), adoptPVC)
				if err != nil {
					return false, err
				}
			} else if err != nil {
				return true, err
			}

			// 2. Create PVC for new Pod
			log.Info("Create PVC for new volume double")
			newSpec := pvc.Spec.DeepCopy()
			newSpec.VolumeName = ""
			newPVC2 := &v1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:        newPVCName,
					Namespace:   pvc.Namespace,
					Annotations: map[string]string{webhook.TrustMeAnnotation: "true"},
				},
				Spec: *newSpec,
			}
			err = k.Create(context.TODO(), newPVC2)
			if err != nil {
				return false, err
			}
		}
	}
	return
}

func bindPV(k k8s.Client, pvName, pvcName string) error {

}

func pvcExists(k k8s.Client, namespace, name, uid string) (bool, error) {
	key := types.NamespacedName{
		Namespace: namespace,
		Name:      name,
	}
	// Check if former PVC still exists
	pvc := &v1.PersistentVolumeClaim{}
	if err := k.Get(context.TODO(), key, pvc); err != nil {
		if errors.IsNotFound(err) {
			log.Info("PVC does not exist anymore", "pvc", name, "uid", uid)
			return false, nil
		}
		return false, err
	}
	// PVC exists, check uid
	if string(pvc.UID) == uid {
		return true, nil
	}
	// UIDs differ
	log.Info("PVC does not exist anymore", "pvc", name, "uid", uid)
	return false, nil
}

func removeNodeSet(es *esv1.Elasticsearch, set string) {
	for i, nodeSet := range es.Spec.NodeSets {
		if nodeSet.Name == set {
			es.Spec.NodeSets = append(es.Spec.NodeSets[:i], es.Spec.NodeSets[i+1:]...)
			return
		}
	}
}

func patchPV(k k8s.Client, pvc v1.PersistentVolumeClaim) error {
	pv := &v1.PersistentVolume{}
	err := k.Get(context.TODO(), types.NamespacedName{
		Name: pvc.Spec.VolumeName,
	}, pv)
	if err != nil {
		return err
	}
	updated := false
	if pv.Spec.PersistentVolumeReclaimPolicy != v1.PersistentVolumeReclaimRetain {
		pv.Spec.PersistentVolumeReclaimPolicy = v1.PersistentVolumeReclaimRetain
		updated = true
	}
	if pv.Annotations == nil {
		pv.Annotations = make(map[string]string)
	}
	if _, hasFormerPVC := pv.Annotations[VacatedFromPVAnnotationName]; !hasFormerPVC {
		// Serialize PVC
		asJSON, err := json.Marshal(pvc)
		if err != nil {
			return err
		}
		pv.Annotations[VacatedFromPVAnnotationName] = string(asJSON)
		updated = true
	}

	if updated {
		err := k.Update(context.TODO(), pv)
		if err != nil {
			return err
		}
	}
	return nil
}

func copyNodeSet(es *esv1.Elasticsearch, nodeSet, newName string) (esv1.NodeSet, error) {
	for i := range es.Spec.NodeSets {
		if es.Spec.NodeSets[i].Name == nodeSet {
			deepCopy := es.Spec.NodeSets[i].DeepCopy()
			deepCopy.Name = newName
			deepCopy.Count = 1
			if deepCopy.PodTemplate.Annotations == nil {
				deepCopy.PodTemplate.Annotations = make(map[string]string)
			}
			if len(deepCopy.VolumeClaimTemplates) == 0 {
				// Assume user is relying on the default one
				defaultPVC := volume.DefaultDataVolumeClaim.DeepCopy()
				deepCopy.VolumeClaimTemplates = append(deepCopy.VolumeClaimTemplates, *defaultPVC)
			}
			for j := range deepCopy.VolumeClaimTemplates {
				vct := deepCopy.VolumeClaimTemplates[j]
				if vct.Annotations == nil {
					vct.Annotations = make(map[string]string)
				}
				vct.Annotations[webhook.DelayAnnotation] = "true"
				deepCopy.VolumeClaimTemplates[j] = vct
			}
			deepCopy.PodTemplate.Annotations[AnnotationName] = "true"
			return *deepCopy, nil
		}
	}
	return esv1.NodeSet{}, fmt.Errorf("nodeset not found: %s", nodeSet)
}
