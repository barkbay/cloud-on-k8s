package driver

import (
	"fmt"

	"k8s.io/apimachinery/pkg/types"

	"github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1alpha1"
	esclient "github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/sset"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	v1 "k8s.io/api/core/v1"
)

type DeletionController interface {
	// Delete goes thought the candidates and actually deletes the ones
	Delete(pod []*v1.Pod) (victims []*v1.Pod, err error)
}

type DefaultDeletionController struct {
	client       k8s.Client
	esClient     esclient.Client
	es           *v1alpha1.Elasticsearch
	state        *ESState
	healthyPods  map[types.NamespacedName]*v1.Pod
	strategy     DeletionStrategy
	expectations *RestartExpectations
}

// NewDeletionController creates a new deletion controller.
func NewDeletionController(
	client k8s.Client,
	esClient esclient.Client,
	es *v1alpha1.Elasticsearch,
	state *ESState,
	healthyPods map[types.NamespacedName]*v1.Pod,
	expectations *RestartExpectations,
) *DefaultDeletionController {
	return &DefaultDeletionController{
		client:       client,
		esClient:     esClient,
		es:           es,
		state:        state,
		healthyPods:  healthyPods,
		expectations: expectations,
		strategy:     GetDeletionStrategy(state),
	}
}

func (d *DefaultDeletionController) Delete(potentialVictims []*v1.Pod) (victims []*v1.Pod, err error) {
	if len(potentialVictims) == 0 {
		return nil, nil
	}
	es := k8s.ExtractNamespacedName(d.es)
	if len(d.expectations.GetExpectedRestarts(es)) > 0 {
		// TODO: Try to adopt / re-enqueue a deletion (Step 4.1)
		return
	}

	// Start Step 4.2: expectations are empty, try to find some victims

	// Check if we are not over disruption budget
	// Upscale is done, we should have the required number of Pods
	statefulSets, err := sset.RetrieveActualStatefulSets(d.client, es)
	if err != nil {
		return nil, err
	}
	expectedPods := statefulSets.PodNames()
	missingPods := len(expectedPods) - len(d.healthyPods)
	maxUnavailable := 1
	if d.es.Spec.UpdateStrategy.ChangeBudget != nil {
		maxUnavailable = d.es.Spec.UpdateStrategy.ChangeBudget.MaxUnavailable
	}
	allowedDeletions := maxUnavailable - missingPods
	if allowedDeletions <= 0 {
		return nil, fmt.Errorf("max unavailable Pods reached")
	}

	// Step 2. Sort the Pods to get the ones with the higher priority
	err = d.strategy.SortFunction()(potentialVictims, d.state)
	if err != nil {
		return nil, err
	}

	// Step 3: Pick the first one and apply predicates
	predicates := d.strategy.Predicates()
	for _, candidate := range potentialVictims {
		if ok, err := d.runPredicates(candidate, predicates); err != nil {
			return nil, err
		} else if ok {
			candidate := candidate
			// Remove from healthy nodes if it was there
			delete(d.healthyPods, k8s.ExtractNamespacedName(candidate))
			// Append to the victims list
			victims = append(victims, candidate)
			allowedDeletions = allowedDeletions - 1
			if allowedDeletions == 0 {
				break
			}
		}
	}

	if err := prepareClusterForNodeRestart(d.esClient, *d.state); err != nil {
		return nil, err
	}
	for _, victim := range victims {
		d.expectations.ExpectRestart(victim)
		err := d.client.Delete(victim)
		if err != nil {
			d.expectations.RemoveExpectation(victim)
			return victims, err
		}
	}

	return victims, nil
}

func (d *DefaultDeletionController) runPredicates(
	candidate *v1.Pod,
	predicates map[string]Predicate,
) (bool, error) {
	expectedDeletions := d.expectations.GetExpectedRestarts(k8s.ExtractNamespacedName(d.es))
	for _, predicate := range predicates {
		canDelete, err := predicate(candidate, expectedDeletions)
		if err != nil {
			return false, err
		}
		if !canDelete {
			//skip this Pod, it can't be deleted for the moment
			continue
		}
	}
	// All predicates passed !
	return true, nil
}

func prepareClusterForNodeRestart(esClient esclient.Client, esState ESState) error {
	// Disable shard allocations to avoid shards moving around while the node is temporarily down
	shardsAllocationEnabled, err := esState.ShardAllocationsEnabled()
	if err != nil {
		return err
	}
	if shardsAllocationEnabled {
		if err := disableShardsAllocation(esClient); err != nil {
			return err
		}
	}

	// Request a sync flush to optimize indices recovery when the node restarts.
	if err := doSyncFlush(esClient); err != nil {
		return err
	}

	// TODO: halt ML jobs on that node
	return nil
}
