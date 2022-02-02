package reconcile

import (
	"fmt"
	"strings"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	corev1 "k8s.io/api/core/v1"
)

type ExpectedChanges struct {
	PodsToUpgrade, PodsToUpscale, PodsToDownscale []string
}

// ReportExpectedChanges records expected changes on the cluster: upscale, downscale and upgrades.
func (s *State) ReportExpectedChanges(expectedChanges ExpectedChanges) *State {

	// Record scheduled node changes.
	s.RecordNodesToBeUpscaled(expectedChanges.PodsToUpscale)
	s.RecordNodesToBeRemoved(expectedChanges.PodsToDownscale)
	s.RecordNodesToBeUpgraded(expectedChanges.PodsToUpgrade)

	if expectedChanges.IsEmpty() {
		s.ReportCondition(esv1.NodesSpecificationReconciled, corev1.ConditionTrue, "")
		// ReconciliationComplete is initially set to True until another condition is reported.
		s.ReportCondition(esv1.ReconciliationComplete, corev1.ConditionTrue, "")
	} else {
		s.ReportCondition(esv1.NodesSpecificationReconciled, corev1.ConditionFalse, expectedChanges.String())
		s.ReportCondition(esv1.ReconciliationComplete, corev1.ConditionFalse, "Nodes specification are not reconciled yet")
	}

	// When not reconciled, set the phase to ApplyingChanges only if it was Ready to avoid overriding
	// another "not Ready" phase like MigratingData.
	if expectedChanges.IsEmpty() {
		s.UpdateWithPhase(esv1.ElasticsearchReadyPhase)
	} else if s.IsElasticsearchReady() {
		s.UpdateWithPhase(esv1.ElasticsearchApplyingChangesPhase)
	}

	return s
}

func (e *ExpectedChanges) IsEmpty() bool {
	if e == nil {
		return true
	}
	return len(e.PodsToDownscale) == 0 && len(e.PodsToUpscale) == 0 && len(e.PodsToUpgrade) == 0
}

func (e *ExpectedChanges) String() string {
	items := make([]string, 0, 3)
	if len(e.PodsToUpscale) > 0 {
		items = append(items, fmt.Sprintf("%d node%s to add", len(e.PodsToUpscale), plural(e.PodsToUpscale)))
	}
	if len(e.PodsToDownscale) > 0 {
		items = append(items, fmt.Sprintf("%d node%s to remove", len(e.PodsToDownscale), plural(e.PodsToDownscale)))
	}
	if len(e.PodsToUpgrade) > 0 {
		items = append(items, fmt.Sprintf("%d node%s to update", len(e.PodsToUpgrade), plural(e.PodsToUpgrade)))
	}
	return strings.Join(items, ", ")
}

func plural(items []string) string {
	if len(items) > 0 {
		return "s"
	}
	return ""
}
