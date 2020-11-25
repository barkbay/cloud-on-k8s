package autoscaling

import (
	"context"

	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
)

// updatePolicies
func updatePolicies(
	resourcePolicies commonv1.ResourcePolicies,
	esclient client.AutoScalingClient,
) error {
	// Cleanup existing autoscaling policies
	if err := esclient.DeleteAutoscalingAutoscalingPolicies(context.Background()); err != nil {
		return err
	}
	// Create the expected autoscaling policies
	for _, rp := range resourcePolicies {
		if err := esclient.UpsertAutoscalingPolicy(context.Background(), *rp.Name, rp.AutoscalingPolicy); err != nil {
			return err
		}
	}
	return nil
}
