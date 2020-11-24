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
	for _, rp := range resourcePolicies {
		if err := esclient.UpsertAutoscalingPolicy(context.Background(), *rp.Name, rp.AutoscalingPolicy); err != nil {
			return err
		}
	}
	return nil
}
