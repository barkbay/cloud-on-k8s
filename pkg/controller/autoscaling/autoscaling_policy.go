package autoscaling

import (
	"context"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/tracing"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/go-logr/logr"
	"go.elastic.co/apm"
)

// updatePolicies updates the autoscaling policies in the Elasticsearch cluster.
func updatePolicies(
	log logr.Logger,
	ctx context.Context,
	autoscalingSpec esv1.AutoscalingSpec,
	esclient client.AutoScalingClient,
) error {
	span, _ := apm.StartSpan(ctx, "update_autoscaling_policies", tracing.SpanTypeApp)
	defer span.End()
	// Cleanup existing autoscaling policies
	if err := esclient.DeleteAutoscalingAutoscalingPolicies(context.Background()); err != nil {
		log.Error(err, "Error while deleting policies")
		return err
	}
	// Create the expected autoscaling policies
	for _, rp := range autoscalingSpec.AutoscalingPolicySpecs {
		if err := esclient.UpsertAutoscalingPolicy(ctx, rp.Name, rp.AutoscalingPolicy); err != nil {
			log.Error(err, "Error while updating an autoscaling policy", "policy", rp.Name)
			return err
		}
	}
	return nil
}
