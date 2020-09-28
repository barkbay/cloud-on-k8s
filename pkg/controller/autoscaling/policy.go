package autoscaling

import (
	"context"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
)

func updatePolicies(
	parentCtx context.Context,
	es esv1.Elasticsearch,
	client k8s.Client,
	esclient client.AutoScalingClient,
) (NamedTiers, error) {
	namedTiers, err := GetNamedTiers(client, es)
	if err != nil {
		return nil, err
	}

	for namedTier := range namedTiers {
		if err := updatePolicy(parentCtx, esclient, namedTier); err != nil {
			return nil, err
		}
	}

	return namedTiers, nil
}

func updatePolicy(
	parentCtx context.Context,
	esclient client.AutoScalingClient,
	namedTier string,
) error {
	ctx, cancel := context.WithTimeout(parentCtx, client.DefaultReqTimeout)
	defer cancel()
	return esclient.UpsertAutoscalingPolicy(ctx, namedTier, client.AutoscalingPolicy{})
}
