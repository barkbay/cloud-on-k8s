package autoscaling

import (
	"context"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
)

func updatePolicies(
	es esv1.Elasticsearch,
	client k8s.Client,
	esclient client.AutoScalingClient,
) (NamedTiers, error) {
	namedTiers, err := getNamedTiers(client, es)
	if err != nil {
		return nil, err
	}

	for namedTier := range namedTiers {
		if err := updatePolicy(esclient, namedTier); err != nil {
			return nil, err
		}
	}

	return namedTiers, nil
}

func updatePolicy(
	esclient client.AutoScalingClient,
	namedTier string,
) error {
	return esclient.UpsertAutoscalingPolicy(context.Background(), namedTier, client.AutoscalingPolicy{})
}
