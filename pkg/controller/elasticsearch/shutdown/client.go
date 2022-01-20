package shutdown

import (
	"context"

	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/reconcile"

	esclient "github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
)

type Client struct {
	esclient.Client
	*reconcile.StatusReporter
	podToNodeID map[string]string
}

var _ esclient.Client = &Client{}

func NewDownscaleClient(
	client esclient.Client,
	statusReporter *reconcile.StatusReporter,
	podToNodeID map[string]string,
) esclient.Client {
	return &Client{
		Client:         client,
		StatusReporter: statusReporter,
		podToNodeID:    podToNodeID,
	}
}

func (c *Client) GetShutdown(ctx context.Context, nodeID *string) (esclient.ShutdownResponse, error) {
	shutdownResponse, err := c.Client.GetShutdown(ctx, nodeID)
	if err == nil {
		c.RecordShutdownAPIResult(shutdownResponse, c.podToNodeID)
	}
	return shutdownResponse, err
}

// PutShutdown initiates a node shutdown procedure for the given node.
// Introduced in: Elasticsearch 7.14.0

func (c *Client) PutShutdown(ctx context.Context, nodeID string, shutdownType esclient.ShutdownType, reason string) error {
	return c.Client.PutShutdown(ctx, nodeID, shutdownType, reason)
}

// DeleteShutdown attempts to cancel an ongoing node shutdown.
// Introduced in: Elasticsearch 7.14.0
func (c *Client) DeleteShutdown(ctx context.Context, nodeID string) error {
	err := c.Client.DeleteShutdown(ctx, nodeID)
	if err == nil {
		c.RecordDeleteShutdown(nodeID, c.podToNodeID)
	}
	return err
}
