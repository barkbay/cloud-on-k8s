package nodesets

import esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"

type NodeSetsResources []NodeSetResources

type NodeSetResources struct {
	Name string `json:"name,omitempty"`
	esv1.ResourcesSpecification
}

// TODO: Create a context struct to embed things like nodeSets, log, autoscalingSpec or statusBuilder

func (nsr NodeSetsResources) ByNodeSet() map[string]NodeSetResources {
	byNodeSet := make(map[string]NodeSetResources, len(nsr))
	for _, nodeSetsResource := range nsr {
		byNodeSet[nodeSetsResource.Name] = nodeSetsResource
	}
	return byNodeSet
}
