// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package certificates

import "fmt"

// GetSubjectName returns the wildcard name of the nodes we want to allow to connect.
func GetSubjectName(clusterName string, namespace string) []string {
	return []string{fmt.Sprintf("*.node.%s.%s.es.cluster.local", clusterName, namespace)}
}
