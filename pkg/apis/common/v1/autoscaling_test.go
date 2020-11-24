// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package v1

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResourcePolicies_Validate(t *testing.T) {
	tests := []struct {
		name             string
		resourcePolicies string
		expectedError    string
	}{
		{
			name:          "No name",
			expectedError: "name: Required value: name is mandatory",
			resourcePolicies: `
[{
}]
`,
		},
		{
			name:          "No roles",
			expectedError: "roles: Required value: roles is mandatory",
			resourcePolicies: `
[{
  "name": "my_policy"
}]
`,
		},
		{
			name:          "No count",
			expectedError: "minAllowed.count: Required value: count field is mandatory",
			resourcePolicies: `
[{
  "name": "my_policy",
  "roles": [ "data, ml" ]
}]
`,
		},
		{
			name:          "Min. count should not be negative value",
			expectedError: "minAllowed.count: Invalid value: -1: count must be a positive integer",
			resourcePolicies: `
[{
  "name": "my_policy",
  "roles": [ "data, ml" ],
  "minAllowed" : {
    "count" : -1,
    "cpu" : "1",
    "memory" : "2Gi",
    "storage" : "5Gi"
  },
  "maxAllowed" : {
    "count" : 4,
    "cpu" : "1",
    "memory" : "2Gi",
    "storage" : "10Gi"
  }
}]
`,
		},
		{
			name:          "Min. count is greater than max",
			expectedError: "maxAllowed.count: Invalid value: 4: maxAllowed must be greater or equal than minAllowed",
			resourcePolicies: `
[{
  "name": "my_policy",
  "roles": [ "data, ml" ],
  "minAllowed" : {
    "count" : 5,
    "cpu" : "1",
    "memory" : "2Gi",
    "storage" : "5Gi"
  },
  "maxAllowed" : {
    "count" : 4,
    "cpu" : "1",
    "memory" : "2Gi",
    "storage" : "10Gi"
  }
}]
`,
		},
		{
			name:          "Min. CPU is greater than max",
			expectedError: "cpu: Invalid value: \"50m\": maxAllowed must be greater or equal than minAllowed",
			resourcePolicies: `
[{
  "name": "my_policy",
  "roles": [ "data, ml" ],
  "minAllowed" : {
    "count" : 1,
    "cpu" : "100m",
    "memory" : "2Gi",
    "storage" : "5Gi"
  },
  "maxAllowed" : {
    "count" : 4,
    "cpu" : "50m",
    "memory" : "2Gi",
    "storage" : "10Gi"
  }
}]
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rp, err := ResourcePoliciesFrom(tt.resourcePolicies)
			assert.NoError(t, err)
			got := rp.Validate()
			found := false
			for _, gotErr := range rp.Validate() {
				if strings.Contains(gotErr.Error(), tt.expectedError) {
					found = true
					break
				}
			}

			if !found {
				t.Errorf("ResourcePolicies.Validate() = %v, want string \"%v\"", got, tt.expectedError)
			}
		})
	}
}
