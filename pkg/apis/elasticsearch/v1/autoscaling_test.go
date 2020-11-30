// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package v1

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestResourcePolicies_Validate(t *testing.T) {
	tests := []struct {
		name             string
		resourcePolicies string
		wantError        bool
		expectedError    string
	}{
		{
			name:      "Happy path",
			wantError: false,
			resourcePolicies: `
[{
  "name": "data_policy",
  "roles": [ "data" ],
  "minAllowed" : { "count" : 1, "cpu" : "1", "memory" : "2Gi", "storage" : "5Gi" },
  "maxAllowed" : { "count" : 2, "cpu" : "1", "memory" : "2Gi", "storage" : "10Gi" }
},
{
  "name": "ml_policy",
  "roles": [ "ml" ],
  "minAllowed" : { "count" : 1, "cpu" : "1", "memory" : "2Gi", "storage" : "5Gi" },
  "maxAllowed" : { "count" : 2, "cpu" : "1", "memory" : "2Gi", "storage" : "10Gi" }
}]
`,
		},
		{
			name:          "Policy name is duplicated",
			wantError:     true,
			expectedError: "[1].name: Invalid value: \"my_policy\": policy is duplicated",
			resourcePolicies: `
[{
  "name": "my_policy",
  "roles": [ "data" ],
  "minAllowed" : { "count" : 1, "cpu" : "1", "memory" : "2Gi", "storage" : "5Gi" },
  "maxAllowed" : { "count" : 2, "cpu" : "1", "memory" : "2Gi", "storage" : "10Gi" }
},
{
  "name": "my_policy",
  "roles": [ "ml" ],
  "minAllowed" : { "count" : 1, "cpu" : "1", "memory" : "2Gi", "storage" : "5Gi" },
  "maxAllowed" : { "count" : 2, "cpu" : "1", "memory" : "2Gi", "storage" : "10Gi" }
}]
`,
		},
		{
			name:          "Duplicated roles sets",
			wantError:     true,
			expectedError: "[1].name: Invalid value: \"data, ml\": roles set is duplicated",
			resourcePolicies: `
[{
  "name": "my_policy",
  "roles": [ "data, ml" ],
  "minAllowed" : { "count" : 1, "cpu" : "1", "memory" : "2Gi", "storage" : "5Gi" },
  "maxAllowed" : { "count" : 2, "cpu" : "1", "memory" : "2Gi", "storage" : "10Gi" }
},
{
  "name": "my_policy2",
  "roles": [ "data, ml" ],
  "minAllowed" : { "count" : 1, "cpu" : "1", "memory" : "2Gi", "storage" : "5Gi" },
  "maxAllowed" : { "count" : 2, "cpu" : "1", "memory" : "2Gi", "storage" : "10Gi" }
}]
`,
		},
		{
			name:          "No name",
			wantError:     true,
			expectedError: "name: Required value: name is mandatory",
			resourcePolicies: `
[{
}]
`,
		},
		{
			name:          "No roles",
			wantError:     true,
			expectedError: "roles: Required value: roles is mandatory",
			resourcePolicies: `
[{
  "name": "my_policy"
}]
`,
		},
		{
			name:          "No count",
			wantError:     true,
			expectedError: "maxAllowed.count: Invalid value: 0: count must be an integer greater than 0",
			resourcePolicies: `
[{
  "name": "my_policy",
  "roles": [ "data, ml" ],
  "minAllowed" : {
    "count" : 0,
    "cpu" : "1",
    "memory" : "2Gi",
    "storage" : "5Gi"
  },
  "maxAllowed" : {
    "cpu" : "1",
    "memory" : "2Gi",
    "storage" : "10Gi"
  }
}]
`,
		},
		{
			name:          "Min. count should not be negative value",
			wantError:     true,
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
			wantError:     true,
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
			wantError:     true,
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
			es := Elasticsearch{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						ElasticsearchAutoscalingSpecAnnotationName: tt.resourcePolicies,
					},
				},
			}
			rp, err := es.GetAutoscalingSpecifications()
			assert.NoError(t, err)
			got := rp.Validate()
			assert.Equal(t, tt.wantError, got != nil)
			found := false
			for _, gotErr := range rp.Validate() {
				if strings.Contains(gotErr.Error(), tt.expectedError) {
					found = true
					break
				}
			}

			if tt.wantError && !found {
				t.Errorf("AutoscalingSpecs.Validate() = %v, want string \"%v\"", got, tt.expectedError)
			}
		})
	}
}
