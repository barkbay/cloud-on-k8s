// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package v1

import (
	"reflect"
	"strings"
	"testing"

	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestResourcePolicies_Validate(t *testing.T) {
	tests := []struct {
		name            string
		autoscalingSpec string
		nodeSets        map[string][]string
		wantError       bool
		expectedError   string
	}{
		{
			name:      "Happy path",
			wantError: false,
			nodeSets:  map[string][]string{"nodeset-data-1": {"data"}, "nodeset-data-2": {"data"}, "nodeset-ml": {"ml"}},
			autoscalingSpec: `
{
	 "policies" : [{
		  "name": "data_policy",
		  "roles": [ "data" ],
		  "resources" : {
			"nodeCount" : { "min" : 1 , "max" : 2 },
			"cpu" : { "min" : 1 , "max" : 1 },
			"memory" : { "min" : "2Gi" , "max" : "2Gi" },
			"storage" : { "min" : "5Gi" , "max" : "10Gi" }
		  }
		},
		{
		  "name": "ml_policy",
		  "roles": [ "ml" ],
		  "resources" : {
			"nodeCount" : { "min" : 1 , "max" : 2 },
			"cpu" : { "min" : 1 , "max" : 1 },
			"memory" : { "min" : "2Gi" , "max" : "2Gi" },
			"storage" : { "min" : "5Gi" , "max" : "10Gi" }
		  }
		}]
}
`,
		},
		{
			name:          "Autoscaling policy with no NodeSet",
			wantError:     true,
			expectedError: "Invalid value: []string{\"ml\"}: roles must be set in at least one nodeSet",
			nodeSets:      map[string][]string{"nodeset-data-1": {"data"}, "nodeset-data-2": {"data"}},
			autoscalingSpec: `
{
	 "policies" : [{
		  "name": "data_policy",
		  "roles": [ "data" ],
		  "resources" : {
			"nodeCount" : { "min" : 1 , "max" : 2 },
			"cpu" : { "min" : 1 , "max" : 1 },
			"memory" : { "min" : "2Gi" , "max" : "2Gi" },
			"storage" : { "min" : "5Gi" , "max" : "10Gi" }
		  }
		},
		{
		  "name": "ml_policy",
		  "roles": [ "ml" ],
		  "resources" : {
			"nodeCount" : { "min" : 1 , "max" : 2 },
			"cpu" : { "min" : 1 , "max" : 1 },
			"memory" : { "min" : "2Gi" , "max" : "2Gi" },
			"storage" : { "min" : "5Gi" , "max" : "10Gi" }
		  }
		}]
}
`,
		},
		{
			name:          "nodeSet with no roles",
			wantError:     true,
			expectedError: "cannot parse nodeSet configuration: node.roles must be set",
			nodeSets:      map[string][]string{"nodeset-data-1": nil, "nodeset-data-2": {"data"}},
			autoscalingSpec: `
{
	 "policies" : [{
		  "name": "data_policy",
		  "roles": [ "data" ],
		  "resources" : {
			"nodeCount" : { "min" : 1 , "max" : 2 },
			"cpu" : { "min" : 1 , "max" : 1 },
			"memory" : { "min" : "2Gi" , "max" : "2Gi" },
			"storage" : { "min" : "5Gi" , "max" : "10Gi" }
		  }
		},
		{
		  "name": "ml_policy",
		  "roles": [ "ml" ],
		  "resources" : {
			"nodeCount" : { "min" : 1 , "max" : 2 },
			"cpu" : { "min" : 1 , "max" : 1 },
			"memory" : { "min" : "2Gi" , "max" : "2Gi" },
			"storage" : { "min" : "5Gi" , "max" : "10Gi" }
		  }
		}]
}
`,
		},
		{
			name:          "Min memory is 2G",
			wantError:     true,
			expectedError: "min quantity must be greater than 2G",
			autoscalingSpec: `
{
	 "policies" : [{
		  "name": "data_policy",
		  "roles": [ "data" ],
		  "resources" : {
			"nodeCount" : { "min" : 1 , "max" : 2 },
			"cpu" : { "min" : 1 , "max" : 1 },
			"memory" : { "min" : "1Gi" , "max" : "2Gi" },
			"storage" : { "min" : "5Gi" , "max" : "10Gi" }
		  }
		},
		{
		  "name": "ml_policy",
		  "roles": [ "ml" ],
		  "resources" : {
			"nodeCount" : { "min" : 1 , "max" : 2 },
			"cpu" : { "min" : 1 , "max" : 1 },
			"memory" : { "min" : "2Gi" , "max" : "2Gi" },
			"storage" : { "min" : "5Gi" , "max" : "10Gi" }
		  }
		}]
}
`,
		},
		{
			name:          "Policy name is duplicated",
			wantError:     true,
			expectedError: "[1].name: Invalid value: \"my_policy\": policy is duplicated",
			autoscalingSpec: `
{
	 "policies" : [{
		  "name": "my_policy",
		  "roles": [ "data" ],
		  "resources" : {
			"nodeCount" : { "min" : 1 , "max" : 2 },
			"cpu" : { "min" : 1 , "max" : 1 },
			"memory" : { "min" : "2Gi" , "max" : "2Gi" },
			"storage" : { "min" : "5Gi" , "max" : "10Gi" }
		  }
		},
		{
		  "name": "my_policy",
		  "roles": [ "ml" ],
		  "resources" : {
			"nodeCount" : { "min" : 1 , "max" : 2 },
			"cpu" : { "min" : 1 , "max" : 1 },
			"memory" : { "min" : "2Gi" , "max" : "2Gi" },
			"storage" : { "min" : "5Gi" , "max" : "10Gi" }
		  }
		}]
}
`,
		},
		{
			name:          "Duplicated roles sets",
			wantError:     true,
			expectedError: "[1].name: Invalid value: \"data, ml\": roles set is duplicated",
			autoscalingSpec: `
{
	 "policies" : [{
		  "name": "my_policy",
		  "roles": [ "data, ml" ],
		  "resources" : {
			  "nodeCount" : { "min" : 1 , "max" : 2 },
			  "cpu" : { "min" : 1 , "max" : 1 },
			  "memory" : { "min" : "2Gi" , "max" : "2Gi" },
			  "storage" : { "min" : "5Gi" , "max" : "10Gi" }
          }
		},
		{
		  "name": "my_policy2",
		  "roles": [ "data, ml" ],
		  "resources" : {
			  "nodeCount" : { "min" : 1 , "max" : 2 },
			  "cpu" : { "min" : 1 , "max" : 1 },
			  "memory" : { "min" : "2Gi" , "max" : "2Gi" },
			  "storage" : { "min" : "5Gi" , "max" : "10Gi" }
		  }
		}]
}
`,
		},
		{
			name:          "No name",
			wantError:     true,
			expectedError: "name: Required value: name is mandatory",
			autoscalingSpec: `
{
	 "policies" : [{
	  "roles": [ "data, ml" ],
      "resources" : {
		  "nodeCount" : { "min" : 1 , "max" : 2 },
		  "cpu" : { "min" : 1 , "max" : 1 },
		  "memory" : { "min" : "2Gi" , "max" : "2Gi" },
		  "storage" : { "min" : "5Gi" , "max" : "10Gi" }
      }
	}]
}
`,
		},
		{
			name:          "No roles",
			wantError:     true,
			expectedError: "roles: Required value: roles is mandatory",
			autoscalingSpec: `
{
	 "policies" : [{
     "name": "my_policy",
      "resources" : {
		  "nodeCount" : { "min" : 1 , "max" : 2 },
		  "cpu" : { "min" : 1 , "max" : 1 },
		  "memory" : { "min" : "2Gi" , "max" : "2Gi" },
		  "storage" : { "min" : "5Gi" , "max" : "10Gi" }
      }
      }]
}
`,
		},
		{
			name:          "No count",
			wantError:     true,
			expectedError: "maxAllowed.count: Invalid value: 0: max node count must be an integer greater than min node count",
			autoscalingSpec: `
{
	 "policies" : [{
  "name": "my_policy",
  "resources" : {
	  "cpu" : { "min" : 1 , "max" : 1 },
	  "memory" : { "min" : "2Gi" , "max" : "2Gi" },
	  "storage" : { "min" : "5Gi" , "max" : "10Gi" }
  }
}]
}
`,
		},
		{
			name:          "Min. count should be greater than 1",
			wantError:     true,
			expectedError: "minAllowed.count: Invalid value: -1: count must be a greater than 1",
			autoscalingSpec: `
{
	"policies": [{
		"name": "my_policy",
		"roles": ["data, ml"],
		"resources": {
			"nodeCount": {
				"min": -1,
				"max": 2
			},
			"cpu": {
				"min": 1,
				"max": 1
			},
			"memory": {
				"min": "2Gi",
				"max": "2Gi"
			},
			"storage": {
				"min": "5Gi",
				"max": "10Gi"
			}
		}
	}]
}
`,
		},
		{
			name:          "Min. count is greater than max",
			wantError:     true,
			expectedError: "maxAllowed.count: Invalid value: 4: max node count must be an integer greater than min node count",
			autoscalingSpec: `
{
	"policies": [{
		"name": "my_policy",
		"roles": ["data, ml"],
		"resources": {
			"nodeCount": {
				"min": 5,
				"max": 4
			},
			"cpu": {
				"min": 1,
				"max": 1
			},
			"memory": {
				"min": "2Gi",
				"max": "2Gi"
			},
			"storage": {
				"min": "5Gi",
				"max": "10Gi"
			}
		}
	}]
}
`,
		},
		{
			name:          "Min. CPU is greater than max",
			wantError:     true,
			expectedError: "cpu: Invalid value: \"50m\": max quantity must be greater or equal than min quantity",
			autoscalingSpec: `
{
	"policies": [{
		"name": "my_policy",
		"roles": ["data, ml"],
		"resources": {
			"nodeCount": {
				"min": -1,
				"max": 2
			},
			"cpu": {
				"min": "100m",
				"max": "50m"
			},
			"memory": {
				"min": "2Gi",
				"max": "2Gi"
			},
			"storage": {
				"min": "5Gi",
				"max": "10Gi"
			}
		}
	}]
}
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			es := Elasticsearch{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						ElasticsearchAutoscalingSpecAnnotationName: tt.autoscalingSpec,
					},
				},
				Spec: ElasticsearchSpec{
					Version: "7.11.0",
				},
			}
			for nodeSetName, roles := range tt.nodeSets {
				cfg := commonv1.NewConfig(map[string]interface{}{})
				if roles != nil {
					cfg = commonv1.NewConfig(map[string]interface{}{"node.roles": roles})
				}
				nodeSet := NodeSet{
					Name:   nodeSetName,
					Config: &cfg,
				}
				es.Spec.NodeSets = append(es.Spec.NodeSets, nodeSet)
			}
			rp, err := es.GetAutoscalingSpecification()
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

func TestAutoscalingSpec_findByRoles(t *testing.T) {
	type fields struct {
		AutoscalingPolicySpecs AutoscalingPolicySpecs
	}
	type args struct {
		roles []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *AutoscalingPolicySpec
	}{
		{
			name: "Managed by an autoscaling policy",
			fields: fields{
				AutoscalingPolicySpecs: AutoscalingPolicySpecs{AutoscalingPolicySpec{
					NamedAutoscalingPolicy: NamedAutoscalingPolicy{
						Name: "ml_only",
						AutoscalingPolicy: AutoscalingPolicy{
							Roles: []string{"ml"},
						},
					},
				}},
			},
			args: args{roles: []string{"ml"}},
			want: &AutoscalingPolicySpec{
				NamedAutoscalingPolicy: NamedAutoscalingPolicy{
					Name: "ml_only",
					AutoscalingPolicy: AutoscalingPolicy{
						Roles: []string{"ml"},
					},
				},
			},
		},
		{
			name: "Not managed by an autoscaling policy",
			fields: fields{
				AutoscalingPolicySpecs: AutoscalingPolicySpecs{AutoscalingPolicySpec{
					NamedAutoscalingPolicy: NamedAutoscalingPolicy{
						Name: "ml_only",
						AutoscalingPolicy: AutoscalingPolicy{
							Roles: []string{"ml"},
						},
					},
				}},
			},
			args: args{roles: []string{"master"}},
			want: nil,
		},
		{
			name: "Not managed by an autoscaling policy",
			fields: fields{
				AutoscalingPolicySpecs: AutoscalingPolicySpecs{AutoscalingPolicySpec{
					NamedAutoscalingPolicy: NamedAutoscalingPolicy{
						Name: "ml_only",
						AutoscalingPolicy: AutoscalingPolicy{
							Roles: []string{"ml"},
						},
					},
				}},
			},
			args: args{roles: []string{"ml", "data"}},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			as := AutoscalingSpec{
				AutoscalingPolicySpecs: tt.fields.AutoscalingPolicySpecs,
			}
			got := as.findByRoles(tt.args.roles)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AutoscalingSpec.findByRoles() = %v, want %v", got, tt.want)
			}
		})
	}
}
