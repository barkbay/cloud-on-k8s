// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

// +build es e2e

package es

import (
	"io/ioutil"
	"testing"

	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/test/e2e/test"
	"github.com/elastic/cloud-on-k8s/test/e2e/test/elasticsearch"
	"github.com/stretchr/testify/require"
)

// TestAutoscaling ensures that the operator is compatible with the Elasticsearch API.
// The purpose of this test is only to assess that there is no regression at the API level, as such it only relies on the
// fixed decider to generate scaling events. other deciders, like storage deciders or ML deciders are not exercised.
func TestAutoscaling(t *testing.T) {
	// only execute this test if we have a test license to work with
	if test.Ctx().TestLicense == "" {
		t.SkipNow()
	}

	name := "test-autoscaling"
	ns1 := test.Ctx().ManagedNamespace(0)
	esBuilder := elasticsearch.NewBuilder(name).
		WithNamespace(ns1).
		// Create a dedicated master node
		WithNodeSet(esv1.NodeSet{
			Name:  "master",
			Count: 1,
			Config: &commonv1.Config{
				Data: map[string]interface{}{
					esv1.NodeRoles: []string{"master"},
				},
			},
		}).
		// Add a data tier
		WithNodeSet(esv1.NodeSet{
			Name: "data-ingest",
			Config: &commonv1.Config{
				Data: map[string]interface{}{
					esv1.NodeRoles: []string{"data", "ingest"},
				},
			},
		}).
		// Add a ml tier
		WithNodeSet(esv1.NodeSet{
			Name: "ml",
			Config: &commonv1.Config{
				Data: map[string]interface{}{
					esv1.NodeRoles: []string{"ml"},
				},
			},
		}).
		WithAnnotation(
			esv1.ElasticsearchAutoscalingSpecAnnotationName,
			`      {
          "policies": [{
              "name": "data-ingest",
              "roles": ["data", "ingest"],
              "resources": {
                  "nodeCount": { "min": 3, "max": 5 },
                  "cpu": { "min": 2, "max": 8 },
                  "memory": { "min": "2Gi", "max": "16Gi" },
                  "storage": { "min": "12Gi", "max": "42Gi" }
              }
          },
          {
              "name": "ml",
              "roles": ["ml"],
              "resources": {
                  "nodeCount": { "min": 0, "max": 3 },
                  "cpu": { "min": 1, "max": 4 },
                  "memory": { "min": "2Gi", "max": "8Gi" }
              }
          }]
      }`,
		).
		WithRestrictedSecurityContext().
		WithExpectedNodeSets(
			[]esv1.NodeSet{
				{
					Name:  "master",
					Count: 1,
					Config: &commonv1.Config{
						Data: map[string]interface{}{
							esv1.NodeRoles: []string{"master"},
						},
					},
				},
				{
					Name:  "data-ingest",
					Count: 3,
					Config: &commonv1.Config{
						Data: map[string]interface{}{
							esv1.NodeRoles: []string{"data", "ingest"},
						},
					},
				},
				{
					Name:  "ml",
					Count: 0,
					Config: &commonv1.Config{
						Data: map[string]interface{}{
							esv1.NodeRoles: []string{"ml"},
						},
					},
				},
			},
		)
	licenseTestContext := elasticsearch.NewLicenseTestContext(test.NewK8sClientOrFatal(), esBuilder.Elasticsearch)

	licenseBytes, err := ioutil.ReadFile(test.Ctx().TestLicense)
	require.NoError(t, err)
	licenseSecretName := "eck-license"

	before := func(k *test.K8sClient) test.StepList {
		// Deploy a Trial license
		return test.StepList{
			licenseTestContext.DeleteAllEnterpriseLicenseSecrets(),
			licenseTestContext.CreateEnterpriseLicenseSecret(licenseSecretName, licenseBytes),
		}
	}

	stepsFn := func(k *test.K8sClient) test.StepList {
		return test.StepList{
			// Init license test context
			licenseTestContext.Init(),
			// Check that the cluster is using a Platinum license
			licenseTestContext.CheckElasticsearchLicense(client.ElasticsearchLicenseTypePlatinum),
			test.Step{
				Name: "Add some data to the cluster",
				Test: func(t *testing.T) {
					require.NoError(t, elasticsearch.NewDataIntegrityCheck(k, esBuilder).Init())
				},
			},
			test.Step{
				Name: "Check data in still there in the cluster",
				Test: test.Eventually(func() error {
					return elasticsearch.NewDataIntegrityCheck(k, esBuilder).ForIndex(elasticsearch.DataIntegrityIndex).Verify()
				}),
			},
			// Delete enterprise license
			licenseTestContext.DeleteEnterpriseLicenseSecret(licenseSecretName),
		}
	}

	test.Sequence(before, stepsFn, esBuilder).RunSequential(t)
}
