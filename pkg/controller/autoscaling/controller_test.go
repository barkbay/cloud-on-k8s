package autoscaling

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"path/filepath"
	"reflect"
	"testing"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/nodesets"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/status"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/license"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/operator"
	esclient "github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/client"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/services"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	"github.com/elastic/cloud-on-k8s/pkg/utils/net"
	"github.com/ghodss/yaml"
	"github.com/stretchr/testify/require"
	"gotest.tools/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var (
	fetchEvents = func(recorder *record.FakeRecorder) []string {
		events := make([]string, 0)
		select {
		case event := <-recorder.Events:
			events = append(events, event)
		default:
			break
		}
		return events
	}

	fakeService = &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "testns",
			Name:      services.ExternalServiceName("testes"),
		},
	}
	fakeEndpoints = &corev1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "testns",
			Name:      services.ExternalServiceName("testes"),
		},
		Subsets: []corev1.EndpointSubset{{
			Addresses: []corev1.EndpointAddress{{
				IP: "10.0.0.2",
			}},
			Ports: []corev1.EndpointPort{},
		}},
	}
)

func TestReconcile(t *testing.T) {
	type fields struct {
		EsClient       *fakeEsClient
		Parameters     operator.Parameters
		recorder       *record.FakeRecorder
		licenseChecker license.Checker
	}
	type args struct {
		esManifest string
		isOnline   bool
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		want       reconcile.Result
		wantEvents []string
		wantErr    bool
	}{
		{
			name: "Cluster has just been created, initialize resources",
			fields: fields{
				EsClient:       newFakeEsClient(t).withCapacity("elasticsearch-ml-di-max-reached"),
				Parameters:     operator.Parameters{},
				recorder:       record.NewFakeRecorder(1000),
				licenseChecker: &fakeLicenceChecker{},
			},
			args: args{
				esManifest: "elasticsearch-ml-di",
				isOnline:   false,
			},
			want: defaultReconcile,
		},
		{
			name: "Cluster is online, data tier has reached max. capacity",
			fields: fields{
				EsClient:       newFakeEsClient(t).withCapacity("elasticsearch-ml-di-max-reached"),
				Parameters:     operator.Parameters{},
				recorder:       record.NewFakeRecorder(1000),
				licenseChecker: &fakeLicenceChecker{},
			},
			args: args{
				esManifest: "elasticsearch-ml-di-max-reached",
				isOnline:   true,
			},
			want:       defaultReconcile,
			wantEvents: []string{"Warning HorizontalScalingLimitReached Can't provide total required storage 37106614256, max number of nodes is 8, requires 9 nodes"},
		},
		{
			name: "Cluster is online, data tier needs to be scaled up from 8 to 9 nodes",
			fields: fields{
				EsClient:       newFakeEsClient(t).withCapacity("elasticsearch-ml-di-scaled"),
				Parameters:     operator.Parameters{},
				recorder:       record.NewFakeRecorder(1000),
				licenseChecker: &fakeLicenceChecker{},
			},
			args: args{
				esManifest: "elasticsearch-ml-di-scaled",
				isOnline:   true,
			},
			want: defaultReconcile,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Load the actual Elasticsearch resource from the sample files.
			es := esv1.Elasticsearch{}
			bytes, err := ioutil.ReadFile(filepath.Join("testdata", tt.args.esManifest, "elasticsearch.yml"))
			require.NoError(t, err)
			if err := yaml.Unmarshal(bytes, &es); err != nil {
				t.Fatalf("yaml.Unmarshal error = %v, wantErr %v", err, tt.wantErr)
			}

			var k8sClient k8s.Client
			if tt.args.isOnline {
				k8sClient = k8s.WrappedFakeClient(es.DeepCopy(), fakeService, fakeEndpoints)
			} else {
				k8sClient = k8s.WrappedFakeClient(es.DeepCopy())
			}

			r := &ReconcileElasticsearch{
				Client:           k8sClient,
				esClientProvider: tt.fields.EsClient.newFakeElasticsearchClient,
				Parameters:       tt.fields.Parameters,
				recorder:         tt.fields.recorder,
				licenseChecker:   tt.fields.licenseChecker,
			}
			got, err := r.Reconcile(reconcile.Request{NamespacedName: k8s.ExtractNamespacedName(&es)})
			if (err != nil) != tt.wantErr {
				t.Errorf("ReconcileElasticsearch.reconcileInternal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReconcileElasticsearch.reconcileInternal() = %v, want %v", got, tt.want)
			}
			// Get back Elasticsearch from the API Server.
			updatedElasticsearch := esv1.Elasticsearch{}
			require.NoError(t, k8sClient.Get(client.ObjectKey{Namespace: "testns", Name: "testes"}, &updatedElasticsearch))
			// Read expected the expected Elasticsearch resource.
			expectedElasticsearch := esv1.Elasticsearch{}
			bytes, err = ioutil.ReadFile(filepath.Join("testdata", tt.args.esManifest, "elasticsearch-expected.yml"))
			require.NoError(t, err)
			require.NoError(t, yaml.Unmarshal(bytes, &expectedElasticsearch))
			assert.DeepEqual(t, updatedElasticsearch.Spec, expectedElasticsearch.Spec)
			// Check that the autoscaling spec is still the expected one.
			assert.DeepEqual(
				t,
				updatedElasticsearch.Annotations[esv1.ElasticsearchAutoscalingSpecAnnotationName],
				expectedElasticsearch.Annotations[esv1.ElasticsearchAutoscalingSpecAnnotationName],
			)
			// Compare the statuses.
			statusesEqual(t, updatedElasticsearch, expectedElasticsearch)
			// Check event raised
			gotEvents := fetchEvents(tt.fields.recorder)
			require.ElementsMatch(t, tt.wantEvents, gotEvents)
		})
	}
}

func statusesEqual(t *testing.T, got, want esv1.Elasticsearch) {
	gotStatus, err := status.GetStatus(got)
	require.NoError(t, err)
	wantStatus, err := status.GetStatus(want)
	require.NoError(t, err)
	require.Equal(t, len(gotStatus.AutoscalingPolicyStatuses), len(wantStatus.AutoscalingPolicyStatuses))
	for _, wantPolicyStatus := range wantStatus.AutoscalingPolicyStatuses {
		gotPolicyStatus := getPolicyStatus(gotStatus.AutoscalingPolicyStatuses, wantPolicyStatus.Name)
		require.NotNil(t, gotPolicyStatus, "Autoscaling policy not found")
		require.ElementsMatch(t, gotPolicyStatus.NodeSetNodeCount, wantPolicyStatus.NodeSetNodeCount)
		for resource := range wantPolicyStatus.ResourcesSpecification.Requests {
			require.True(t, nodesets.ResourcesEqual(resource, wantPolicyStatus.ResourcesSpecification.Requests, gotPolicyStatus.ResourcesSpecification.Requests))
		}
		for resource := range wantPolicyStatus.ResourcesSpecification.Limits {
			require.True(t, nodesets.ResourcesEqual(resource, wantPolicyStatus.ResourcesSpecification.Requests, gotPolicyStatus.ResourcesSpecification.Requests))
		}
	}

}

func getPolicyStatus(autoscalingPolicyStatuses []status.AutoscalingPolicyStatus, name string) *status.AutoscalingPolicyStatus {
	for _, policyStatus := range autoscalingPolicyStatuses {
		if policyStatus.Name == name {
			return &policyStatus
		}
	}
	return nil
}

// - Fake Elasticsearch Autoscaling Client

type fakeEsClient struct {
	t *testing.T
	esclient.Client

	autoscalingPolicies esclient.Policies

	policiesCleaned bool
	updatedPolicies map[string]esv1.AutoscalingPolicy
}

func newFakeEsClient(t *testing.T) *fakeEsClient {
	return &fakeEsClient{
		t:                   t,
		autoscalingPolicies: esclient.Policies{Policies: make(map[string]esclient.PolicyInfo)},
		updatedPolicies:     make(map[string]esv1.AutoscalingPolicy),
	}
}

func (f *fakeEsClient) withCapacity(testdata string) *fakeEsClient {
	policies := esclient.Policies{}
	bytes, err := ioutil.ReadFile("testdata/" + testdata + "/capacity.json")
	if err != nil {
		f.t.Fatalf("Error while reading autoscaling capacity content: %v", err)
	}
	if err := json.Unmarshal(bytes, &policies); err != nil {
		f.t.Fatalf("Error while parsing autoscaling capacity content: %v", err)
	}
	f.autoscalingPolicies = policies
	return f
}

func (f *fakeEsClient) newFakeElasticsearchClient(_ k8s.Client, _ net.Dialer, _ esv1.Elasticsearch) (esclient.Client, error) {
	return f, nil
}

func (f *fakeEsClient) DeleteAutoscalingAutoscalingPolicies(_ context.Context) error {
	f.policiesCleaned = true
	return nil
}
func (f *fakeEsClient) UpsertAutoscalingPolicy(_ context.Context, policyName string, autoscalingPolicy esv1.AutoscalingPolicy) error {
	return nil
}
func (f *fakeEsClient) GetAutoscalingCapacity(_ context.Context) (esclient.Policies, error) {
	return f.autoscalingPolicies, nil
}
func (f *fakeEsClient) UpdateMLNodesSettings(_ context.Context, maxLazyMLNodes int32, maxMemory string) error {
	return nil
}

// - Fake licence checker

type fakeLicenceChecker struct{}

func (flc *fakeLicenceChecker) CurrentEnterpriseLicense() (*license.EnterpriseLicense, error) {
	return nil, nil
}

func (flc *fakeLicenceChecker) EnterpriseFeaturesEnabled() (bool, error) {
	return true, nil
}

func (flc *fakeLicenceChecker) Valid(l license.EnterpriseLicense) (bool, error) {
	return true, nil
}
