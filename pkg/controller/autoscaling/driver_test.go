package autoscaling

import (
	"context"
	"reflect"
	"testing"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/autoscaling/status"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/license"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/operator"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestReconcileElasticsearch_reconcileInternal(t *testing.T) {
	type fields struct {
		Client         k8s.Client
		Parameters     operator.Parameters
		recorder       record.EventRecorder
		licenseChecker license.Checker
	}
	type args struct {
		autoscalingStatus status.Status
		namedTiers        esv1.AutoscaledNodeSets
		autoscalingSpecs  esv1.AutoscalingSpec
		es                esv1.Elasticsearch
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    reconcile.Result
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &ReconcileElasticsearch{
				Client:         tt.fields.Client,
				Parameters:     tt.fields.Parameters,
				recorder:       tt.fields.recorder,
				licenseChecker: tt.fields.licenseChecker,
			}
			got, err := r.reconcileInternal(context.Background(), tt.args.autoscalingStatus, tt.args.namedTiers, tt.args.autoscalingSpecs, tt.args.es)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReconcileElasticsearch.reconcileInternal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReconcileElasticsearch.reconcileInternal() = %v, want %v", got, tt.want)
			}
		})
	}
}
