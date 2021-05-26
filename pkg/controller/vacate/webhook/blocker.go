package webhook

import (
	"context"
	"fmt"
	"net/http"

	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	ulog "github.com/elastic/cloud-on-k8s/pkg/utils/log"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const (
	DelayAnnotation = "elasticsearch.k8s.elastic.co/delay-creation"
)

var (
	pvcLog = ulog.Log.WithName("pvc-blocker")
)

// +kubebuilder:webhook:path="/elastic-pvc-blocker-k8s-elastic-co-v1-elasticsearch",mutating=false,failurePolicy=Fail,groups="",resources=persistentvolumeclaims,verbs=create,versions=v1,name=elastic-pvc-blocker.k8s.elastic.co,sideEffects=None,admissionReviewVersions=v1;v1beta1,matchPolicy=Exact

const (
	pvcBlockerWebhookPath = "/elastic-pvc-blocker-k8s-elastic-co-v1-elasticsearch"
)

func RegisterPVCBlockerWebhook(mgr ctrl.Manager) {
	wh := &pvcBlockerWebhook{
		client: mgr.GetClient(),
	}
	pvcLog.Info("Registering PVC blocker webhook", "path", pvcBlockerWebhookPath)
	mgr.GetWebhookServer().Register(pvcBlockerWebhookPath, &webhook.Admission{Handler: wh})
}

type pvcBlockerWebhook struct {
	client  k8s.Client
	decoder *admission.Decoder
}

var _ admission.DecoderInjector = &pvcBlockerWebhook{}

// InjectDecoder injects the decoder automatically.
func (wh *pvcBlockerWebhook) InjectDecoder(d *admission.Decoder) error {
	wh.decoder = d
	return nil
}

// Handle blocks creation of the temporary Pod and PVC until the temporary PVC is created from the original one
func (wh *pvcBlockerWebhook) Handle(_ context.Context, req admission.Request) admission.Response {
	pvc := &corev1.PersistentVolumeClaim{}
	err := wh.decoder.DecodeRaw(req.Object, pvc)
	if err != nil {
		pvcLog.Error(err, fmt.Sprintf("error DecodeRaw/PVC for req %s/%s", req.Namespace, req.Name))
		return admission.Errored(http.StatusBadRequest, err)
	}
	// TODO: should be fine to do _, delay := pvc.Annotations[DelayAnnotation] here
	if req.Operation == admissionv1.Create && len(pvc.Annotations) > 0 {
		_, delay := pvc.Annotations[DelayAnnotation]
		if delay {
			// TODO: use k/v in logs
			pvcLog.Info(fmt.Sprintf("%s on PVC %s/%s - DENIED", req.Operation, pvc.Namespace, pvc.Name))
			return admission.Denied(fmt.Sprintf("Creation of PVC %s/%s delayed", pvc.Namespace, pvc.Name))
		}
	}
	pvcLog.Info(fmt.Sprintf("%s on PVC %s/%s - ALLOWED", req.Operation, pvc.Namespace, pvc.Name))
	return admission.Allowed("")
}
