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

var podLog = ulog.Log.WithName("pod-az")

// +kubebuilder:webhook:path="/elastic-pod-az-k8s-elastic-co-v1-elasticsearch",mutating=true,failurePolicy=Fail,groups="",resources=pods,verbs=create,versions=v1,name=elastic-pod-blocker.k8s.elastic.co,sideEffects=None,admissionReviewVersions=v1;v1beta1,matchPolicy=Exact
const (
	podAZWebhookPath = "/elastic-pod-az-k8s-elastic-co-v1-elasticsearch"
)

func RegisterPodVacateAZWebhook(mgr ctrl.Manager) {
	wh := &podAZWebhook{
		client: mgr.GetClient(),
	}
	podLog.Info("Registering pod blocker webhook", "path", podAZWebhookPath)
	mgr.GetWebhookServer().Register(podAZWebhookPath, &webhook.Admission{Handler: wh})
}

type podAZWebhook struct {
	client  k8s.Client
	decoder *admission.Decoder
}

var _ admission.DecoderInjector = &podAZWebhook{}

// InjectDecoder injects the decoder automatically.
func (wh *podAZWebhook) InjectDecoder(d *admission.Decoder) error {
	wh.decoder = d
	return nil
}

// Handle blocks creation of the temporary Pod and PVC until the temporary PVC is created from the original one
func (wh *podAZWebhook) Handle(_ context.Context, req admission.Request) admission.Response {
	pod := &corev1.Pod{}
	err := wh.decoder.DecodeRaw(req.Object, pod)
	if err != nil {
		pvcLog.Error(err, fmt.Sprintf("error DecodeRaw/POD for req %s/%s", req.Namespace, req.Name))
		return admission.Errored(http.StatusBadRequest, err)
	}

	if req.Operation == admissionv1.Create {
		// Check vacate annotation on Pod

		// Only allow Pod creation if PVC has been created

		//return admission.Denied(err.Error())

	}
	return admission.Allowed("")
}
