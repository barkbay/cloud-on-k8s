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
	DelayAnnotation   = "elasticsearch.k8s.elastic.co/delay-creation"
	TrustMeAnnotation = "elasticsearch.k8s.elastic.co/trust-me-i-m-an-engineer"
)

var (
	pvcLog = ulog.Log.WithName("pvc-blocker")
	podLog = ulog.Log.WithName("pod-blocker")
)

// +kubebuilder:webhook:path="/elastic-pvc-blocker-k8s-elastic-co-v1-elasticsearch",mutating=false,failurePolicy=Fail,groups="",resources=persistentvolumeclaims,verbs=create,versions=v1,name=elastic-pvc-blocker.k8s.elastic.co,sideEffects=None,admissionReviewVersions=v1;v1beta1,matchPolicy=Exact

// +kubebuilder:webhook:path="/elastic-pod-blocker-k8s-elastic-co-v1-elasticsearch",mutating=false,failurePolicy=Fail,groups="",resources=pods,verbs=create,versions=v1,name=elastic-pod-blocker.k8s.elastic.co,sideEffects=None,admissionReviewVersions=v1;v1beta1,matchPolicy=Exact

const (
	pvcBlockerWebhookPath = "/elastic-pvc-blocker-k8s-elastic-co-v1-elasticsearch"
	podBlockerWebhookPath = "/elastic-pod-blocker-k8s-elastic-co-v1-elasticsearch"
)

func RegisterPodBlockerWebhook(mgr ctrl.Manager) {
	wh := &podBlockerWebhook{
		client: mgr.GetClient(),
	}
	podLog.Info("Registering pod blocker webhook", "path", podBlockerWebhookPath)
	mgr.GetWebhookServer().Register(podBlockerWebhookPath, &webhook.Admission{Handler: wh})
}

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
	pvcLog.Info(fmt.Sprintf("Handling change for PVC %s/%s", pvc.Namespace, pvc.Name))
	if pvc.Annotations == nil {
		pvcLog.Info(fmt.Sprintf("Handling change for PVC %s/%s - no annotations: allowed", pvc.Namespace, pvc.Name))
		return admission.Allowed("")
	}
	if req.Operation == admissionv1.Create {
		pvcLog.Info(fmt.Sprintf("Handling PVC %s/%s CREATION [%+v]", pvc.Namespace, pvc.Name, pvc))
		_, delay := pvc.Annotations[DelayAnnotation]
		if delay {
			pvcLog.Info(fmt.Sprintf("PVC %s/%s CREATION is DELAYED", pvc.Namespace, pvc.Name))
			// Also check if there is the trust annotation
			_, trustedCreation := pvc.Annotations[TrustMeAnnotation]
			if trustedCreation {
				pvcLog.Info(fmt.Sprintf("PVC %s/%s CREATION is ALLOWED", pvc.Namespace, pvc.Name))
				return admission.Allowed("this is an engineer")
			}
			pvcLog.Info(fmt.Sprintf("PVC %s/%s CREATION is STILL DELAYED", pvc.Namespace, pvc.Name))
			return admission.Denied(fmt.Sprintf("Creation of PVC %s/%s delayed", pvc.Namespace, pvc.Name))
		}
		pvcLog.Info(fmt.Sprintf("Handling PVC %s/%s - no annotation - CREATION is ALLOWED", pvc.Namespace, pvc.Name))
	}
	return admission.Allowed("")
}

type podBlockerWebhook struct {
	client  k8s.Client
	decoder *admission.Decoder
}

var _ admission.DecoderInjector = &podBlockerWebhook{}

// InjectDecoder injects the decoder automatically.
func (wh *podBlockerWebhook) InjectDecoder(d *admission.Decoder) error {
	wh.decoder = d
	return nil
}

// Handle blocks creation of the temporary Pod and PVC until the temporary PVC is created from the original one
func (wh *podBlockerWebhook) Handle(_ context.Context, req admission.Request) admission.Response {
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
