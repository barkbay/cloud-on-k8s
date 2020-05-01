// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package link

import (
	"crypto/sha256"
	"fmt"
	"strconv"

	"github.com/elastic/cloud-on-k8s/pkg/controller/common"

	"golang.org/x/crypto/bcrypt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/elastic/cloud-on-k8s/pkg/controller/apmserver"

	apmv1 "github.com/elastic/cloud-on-k8s/pkg/apis/apm/v1"

	kbv1 "github.com/elastic/cloud-on-k8s/pkg/apis/kibana/v1"

	apmname "github.com/elastic/cloud-on-k8s/pkg/controller/apmserver/name"

	"github.com/elastic/cloud-on-k8s/pkg/controller/kibana"

	"github.com/elastic/cloud-on-k8s/pkg/controller/common/name"

	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/network"

	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/services"
	"github.com/elastic/cloud-on-k8s/pkg/controller/elasticsearch/user"

	"github.com/elastic/cloud-on-k8s/pkg/controller/common/reconciler"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	esv1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1"

	elasticsearchv1alpha1 "github.com/elastic/cloud-on-k8s/pkg/apis/elasticsearch/v1alpha1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/certificates"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	logf "sigs.k8s.io/controller-runtime/pkg/runtime/log"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("link-controller")

// Add creates a new Link Controller and adds it to the Manager with default RBAC. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	client := k8s.WrapClient(mgr.GetClient())
	return &ReconcileLink{Client: client, scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("link-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to Link
	err = c.Watch(&source.Kind{Type: &elasticsearchv1alpha1.Link{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// TODO(user): Modify this to be the types you create
	// Uncomment watch a Deployment created by Link - change this for objects you create
	err = c.Watch(&source.Kind{Type: &appsv1.Deployment{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &elasticsearchv1alpha1.Link{},
	})
	if err != nil {
		return err
	}

	return nil
}

var _ reconcile.Reconciler = &ReconcileLink{}

// ReconcileLink reconciles a Link object
type ReconcileLink struct {
	k8s.Client
	scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=elasticsearch.k8s.elastic.co,resources=links,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=elasticsearch.k8s.elastic.co,resources=links/status,verbs=get;update;patch
func (r *ReconcileLink) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	// Fetch the Link instance
	instance := &elasticsearchv1alpha1.Link{}
	err := r.Get(request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Object not found, return.  Created objects are automatically garbage collected.
			// For additional cleanup logic use finalizers.
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	// Get Elasticsearch
	es := &esv1.Elasticsearch{}
	if len(instance.Spec.ElasticsearchName) > 0 {
		if err := r.Client.Get(types.NamespacedName{
			Name:      instance.Spec.ElasticsearchName,
			Namespace: instance.Namespace,
		}, es); err != nil {
			return reconcile.Result{}, err
		}
	}

	// Get Kibana
	kb := &kbv1.Kibana{}
	if len(instance.Spec.KibanaName) > 0 {
		if err := r.Client.Get(types.NamespacedName{
			Name:      instance.Spec.KibanaName,
			Namespace: instance.Namespace,
		}, kb); err != nil {
			return reconcile.Result{}, err
		}
	}

	// Get APMServer
	apm := &apmv1.ApmServer{}
	if len(instance.Spec.ApmServerName) > 0 {
		if err := r.Client.Get(types.NamespacedName{
			Name:      instance.Spec.ApmServerName,
			Namespace: instance.Namespace,
		}, apm); err != nil {
			return reconcile.Result{}, err
		}
	}

	// TODO: Create the Secret in the target namespaces
	namespaces := make(map[string]struct{})
	for _, deployment := range instance.Spec.DeploymentRefs {
		if len(deployment.Namespace) == 0 {
			namespaces[instance.Namespace] = struct{}{}
		} else {
			namespaces[deployment.Namespace] = struct{}{}
		}
	}
	for _, daemonset := range instance.Spec.DaemonSetRefs {
		if len(daemonset.Namespace) == 0 {
			namespaces[instance.Namespace] = struct{}{}
		} else {
			namespaces[daemonset.Namespace] = struct{}{}
		}
	}

	// reconcile Secret in those namespaces
	secretData := make(map[string][]byte)
	// Build a checksum of the configuration, add it to the pod labels so a change triggers a rolling update
	configChecksum := sha256.New224()

	// 1a. Get the http crt for Elasticsearch
	if len(instance.Spec.ElasticsearchName) > 0 {
		ca, httpCert, err := r.GetHttpPublicCerts(esv1.ESNamer, instance.Namespace, instance.Spec.ElasticsearchName)
		if err != nil {
			return reconcile.Result{}, err
		}
		secretData["elasticsearch-"+certificates.CAFileName] = ca
		_, _ = configChecksum.Write(ca)
		secretData["elasticsearch-"+certificates.CertFileName] = httpCert
		_, _ = configChecksum.Write(httpCert)
	}

	// 1b. Get the http crt for Kibana
	if len(instance.Spec.KibanaName) > 0 {
		ca, httpCert, err := r.GetHttpPublicCerts(kibana.Namer, instance.Namespace, instance.Spec.KibanaName)
		if err != nil {
			return reconcile.Result{}, err
		}
		secretData["kibana-"+certificates.CAFileName] = ca
		_, _ = configChecksum.Write(ca)
		secretData["kibana-"+certificates.CertFileName] = httpCert
		_, _ = configChecksum.Write(httpCert)
	}

	// 1c. Get the http crt for APMServer
	if len(instance.Spec.ApmServerName) > 0 {
		ca, httpCert, err := r.GetHttpPublicCerts(apmname.APMNamer, instance.Namespace, instance.Spec.ApmServerName)
		if err != nil {
			return reconcile.Result{}, err
		}
		secretData["apmserver-"+certificates.CAFileName] = ca
		_, _ = configChecksum.Write(ca)
		secretData["apmserver-"+certificates.CertFileName] = httpCert
		_, _ = configChecksum.Write(httpCert)
	}

	// TODO: Move this to the end
	// 2. Create the Secret
	linkCert := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: "link-" + instance.Name,
			Labels: map[string]string{
				"elasticsearch.k8s.elastic.co/link": instance.Name,
				"common.k8s.elastic.co/type":        "link",
			},
		},
		Data: secretData,
	}

	// 3. Set hosts
	if len(instance.Spec.ElasticsearchName) > 0 {
		linkCert.Data["elasticsearch-host"] = []byte(services.ExternalServiceHost(*es))
		_, _ = configChecksum.Write(linkCert.Data["elasticsearch-host"])
		linkCert.Data["elasticsearch-port"] = []byte(strconv.Itoa(network.HTTPPort))
		_, _ = configChecksum.Write(linkCert.Data["elasticsearch-port"])
	}
	if len(instance.Spec.KibanaName) > 0 {
		linkCert.Data["kibana-host"] = []byte(kibana.HTTPService(kb.Name) + "." + kb.Namespace + ".svc")
		_, _ = configChecksum.Write(linkCert.Data["kibana-host"])
		linkCert.Data["kibana-port"] = []byte(strconv.Itoa(kibana.HTTPPort))
		_, _ = configChecksum.Write(linkCert.Data["kibana-host"])
	}
	if len(instance.Spec.ApmServerName) > 0 {
		linkCert.Data["apmserver-host"] = []byte(apmname.HTTPService(apm.Name) + "." + apm.Namespace + ".svc")
		_, _ = configChecksum.Write(linkCert.Data["apmserver-host"])
		linkCert.Data["apmserver-port"] = []byte(strconv.Itoa(apmserver.HTTPPort))
		_, _ = configChecksum.Write(linkCert.Data["apmserver-host"])
	}

	applicationsUsers := &corev1.Secret{}
	if err := r.Client.Get(client.ObjectKey{
		Namespace: es.Namespace,
		Name:      esv1.ApplicationsUsersSecret(es.Name),
	}, applicationsUsers); err != nil && !errors.IsNotFound(err) {
		return reconcile.Result{}, err
	}

	if len(applicationsUsers.Data) == 0 {
		applicationsUsers.Data = make(map[string][]byte)
	}

	// Finally, create the secrets in the target namespaces
	for namespace := range namespaces {
		namespacedLinkCert := linkCert.DeepCopy()
		namespacedLinkCert.Namespace = namespace
		username := instance.Namespace + "-" + instance.Name + "-" + namespace

		// Get the current Secret to maybe reconcile password
		reconciled := &corev1.Secret{}
		if err := r.Client.Get(client.ObjectKey{
			Namespace: namespace,
			Name:      namespacedLinkCert.Name,
		}, reconciled); err != nil && !errors.IsNotFound(err) {
			return reconcile.Result{}, err
		}

		reconciled.Data["username"] = []byte(username)
		// Get the clear-text password
		var password []byte
		if existingPassword, exists := reconciled.Data["password"]; exists {
			password = existingPassword
		} else {
			password = common.FixedLengthRandomPasswordBytes()
		}
		reconciled.Data["password"] = password

		// Get the hashed password and reuse the existing hash if valid
		var bcryptHash []byte
		if existingHash, ok := applicationsUsers.Data[username]; !ok {
			if bcrypt.CompareHashAndPassword(existingHash, password) == nil {
				bcryptHash = existingHash
			}
		}

		if bcryptHash == nil {
			bcryptHash, err = bcrypt.GenerateFromPassword(password, bcrypt.DefaultCost)
			if err != nil {
				return reconcile.Result{}, err
			}
		}
		applicationsUsers.Data[username] = bcryptHash
		if _, err := reconciler.ReconcileSecret(r.Client, linkCert, nil); err != nil {
			return reconcile.Result{}, err
		}
	}
	if _, err := reconciler.ReconcileSecret(r.Client, *applicationsUsers, es); err != nil {
		return reconcile.Result{}, err
	}

	// Update the DaemonSets
	for _, daemonset := range instance.Spec.DaemonSetRefs {
		namespace := daemonset.Namespace
		if len(namespace) == 0 {
			namespace = instance.Namespace
		}

		ds := &appsv1.DaemonSet{}
		if err := r.Client.Get(types.NamespacedName{
			Namespace: namespace,
			Name:      daemonset.Name,
		}, ds); err != nil {
			return reconcile.Result{}, err
		}

		if ds.Spec.Template.Annotations == nil {
			ds.Spec.Template.Annotations = make(map[string]string)
		}
		ds.Spec.Template.Annotations["elastic.config.checksum"] = fmt.Sprintf("%x", configChecksum.Sum(nil))

		for i := range ds.Spec.Template.Spec.Containers {
			container := &ds.Spec.Template.Spec.Containers[i]
			if hasElasticsearchVolumeMount(container.VolumeMounts) {
				continue
			}
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
				Name:      "elastic",
				MountPath: "/mnt/elastic/",
				ReadOnly:  true,
			})
		}

		for i := range ds.Spec.Template.Spec.Containers {
			container := &ds.Spec.Template.Spec.Containers[i]
			if len(instance.Spec.ElasticsearchName) > 0 {
				container.Env = setEnvVariable(container.Env, "ELASTICSEARCH_HOST", linkCert.Name, "elasticsearch-host")
				container.Env = setEnvVariable(container.Env, "ELASTICSEARCH_PORT", linkCert.Name, "elasticsearch-port")
			}
			if len(instance.Spec.KibanaName) > 0 {
				container.Env = setEnvVariable(container.Env, "KIBANA_HOST", linkCert.Name, "kibana-host")
				container.Env = setEnvVariable(container.Env, "KIBANA_PORT", linkCert.Name, "kibana-port")
			}
			if len(instance.Spec.ApmServerName) > 0 {
				container.Env = setEnvVariable(container.Env, "APMSERVER_HOST", linkCert.Name, "apmserver-host")
				container.Env = setEnvVariable(container.Env, "APMSERVER_PORT", linkCert.Name, "apmserver-port")
			}
			container.Env = setEnvVariable(container.Env, "ELASTICSEARCH_USERNAME", linkCert.Name, "username")
			container.Env = setEnvVariable(container.Env, "ELASTICSEARCH_PASSWORD", linkCert.Name, "password")
		}

		if !hasElasticsearchVolume(ds.Spec.Template.Spec.Volumes) {
			ds.Spec.Template.Spec.Volumes = append(ds.Spec.Template.Spec.Volumes, corev1.Volume{
				Name: "elastic",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: linkCert.Name,
					},
				},
			})
		}

		// Update the daemonset
		if err := r.Client.Update(ds); err != nil {
			return reconcile.Result{}, err
		}

	}

	// Update the Deployments
	for _, deployment := range instance.Spec.DeploymentRefs {
		namespace := deployment.Namespace
		if len(namespace) == 0 {
			namespace = instance.Namespace
		}

		ds := &appsv1.Deployment{}
		if err := r.Client.Get(types.NamespacedName{
			Namespace: namespace,
			Name:      deployment.Name,
		}, ds); err != nil {
			return reconcile.Result{}, err
		}

		if ds.Spec.Template.Annotations == nil {
			ds.Spec.Template.Annotations = make(map[string]string)
		}
		ds.Spec.Template.Annotations["elastic.config.checksum"] = fmt.Sprintf("%x", configChecksum.Sum(nil))

		for i := range ds.Spec.Template.Spec.Containers {
			container := &ds.Spec.Template.Spec.Containers[i]
			if hasElasticsearchVolumeMount(container.VolumeMounts) {
				continue
			}
			container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
				Name:      "elasticsearch",
				MountPath: "/mnt/elastic/",
				ReadOnly:  true,
			})
		}

		for i := range ds.Spec.Template.Spec.Containers {
			container := &ds.Spec.Template.Spec.Containers[i]
			if len(instance.Spec.ElasticsearchName) > 0 {
				container.Env = setEnvVariable(container.Env, "ELASTICSEARCH_HOST", linkCert.Name, "elasticsearch-host")
				container.Env = setEnvVariable(container.Env, "ELASTICSEARCH_PORT", linkCert.Name, "elasticsearch-port")
			}
			if len(instance.Spec.KibanaName) > 0 {
				container.Env = setEnvVariable(container.Env, "KIBANA_HOST", linkCert.Name, "kibana-host")
				container.Env = setEnvVariable(container.Env, "KIBANA_PORT", linkCert.Name, "kibana-port")
			}
			if len(instance.Spec.ApmServerName) > 0 {
				container.Env = setEnvVariable(container.Env, "APMSERVER_HOST", linkCert.Name, "apmserver-host")
				container.Env = setEnvVariable(container.Env, "APMSERVER_PORT", linkCert.Name, "apmserver-port")
			}
			container.Env = setEnvVariable(container.Env, "ELASTICSEARCH_USERNAME", linkCert.Name, "username")
			container.Env = setEnvVariable(container.Env, "ELASTICSEARCH_PASSWORD", linkCert.Name, "password")
		}

		if !hasElasticsearchVolume(ds.Spec.Template.Spec.Volumes) && len(instance.Spec.ElasticsearchName) > 0 {
			ds.Spec.Template.Spec.Volumes = append(ds.Spec.Template.Spec.Volumes, corev1.Volume{
				Name: "elasticsearch",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: linkCert.Name,
					},
				},
			})
		}

		// Update the daemonset
		if err := r.Client.Update(ds); err != nil {
			return reconcile.Result{}, err
		}

	}

	return reconcile.Result{}, nil
}

func setEnvVariable(vars []corev1.EnvVar, varName, secretName, secretKey string) []corev1.EnvVar {
	// Check if env var exists
	for _, envVar := range vars {
		if envVar.Name == varName {
			return vars
		}
	}
	// Insert if not
	vars = append(vars, corev1.EnvVar{
		Name: varName,
		ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: secretName,
				},
				Key: secretKey,
			},
		},
	})
	return vars
}

func hasElasticsearchVolumeMount(vms []corev1.VolumeMount) bool {
	for _, vm := range vms {
		if vm.Name == "elasticsearch" {
			return true
		}
	}
	return false
}

func hasElasticsearchVolume(vms []corev1.Volume) bool {
	for _, vm := range vms {
		if vm.Name == "elasticsearch" {
			return true
		}
	}
	return false
}

func (r *ReconcileLink) GetElasticUserPassword(esNamespace, esName string) (password []byte, err error) {
	var secret corev1.Secret
	key := types.NamespacedName{
		Namespace: esNamespace,
		Name:      esv1.ElasticUserSecret(esName), //esv1.ESNamer.Suffix(esName, string(certificates.HTTPCAType)),
	}
	if err := r.Client.Get(key, &secret); err != nil {
		return nil, err
	}
	var exists bool
	password, exists = secret.Data[user.ElasticUserName]
	if !exists {
		return nil, fmt.Errorf("no value found for cert in secret %s", certificates.CAFileName)
	}
	return
}

// GetCA returns the CA of the given owner name
func (r *ReconcileLink) GetHttpPublicCerts(namer name.Namer, esNamespace, esName string) (ca []byte, httpCert []byte, err error) {
	var secret corev1.Secret
	key := types.NamespacedName{
		Namespace: esNamespace,
		Name:      certificates.PublicCertsSecretName(namer, esName), //esv1.ESNamer.Suffix(esName, string(certificates.HTTPCAType)),
	}
	if err := r.Client.Get(key, &secret); err != nil {
		return nil, nil, err
	}

	var exists bool
	ca, exists = secret.Data[certificates.CAFileName]
	if !exists {
		return nil, nil, fmt.Errorf("no value found for cert in secret %s", certificates.CAFileName)
	}
	httpCert, exists = secret.Data[certificates.CertFileName]
	if !exists {
		return nil, nil, fmt.Errorf("no value found for cert in secret %s", certificates.CertFileName)
	}
	return
}
