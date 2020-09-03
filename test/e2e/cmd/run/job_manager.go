package run

import (
	"context"
	"fmt"
	"time"

	"github.com/elastic/cloud-on-k8s/test/e2e/test"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// JobsManager represents a test session running on a remote K8S cluster.
type JobsManager struct {
	*helper
	cache.SharedInformer
	context.Context
	cancelFunc context.CancelFunc
	*kubernetes.Clientset

	jobs map[string]*Job
	err  error // used to notify that an error occurred in this session
}

func NewJobsManager(client *kubernetes.Clientset, h *helper) *JobsManager {
	factory := informers.NewSharedInformerFactoryWithOptions(client, kubePollInterval,
		informers.WithNamespace(h.testContext.E2ENamespace),
		informers.WithTweakListOptions(func(opt *metav1.ListOptions) {
			opt.LabelSelector = fmt.Sprintf("%s=%s", testRunLabel, h.testContext.TestRun)
		}))
	ctx, cancelFunc := context.WithTimeout(context.Background(), jobTimeout)
	return &JobsManager{
		helper:         h,
		Clientset:      client,
		SharedInformer: factory.Core().V1().Pods().Informer(),
		Context:        ctx,
		jobs:           map[string]*Job{},
		cancelFunc:     cancelFunc,
	}
}

// Schedule schedules the Job. The Job is started once the dependency is fulfilled.
// The Job is stopped if the dependency is stopped.
func (jm *JobsManager) Schedule(jobs ...*Job) {
	for _, job := range jobs {
		j := job
		jm.jobs[j.jobName] = j
		go func() {
			// Check if dependency is started
			if j.dependency != nil {
				log.Info("Waiting for dependency job to be started", "job_name", j.jobName, "dependency_name", j.dependency.jobName)
				j.dependency.running.Wait()
			}
			// Check if context is still valid
			if jm.Err() != nil {
				log.Info("Skip job creation", "job_name", j.jobName)
				j.running.Done()
				return
			}
			// Create the Job
			log.Info("Creating job", "job_name", j.jobName)
			err := jm.helper.kubectlApplyTemplateWithCleanup(j.templatePath,
				struct {
					Context test.Context
				}{
					Context: jm.helper.testContext,
				},
			)
			if err != nil {
				log.Error(err, "Error while creating Job", "job_name", j.jobName, "path", j.templatePath)
				jm.err = err
				jm.Stop()
			}
		}()
	}
}

func (jm *JobsManager) Start() {
	log.Info("Starting test session")

	defer func() {
		jm.cancelFunc()
		if deadline, _ := jm.Deadline(); deadline.Before(time.Now()) {
			log.Info("Test job timeout exceeded", "timeout", jobTimeout)
		}
		runtime.HandleCrash()
	}()

	jm.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*corev1.Pod)
			log.Info("Pod added", "name", pod.Name)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			newPod := newObj.(*corev1.Pod)
			jobName, hasJobName := newPod.Labels["job-name"]
			if !hasJobName {
				// Unmanaged Job/Pod, this should not happen if the label selector is correct, harmless but report it in the logs.
				log.Error(errors.New("received an update event for an unmanaged Pod"), "namespace", newPod.Namespace, "name", newPod.Name)
				return
			}
			job, ok := jm.jobs[jobName]
			if !ok {
				// Same as previous, it seems to be an unmanaged Job/Pod.
				log.Error(errors.New("received an update event for an unmanaged Pod"), "namespace", newPod.Namespace, "name", newPod.Name)
				return
			}
			switch newPod.Status.Phase {
			case corev1.PodRunning:
				job.onPodEvent(jm.Clientset, newPod)
			case corev1.PodSucceeded:
				if job.podSucceeded {
					// already done, but the informer is not stopped yet so this code is still running
					return
				}
				log.Info("Pod succeeded", "name", newPod.Name, "status", newPod.Status.Phase)
				job.onPodEvent(jm.Clientset, newPod)
				job.WaitForLogs()
				jm.Stop()
			case corev1.PodFailed:
				// One of the managed Job/Pod has failed, wait for logs and return.
				jm.err = errors.Errorf("Pod %s has failed", newPod.Name)
				log.Error(jm.err, "Pod is in failed state", "name", newPod.Name)
				job.WaitForLogs()
				jm.Stop()
			default:
				log.Info("Waiting for pod to be ready", "name", newPod.Name, "status", newPod.Status.Phase)
			}
		},
		DeleteFunc: func(obj interface{}) {
			pod := obj.(*corev1.Pod)
			log.Info("Pod deleted", "name", pod.Name)
			jm.Stop()
		},
	})
	jm.Run(jm.Done())
}

func (jm *JobsManager) Stop() {
	for _, job := range jm.jobs {
		job.Stop()
	}
	jm.cancelFunc()
}
