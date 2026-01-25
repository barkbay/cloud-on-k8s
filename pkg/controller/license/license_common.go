// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License 2.0;
// you may not use this file except in compliance with the Elastic License 2.0.

package license

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	commonv1 "github.com/elastic/cloud-on-k8s/v3/pkg/apis/common/v1"
	escommon "github.com/elastic/cloud-on-k8s/v3/pkg/apis/elasticsearch/common"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/events"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/license"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/reconciler"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/common/version"
	esclient "github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/client"
	eslabel "github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/label"
	"github.com/elastic/cloud-on-k8s/v3/pkg/controller/elasticsearch/sset"
	"github.com/elastic/cloud-on-k8s/v3/pkg/utils/k8s"
	ulog "github.com/elastic/cloud-on-k8s/v3/pkg/utils/log"
)

const (
	// defaultSafetyMargin is the duration used by this controller to ensure licenses are updated well before expiry
	// In case of any operational issues affecting this controller clusters will have enough runway on their current license.
	defaultSafetyMargin  = 30 * 24 * time.Hour
	minimumRetryInterval = 1 * time.Hour
)

// nextReconcile calculates the next reconciliation time based on license expiry.
func nextReconcile(expiry time.Time, safety time.Duration) time.Duration {
	return nextReconcileRelativeTo(time.Now(), expiry, safety)
}

// nextReconcileRelativeTo calculates the next reconciliation time relative to a given time.
func nextReconcileRelativeTo(now, expiry time.Time, safety time.Duration) time.Duration {
	// short-circuit to default if no expiry given
	if expiry.IsZero() {
		return minimumRetryInterval
	}
	// requeue at expiry minus safetyMargin/2 to ensure we actually reissue a license on the next attempt
	requeueAfter := expiry.Add(-1 * (safety / 2)).Sub(now)
	if requeueAfter <= 0 {
		return reconciler.DefaultRequeue
	}
	return requeueAfter
}

// findLicense tries to find the best Elastic stack license available.
func findLicense(
	ctx context.Context,
	c k8s.Client,
	checker license.Checker,
	recorder record.EventRecorder,
	minVersion *version.Version,
) (esclient.License, string, bool) {
	licenseList, errs := license.EnterpriseLicensesOrErrors(c)
	if len(errs) > 0 {
		ulog.FromContext(ctx).Error(errors.Join(errs...), "Ignoring invalid license objects")
		recordInvalidLicenseEvents(errs, recorder)
	}
	valid := func(l license.EnterpriseLicense) (bool, error) {
		return checker.Valid(ctx, l)
	}
	return license.BestMatch(ctx, minVersion, licenseList, valid)
}

// recordInvalidLicenseEvents records events for invalid licenses.
func recordInvalidLicenseEvents(errs []error, recorder record.EventRecorder) {
	for _, err := range errs {
		var licenseErr *license.Error
		if errors.As(err, &licenseErr) {
			recorder.Event(licenseErr.Source, corev1.EventTypeWarning, events.EventReasonInvalidLicense, err.Error())
		}
	}
}

// reconcileClusterLicenseSecret upserts a secret in the namespace of the Elasticsearch cluster containing the signature of its license.
func reconcileClusterLicenseSecret(
	ctx context.Context,
	c k8s.Client,
	cluster escommon.ElasticsearchCluster,
	parent string,
	esLicense esclient.License,
) error {
	secretName := escommon.LicenseSecretName(cluster)

	licenseBytes, err := json.Marshal(esLicense)
	if err != nil {
		return err
	}

	expected := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: cluster.GetNamespace(),
			Labels: map[string]string{
				commonv1.TypeLabelName:    license.Type,
				license.LicenseLabelName:  parent,
				license.LicenseLabelScope: string(license.LicenseScopeElasticsearch),
				license.LicenseLabelType:  esLicense.Type,
			},
		},
		Data: map[string][]byte{
			license.FileName: licenseBytes,
		},
	}
	// create/update a secret in the cluster's namespace containing the same data
	_, err = reconciler.ReconcileSecret(ctx, c, expected, cluster)
	return err
}

// minVersion returns the minimum Elasticsearch version running in the cluster.
// It checks the version labels on pods and falls back to the spec version if no pods exist.
func minVersion(c k8s.Client, cluster escommon.ElasticsearchCluster) (*version.Version, error) {
	pods, err := sset.GetActualPodsForCluster(c, cluster)
	if err != nil {
		return nil, err
	}
	minVer, err := version.MinInPods(pods, eslabel.VersionLabelName)
	if err != nil {
		return nil, err
	}
	if minVer == nil {
		v, err := version.Parse(cluster.GetVersion())
		if err != nil {
			return nil, err
		}
		minVer = &v
	}
	return minVer, nil
}
