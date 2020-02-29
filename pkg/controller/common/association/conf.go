// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package association

import (
	"context"
	"encoding/json"
	"reflect"
	"unsafe"

	commonv1 "github.com/elastic/cloud-on-k8s/pkg/apis/common/v1"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/events"
	"github.com/elastic/cloud-on-k8s/pkg/controller/common/tracing"
	"github.com/elastic/cloud-on-k8s/pkg/utils/k8s"
	"github.com/pkg/errors"
	"go.elastic.co/apm"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// FetchWithAssociation retrieves an object and extracts its association configuration.
func FetchWithAssociation(
	ctx context.Context,
	client k8s.Client,
	request reconcile.Request,
	associationKind commonv1.AssociationKind,
	obj commonv1.Associator,
) error {
	span, _ := apm.StartSpan(ctx, "fetch_association", tracing.SpanTypeApp)
	defer span.End()

	if err := client.Get(request.NamespacedName, obj); err != nil {
		return err
	}

	assocConf, err := GetAssociationConf(associationKind, obj)
	if err != nil {
		return err
	}

	obj.SetAssociationConf(associationKind, assocConf)
	return nil
}

// GetAssociationConf extracts the association configuration from the given object by reading the annotations.
func GetAssociationConf(associationKind commonv1.AssociationKind, obj runtime.Object) (*commonv1.AssociationConf, error) {
	accessor := meta.NewAccessor()
	annotations, err := accessor.Annotations(obj)
	if err != nil {
		return nil, err
	}

	cfgAnnotation, err := associationKind.ConfigurationAnnotation()
	if err != nil {
		return nil, err
	}

	return extractAssociationConf(cfgAnnotation, annotations)
}

func extractAssociationConf(annotation string, annotations map[string]string) (*commonv1.AssociationConf, error) {
	if len(annotations) == 0 {
		return nil, nil
	}

	var assocConf commonv1.AssociationConf
	serializedConf, exists := annotations[annotation]
	if !exists || serializedConf == "" {
		return nil, nil
	}

	if err := json.Unmarshal(unsafeStringToBytes(serializedConf), &assocConf); err != nil {
		return nil, errors.Wrapf(err, "failed to extract association configuration")
	}

	return &assocConf, nil
}

// RemoveAssociationConf removes the association configuration annotation.
func RemoveAssociationConf(client k8s.Client, associationKind commonv1.AssociationKind, obj runtime.Object) error {
	accessor := meta.NewAccessor()
	annotations, err := accessor.Annotations(obj)
	if err != nil {
		return err
	}

	cfgAnnotation, err := associationKind.ConfigurationAnnotation()
	if err != nil {
		return err
	}

	if len(annotations) == 0 {
		return nil
	}

	if _, exists := annotations[cfgAnnotation]; !exists {
		return nil
	}

	delete(annotations, cfgAnnotation)
	if err := accessor.SetAnnotations(obj, annotations); err != nil {
		return err
	}

	return client.Update(obj)
}

// GetOrUnbindBackendObject
func GetOrUnbindBackendObject(
	ctx context.Context,
	client k8s.Client,
	r record.EventRecorder,
	key types.NamespacedName,
	associationKind commonv1.AssociationKind,
	obj runtime.Object,
) (commonv1.AssociationStatus, error) {
	span, _ := apm.StartSpan(ctx, "get_association_backend", tracing.SpanTypeApp)
	defer span.End()

	err := client.Get(key, obj)
	if err != nil {
		k8s.EmitErrorEvent(r, err, obj, events.EventAssociationError,
			"Failed to find referenced backend %s: %v", key, err)
		if apierrors.IsNotFound(err) {
			// ES is not found, remove any existing backend configuration and retry in a bit.
			if err := RemoveAssociationConf(client, associationKind, obj); err != nil && !apierrors.IsConflict(err) {
				log.Error(err, "Failed to remove Elasticsearch output from APMServer object", "namespace", key.Namespace, "name", key.Name)
				return commonv1.AssociationPending, err
			}
			return commonv1.AssociationPending, nil
		}
		return commonv1.AssociationFailed, err
	}
	return "", nil
}

// UpdateAssociationConf updates the association configuration annotation.
func UpdateAssociationConf(client k8s.Client, associationKind commonv1.AssociationKind, obj runtime.Object, wantConf *commonv1.AssociationConf) error {
	accessor := meta.NewAccessor()
	annotations, err := accessor.Annotations(obj)
	if err != nil {
		return err
	}

	// serialize the config and update the object
	serializedConf, err := json.Marshal(wantConf)
	if err != nil {
		return errors.Wrapf(err, "failed to serialize configuration")
	}

	if annotations == nil {
		annotations = make(map[string]string)
	}

	cfgAnnotation, err := associationKind.ConfigurationAnnotation()
	if err != nil {
		return err
	}

	annotations[cfgAnnotation] = unsafeBytesToString(serializedConf)
	if err := accessor.SetAnnotations(obj, annotations); err != nil {
		return err
	}

	// persist the changes
	return client.Update(obj)
}

// unsafeStringToBytes converts a string to a byte array without making extra allocations.
// since we read potentially large strings from annotations on every reconcile loop, this should help
// reduce the amount of garbage created.
func unsafeStringToBytes(s string) []byte {
	hdr := *(*reflect.StringHeader)(unsafe.Pointer(&s))
	return *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: hdr.Data,
		Len:  hdr.Len,
		Cap:  hdr.Len,
	}))
}

// unsafeBytesToString converts a byte array to string without making extra allocations.
func unsafeBytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
