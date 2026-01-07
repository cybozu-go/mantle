package v1

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const annotRemoteUID = "mantle.cybozu.io/remote-uid"

// SetupVolumeAttachmentWebhookWithManager registers the webhook for VolumeAttachment in the manager.
func SetupVolumeAttachmentWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).For(&storagev1.VolumeAttachment{}).
		WithValidator(&VolumeAttachmentCustomValidator{
			client: mgr.GetClient(),
		}).
		Complete()
}

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-storage-k8s-io-v1-volumeattachment,mutating=false,failurePolicy=fail,sideEffects=None,groups=storage.k8s.io,resources=volumeattachments,verbs=create,versions=v1,name=volumeattachment.mantle.cybozu.io,admissionReviewVersions=v1

// VolumeAttachmentCustomValidator struct is responsible for validating the VolumeAttachment resource
// when it is created.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type VolumeAttachmentCustomValidator struct {
	client client.Client
}

var _ webhook.CustomValidator = &VolumeAttachmentCustomValidator{}

//+kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type VolumeAttachment.
func (v *VolumeAttachmentCustomValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	logger := log.FromContext(ctx)
	va, ok := obj.(*storagev1.VolumeAttachment)
	if !ok {
		return nil, fmt.Errorf("expected a VolumeAttachment object but got %T", obj)
	}

	pvName := va.Spec.Source.PersistentVolumeName
	var pv corev1.PersistentVolume
	if err := v.client.Get(ctx, types.NamespacedName{Name: *pvName}, &pv); err != nil {
		logger.Error(err, "failed to get PV", "name", *pvName)

		return nil, err
	}

	claimRef := pv.Spec.ClaimRef
	var pvc corev1.PersistentVolumeClaim
	if err := v.client.Get(ctx, types.NamespacedName{Namespace: claimRef.Namespace, Name: claimRef.Name}, &pvc); err != nil {
		logger.Error(err, "failed to get PVC", "namespace", claimRef.Namespace, "name", claimRef.Name)

		return nil, err
	}
	if _, ok := pvc.Annotations[annotRemoteUID]; ok {
		return nil, errors.New("should not attach a volume made by mantle")
	}

	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type VolumeAttachment.
func (v *VolumeAttachmentCustomValidator) ValidateUpdate(_ context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type VolumeAttachment.
func (v *VolumeAttachmentCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return nil, nil
}
