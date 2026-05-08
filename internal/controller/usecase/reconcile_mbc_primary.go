// Package usecase provides use case implementations for the mantle controller.
// It orchestrates domain logic and handles the business workflows for
// MantleBackupConfig and related resources.
package usecase

import (
	"context"
	"fmt"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	aerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

// ReconcileMBCPrimary is a use case that handles the reconciliation of
// MantleBackupConfig resources on the primary cluster.
type ReconcileMBCPrimary struct {
	reconciler       *domain.MBCPrimaryReconciler
	k8sClient        KubernetesClient
	cronJobNamespace string
}

// NewReconcileMBCPrimary creates a new ReconcileMBCPrimary instance.
func NewReconcileMBCPrimary(
	managedCephClusterID string,
	k8sClient KubernetesClient,
	cronJobNamespace string,
	overwriteMBCSchedule string,
	serviceAccountName string,
	image string,
) *ReconcileMBCPrimary {
	return &ReconcileMBCPrimary{
		reconciler: domain.NewMBCPrimaryReconciler(
			managedCephClusterID,
			overwriteMBCSchedule,
			cronJobNamespace,
			serviceAccountName,
			image,
		),
		k8sClient:        k8sClient,
		cronJobNamespace: cronJobNamespace,
	}
}

// Run executes the reconciliation logic for a MantleBackupConfig resource. It
// basically does the following steps:
//
// 1. Fetch MantleBackupConfig and other related resources.
// 2. Run Provision or Finalize based on the deletion timestamp of MantleBackupConfig.
// 3. Update the status of MantleBackupConfig and related resources accordingly.
func (r *ReconcileMBCPrimary) Run(ctx context.Context, mbcNamespacedName types.NamespacedName) error {
	mbc, err := getResource[mantlev1.MantleBackupConfig](
		ctx, r.k8sClient, mbcNamespacedName.Name, mbcNamespacedName.Namespace)
	if err != nil {
		if aerrors.IsNotFound(err) {
			return nil
		}

		return err
	}

	origMBC := mbc.DeepCopy()
	_ = r.reconciler.Operations.TakeAll()

	if mbc.DeletionTimestamp.IsZero() {
		err = r.runProvision(ctx, mbc)
	} else {
		err = r.runFinalize(ctx, mbc)
	}

	if err != nil {
		return err
	}

	if !equality.Semantic.DeepEqual(origMBC, mbc) {
		err := r.k8sClient.Update(ctx, mbc)
		if err != nil {
			return fmt.Errorf("failed to update MBC %s/%s: %w", mbc.GetNamespace(), mbc.GetName(), err)
		}
	}

	err = r.k8sClient.ApplyReconcilerOperations(ctx, r.reconciler.Operations.TakeAll())
	if err != nil {
		return fmt.Errorf("failed to apply reconciler operations: %w", err)
	}

	return nil
}

func (r *ReconcileMBCPrimary) runProvision(ctx context.Context, mbc *mantlev1.MantleBackupConfig) error {
	storageClass, err := r.getPVCStorageClass(ctx, mbc)
	if err != nil {
		return fmt.Errorf("failed to get PVC StorageClass: %w", err)
	}

	err = r.reconciler.Provision(mbc, storageClass)
	if err != nil {
		return fmt.Errorf("provision failed: %w", err)
	}

	return nil
}

func (r *ReconcileMBCPrimary) getPVCStorageClass(
	ctx context.Context, mbc *mantlev1.MantleBackupConfig,
) (*storagev1.StorageClass, error) {
	pvc, err := getResource[corev1.PersistentVolumeClaim](
		ctx, r.k8sClient, mbc.Spec.PVC, mbc.Namespace,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get PVC: %s: %s: %w", mbc.Namespace, mbc.Spec.PVC, err)
	}

	if pvc.Spec.StorageClassName == nil {
		return nil, nil //nolint:nilnil // nil StorageClass signals "not responsible" to domain.Provision
	}

	storageClass, err := getResource[storagev1.StorageClass](
		ctx, r.k8sClient, *pvc.Spec.StorageClassName, "",
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get StorageClass: %s: %w", *pvc.Spec.StorageClassName, err,
		)
	}

	return storageClass, nil
}

func (r *ReconcileMBCPrimary) runFinalize(
	ctx context.Context,
	mbc *mantlev1.MantleBackupConfig,
) error {
	cronJob, err := getResource[batchv1.CronJob](ctx, r.k8sClient, domain.GetMBCCronJobName(mbc), r.cronJobNamespace)
	if err != nil && !aerrors.IsNotFound(err) {
		return fmt.Errorf("failed to get CronJob: %w", err)
	}

	err = r.reconciler.Finalize(mbc, cronJob)
	if err != nil {
		return fmt.Errorf("finalize failed: %w", err)
	}

	return nil
}
