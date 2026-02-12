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
)

type ReconcileMBCInPrimary struct {
	reconciler       *domain.MBCPrimaryReconciler
	k8sClient        KubernetesClient
	cronjobNamespace string
}

func NewReconcileMBCInPrimary(
	reconciler *domain.MBCPrimaryReconciler,
	k8sClient KubernetesClient,
	cronjobNamespace string,
) *ReconcileMBCInPrimary {
	return &ReconcileMBCInPrimary{
		reconciler:       reconciler,
		k8sClient:        k8sClient,
		cronjobNamespace: cronjobNamespace,
	}
}

func (r *ReconcileMBCInPrimary) runProvision(ctx context.Context, mbc *mantlev1.MantleBackupConfig, cronJob *batchv1.CronJob) error {
	// Get necessary resources
	pvc, err := getResource[corev1.PersistentVolumeClaim](ctx, r.k8sClient, mbc.Spec.PVC, mbc.GetNamespace())
	if err != nil {
		return err
	}
	var sc *storagev1.StorageClass
	if pvc.Spec.StorageClassName != nil {
		sc, err = getResource[storagev1.StorageClass](ctx, r.k8sClient, *pvc.Spec.StorageClassName, "")
		if err != nil {
			return err
		}
	}

	// Run provision logic
	origMBC := mbc.DeepCopy()
	_ = r.reconciler.Events.TakeAll()
	if err := r.reconciler.Provision(&domain.MBCPrimaryReconcilerProvisionInput{
		MBC:     mbc,
		PVCSC:   sc,
		CronJob: cronJob,
	}); err != nil {
		return fmt.Errorf("failed to provision MBC %s/%s: %w", mbc.GetNamespace(), mbc.GetName(), err)
	}

	// Persist changes
	if !equality.Semantic.DeepEqual(origMBC, mbc) {
		if err := r.k8sClient.Update(ctx, mbc); err != nil {
			return fmt.Errorf("failed to update MBC %s/%s: %w", mbc.GetNamespace(), mbc.GetName(), err)
		}
	}

	return r.k8sClient.DispatchReconcilerEvents(ctx, r.reconciler.Events.TakeAll())
}

func (r *ReconcileMBCInPrimary) runFinalize(ctx context.Context, mbc *mantlev1.MantleBackupConfig, cronJob *batchv1.CronJob) error {
	// Run finalize logic
	origMBC := mbc.DeepCopy()
	_ = r.reconciler.Events.TakeAll()
	if err := r.reconciler.Finalize(&domain.MBCPrimaryReconcilerFinalizeInput{
		MBC:     mbc,
		CronJob: cronJob,
	}); err != nil {
		return fmt.Errorf("failed to finalize MBC %s/%s: %w", mbc.GetNamespace(), mbc.GetName(), err)
	}

	// Persist changes
	if !equality.Semantic.DeepEqual(origMBC, mbc) {
		// Assume that origMBC.Spec is equal to mbc.Spec.
		if err := r.k8sClient.Update(ctx, mbc); err != nil {
			return fmt.Errorf("failed to update MBC %s/%s: %w", mbc.GetNamespace(), mbc.GetName(), err)
		}
	}

	return r.k8sClient.DispatchReconcilerEvents(ctx, r.reconciler.Events.TakeAll())
}

func (r *ReconcileMBCInPrimary) Run(
	ctx context.Context,
	mbcName string,
	mbcNamespace string,
) error {
	mbc, err := getResource[mantlev1.MantleBackupConfig](ctx, r.k8sClient, mbcName, mbcNamespace)
	if err != nil {
		return err
	}
	cronJob, err := getResource[batchv1.CronJob](ctx, r.k8sClient, domain.GetMBCCronJobName(mbc), r.cronjobNamespace)
	if aerrors.IsNotFound(err) {
		cronJob = nil
	} else if err != nil {
		return err
	}

	if mbc.DeletionTimestamp.IsZero() {
		return r.runProvision(ctx, mbc, cronJob)
	} else {
		return r.runFinalize(ctx, mbc, cronJob)
	}
}
