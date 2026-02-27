// Package usecase provides use case implementations for the mantle controller.
// It orchestrates domain logic and handles the business workflows for
// MantleBackupConfig and related resources.
package usecase

import (
	"context"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/pkg/errors"
	batchv1 "k8s.io/api/batch/v1"
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
	managedCephCluster string,
	k8sClient KubernetesClient,
	cronJobNamespace string,
) *ReconcileMBCPrimary {
	return &ReconcileMBCPrimary{
		reconciler:       domain.NewMBCPrimaryReconciler(managedCephCluster),
		k8sClient:        k8sClient,
		cronJobNamespace: cronJobNamespace,
	}
}

// Run executes the reconciliation logic for a MantleBackupConfig resource.
// It determines whether to provision or finalize based on the deletion timestamp.
func (r *ReconcileMBCPrimary) Run(ctx context.Context, mbcNamespacedName types.NamespacedName) error {
	mbc, err := getResource[mantlev1.MantleBackupConfig](
		ctx, r.k8sClient, mbcNamespacedName.Name, mbcNamespacedName.Namespace)
	if err != nil {
		return err
	}

	cronJob, err := getResource[batchv1.CronJob](ctx, r.k8sClient, domain.GetMBCCronJobName(mbc), r.cronJobNamespace)
	if err != nil && !aerrors.IsNotFound(err) {
		return err
	}

	if mbc.DeletionTimestamp.IsZero() {
		return r.runProvision()
	}

	return r.runFinalize(mbc, cronJob)
}

func (r *ReconcileMBCPrimary) runProvision() error {
	return errors.Wrap(r.reconciler.Provision(), "provision failed")
}

func (r *ReconcileMBCPrimary) runFinalize(mbc *mantlev1.MantleBackupConfig, cronJob *batchv1.CronJob) error {
	return errors.Wrap(r.reconciler.Finalize(mbc, cronJob), "finalize failed")
}
