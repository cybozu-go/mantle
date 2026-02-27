// Package domain provides domain logic for the mantle controller.
// It contains reconciler implementations that handle the core business logic
// for MantleBackupConfig and related resources.
package domain

import (
	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/metrics"
	"github.com/prometheus/client_golang/prometheus"
	batchv1 "k8s.io/api/batch/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// MantleBackupConfig related constants.
const (
	// MantleBackupConfigFinalizerName is the finalizer name for MantleBackupConfig resources.
	MantleBackupConfigFinalizerName = "mantlebackupconfig.mantle.cybozu.io/finalizer"
	// MantleBackupConfigAnnotationManagedClusterID is the annotation key for the managed cluster ID.
	MantleBackupConfigAnnotationManagedClusterID = "mantlebackupconfig.mantle.cybozu.io/managed-cluster-id"
	// MantleBackupConfigCronJobNamePrefix is the prefix for CronJob names created for MantleBackupConfig.
	MantleBackupConfigCronJobNamePrefix = "mbc-"
)

// DeleteMBCCronJobOperation represents an operation to delete a CronJob associated with a MantleBackupConfig.
type DeleteMBCCronJobOperation struct {
	CronJob *batchv1.CronJob
}

// GetMBCCronJobName returns the CronJob name for the given MantleBackupConfig.
func GetMBCCronJobName(mbc *mantlev1.MantleBackupConfig) string {
	return MantleBackupConfigCronJobNamePrefix + string(mbc.UID)
}

// MBCPrimaryReconciler is a reconciler for MantleBackupConfig resources
// running on the primary cluster.
type MBCPrimaryReconciler struct {
	managedCephClusterID string
	Operations           *ReconcilerOperations
}

// NewMBCPrimaryReconciler creates a new MBCPrimaryReconciler instance.
func NewMBCPrimaryReconciler(managedCephClusterID string) *MBCPrimaryReconciler {
	return &MBCPrimaryReconciler{
		managedCephClusterID: managedCephClusterID,
		Operations:           NewReconcilerOperations(),
	}
}

// Provision handles the provisioning logic for MantleBackupConfig resources.
func (r *MBCPrimaryReconciler) Provision() error {
	return nil
}

// Finalize handles the finalization logic for MantleBackupConfig resources.
func (r *MBCPrimaryReconciler) Finalize(mbc *mantlev1.MantleBackupConfig, cronJob *batchv1.CronJob) error {
	if !r.hasManagedCephClusterIDAnnotation(mbc) {
		// We don't have to finalize this MBC.
		return nil
	}

	_ = metrics.BackupConfigInfo.Delete(prometheus.Labels{
		"persistentvolumeclaim": mbc.Spec.PVC,
		"resource_namespace":    mbc.Namespace,
		"mantlebackupconfig":    mbc.Name,
	})

	if cronJob != nil {
		return r.deleteCronJob(cronJob)
	}

	return r.removeFinalizer(mbc)
}

func (r *MBCPrimaryReconciler) hasManagedCephClusterIDAnnotation(mbc *mantlev1.MantleBackupConfig) bool {
	return mbc.Annotations[MantleBackupConfigAnnotationManagedClusterID] == r.managedCephClusterID
}

func (r *MBCPrimaryReconciler) deleteCronJob(cronJob *batchv1.CronJob) error {
	r.Operations.Append(&DeleteMBCCronJobOperation{CronJob: cronJob})

	return nil
}

func (r *MBCPrimaryReconciler) removeFinalizer(mbc *mantlev1.MantleBackupConfig) error {
	controllerutil.RemoveFinalizer(mbc, MantleBackupConfigFinalizerName)

	return nil
}
