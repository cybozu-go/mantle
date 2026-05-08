// Package domain provides domain logic for the mantle controller.
// It contains reconciler implementations that handle the core business logic
// for MantleBackupConfig and related resources.
package domain

import (
	"strings"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/metrics"
	"github.com/prometheus/client_golang/prometheus"
	batchv1 "k8s.io/api/batch/v1"
	storagev1 "k8s.io/api/storage/v1"
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

func (*DeleteMBCCronJobOperation) isOperation() {}

// CreateOrUpdateMBCCronJobOperation represents an operation to create or update a CronJob for a MantleBackupConfig.
type CreateOrUpdateMBCCronJobOperation struct {
	CronJobName        string
	CronJobNamespace   string
	Schedule           string
	Suspend            bool
	ServiceAccountName string
	Image              string
	MBCName            string
	MBCNamespace       string
}

func (*CreateOrUpdateMBCCronJobOperation) isOperation() {}

// GetMBCCronJobName returns the CronJob name for the given MantleBackupConfig.
func GetMBCCronJobName(mbc *mantlev1.MantleBackupConfig) string {
	return MantleBackupConfigCronJobNamePrefix + string(mbc.UID)
}

// MBCPrimaryReconciler is a reconciler for MantleBackupConfig resources
// running on the primary cluster.
type MBCPrimaryReconciler struct {
	managedCephClusterID string
	overwriteMBCSchedule string
	cronJobNamespace     string
	serviceAccountName   string
	image                string
	Operations           *ReconcilerOperations
}

// NewMBCPrimaryReconciler creates a new MBCPrimaryReconciler instance.
func NewMBCPrimaryReconciler(
	managedCephClusterID string,
	overwriteMBCSchedule string,
	cronJobNamespace string,
	serviceAccountName string,
	image string,
) *MBCPrimaryReconciler {
	return &MBCPrimaryReconciler{
		managedCephClusterID: managedCephClusterID,
		overwriteMBCSchedule: overwriteMBCSchedule,
		cronJobNamespace:     cronJobNamespace,
		serviceAccountName:   serviceAccountName,
		image:                image,
		Operations:           NewReconcilerOperations(),
	}
}

// Provision handles the provisioning logic for MantleBackupConfig resources.
// This function should be written in the following way:
//
//	if condition1 {
//	  return process1()
//	}
//	if condition2 {
//	  return process2()
//	}
//	...
//
// processN() should do the actual work and make conditionN false eventually.
// Comments should be left where this rule is not followed to explain why.
func (r *MBCPrimaryReconciler) Provision(mbc *mantlev1.MantleBackupConfig, sc *storagev1.StorageClass) error {
	if !r.isResponsibleToStorageClass(sc) {
		return nil
	}

	if !controllerutil.ContainsFinalizer(mbc, MantleBackupConfigFinalizerName) {
		r.addFinalizerAndAnnotation(mbc)

		return nil
	}

	metrics.BackupConfigInfo.With(prometheus.Labels{
		"persistentvolumeclaim": mbc.Spec.PVC,
		"resource_namespace":    mbc.Namespace,
		"mantlebackupconfig":    mbc.Name,
	}).Set(1)

	r.createOrUpdateCronJob(mbc)

	return nil
}

// Finalize handles the finalization logic for MantleBackupConfig resources.
// This function should be written in the following way:
//
//	if condition1 {
//	  return process1()
//	}
//	if condition2 {
//	  return process2()
//	}
//	...
//
// processN() should do the actual work and make conditionN false eventually.
// Comments should be left where this rule is not followed to explain why.
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
		// Delete the CronJob first and return. The finalizer will be removed
		// in the next reconcile cycle after the CronJob deletion is confirmed
		// (i.e., cronJob becomes nil).
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

func (r *MBCPrimaryReconciler) addFinalizerAndAnnotation(mbc *mantlev1.MantleBackupConfig) {
	if mbc.Annotations == nil {
		mbc.Annotations = make(map[string]string)
	}

	mbc.Annotations[MantleBackupConfigAnnotationManagedClusterID] = r.managedCephClusterID
	controllerutil.AddFinalizer(mbc, MantleBackupConfigFinalizerName)
}

func (r *MBCPrimaryReconciler) isResponsibleToStorageClass(storageClass *storagev1.StorageClass) bool {
	if storageClass == nil {
		return false
	}

	if !strings.HasSuffix(storageClass.Provisioner, ".rbd.csi.ceph.com") {
		return false
	}

	return storageClass.Parameters["clusterID"] == r.managedCephClusterID
}

func (r *MBCPrimaryReconciler) createOrUpdateCronJob(mbc *mantlev1.MantleBackupConfig) {
	schedule := mbc.Spec.Schedule
	if r.overwriteMBCSchedule != "" {
		schedule = r.overwriteMBCSchedule
	}

	r.Operations.Append(&CreateOrUpdateMBCCronJobOperation{
		CronJobName:        GetMBCCronJobName(mbc),
		CronJobNamespace:   r.cronJobNamespace,
		Schedule:           schedule,
		Suspend:            mbc.Spec.Suspend,
		ServiceAccountName: r.serviceAccountName,
		Image:              r.image,
		MBCName:            mbc.Name,
		MBCNamespace:       mbc.Namespace,
	})
}
