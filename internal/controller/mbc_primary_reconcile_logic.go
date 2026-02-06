package controller

import (
	"strings"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type Event any // FIXME

type CreateOrUpdateMantleBackupConfigCronJobEvent struct {
	mbcName          string
	mbcNamespace     string
	cronJobName      string
	cronJobNamespace string
	schedule         string
	suspend          bool
}

type UpdateMantleBackupConfigEvent struct {
	MBC *mantlev1.MantleBackupConfig
}

type DeleteMBCCronJobEvent struct {
	name          string
	namespace     string
	preconditions *metav1.Preconditions
}

type DeleteMBCFinalizerEvent struct {
	mbcName      string
	mbcNamespace string
}

type RequeueEvent struct{}

type MantleBackupConfigPrimaryReconcileLogic struct {
	runningNamespace     string
	managedCephClusterID string
	overwriteMBCSchedule string
	events               []Event
}

func NewMantleBackupConfigPrimaryReconcileLogic(managedCephClusterID, overwriteMBCSchedule string) *MantleBackupConfigPrimaryReconcileLogic {
	return &MantleBackupConfigPrimaryReconcileLogic{
		managedCephClusterID: managedCephClusterID,
		overwriteMBCSchedule: overwriteMBCSchedule,
		events:               []Event{},
	}
}

func (logic *MantleBackupConfigPrimaryReconcileLogic) SetRunningNamespace(namespace string) {
	logic.runningNamespace = namespace
}

func (logic *MantleBackupConfigPrimaryReconcileLogic) Reconcile(
	mbc *mantlev1.MantleBackupConfig,
	mbcPVC *corev1.PersistentVolumeClaim,
	mbcPVCSC *storagev1.StorageClass,
	mbcCronJob *batchv1.CronJob,
) (ctrl.Result, error) {
	if !mbc.DeletionTimestamp.IsZero() {
		logic.finalize(mbc, mbcCronJob)

		return ctrl.Result{}, nil
	}
	if !logic.isResponsibleToStorageClass(mbcPVCSC) {
		return ctrl.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(mbc, MantleBackupConfigFinalizerName) {
		logic.attachFinalizer(mbc)

		return ctrl.Result{}, nil
	}

	logic.createOrUpdateCronJob(mbc)

	return ctrl.Result{}, nil
}

func (logic *MantleBackupConfigPrimaryReconcileLogic) finalize(mbc *mantlev1.MantleBackupConfig, cronJob *batchv1.CronJob) {
	if !controllerutil.ContainsFinalizer(mbc, MantleBackupConfigFinalizerName) {
		return
	}
	if mbc.Annotations[MantleBackupConfigAnnotationManagedClusterID] != logic.managedCephClusterID {
		return
	}

	if cronJob != nil { // CronJob still exists
		logic.events = append(logic.events, DeleteMBCCronJobEvent{
			name:      cronJob.Name,
			namespace: cronJob.Namespace,
			preconditions: &metav1.Preconditions{
				UID:             ptr.To(cronJob.GetUID()),
				ResourceVersion: ptr.To(cronJob.GetResourceVersion()),
			},
		})

		return
	}

	logic.events = append(logic.events, DeleteMBCFinalizerEvent{
		mbcName:      mbc.Name,
		mbcNamespace: mbc.Namespace,
	})
}

func (logic *MantleBackupConfigPrimaryReconcileLogic) isResponsibleToStorageClass(sc *storagev1.StorageClass) bool {
	if sc == nil { // PVC doesn't have StorageClass
		return false
	}
	if !strings.HasSuffix(sc.Provisioner, ".rbd.csi.ceph.com") {
		return false
	}
	clusterID, ok := sc.Parameters["clusterID"]
	if !ok {
		return false
	}

	return clusterID == logic.managedCephClusterID
}

func (logic *MantleBackupConfigPrimaryReconcileLogic) attachFinalizer(mbc *mantlev1.MantleBackupConfig) {
	if mbc.Annotations == nil {
		mbc.Annotations = make(map[string]string)
	}
	mbc.Annotations[MantleBackupConfigAnnotationManagedClusterID] = logic.managedCephClusterID
	controllerutil.AddFinalizer(mbc, MantleBackupConfigFinalizerName)
	logic.events = append(logic.events, UpdateMantleBackupConfigEvent{MBC: mbc})
}

func (logic *MantleBackupConfigPrimaryReconcileLogic) createOrUpdateCronJob(mbc *mantlev1.MantleBackupConfig) {
	schedule := mbc.Spec.Schedule
	if logic.overwriteMBCSchedule != "" {
		schedule = logic.overwriteMBCSchedule
	}
	logic.events = append(logic.events, CreateOrUpdateMantleBackupConfigCronJobEvent{
		mbcName:          mbc.Name,
		mbcNamespace:     mbc.Namespace,
		cronJobName:      getMBCCronJobName(mbc),
		cronJobNamespace: logic.runningNamespace,
		schedule:         schedule,
		suspend:          mbc.Spec.Suspend,
	})
}
