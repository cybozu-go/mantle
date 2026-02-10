package domain

import (
	"slices"
	"strings"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type CreateOrUpdateMBCCronJobEvent struct {
	CronJob *batchv1.CronJob
}

type DeleteMBCCronJobEvent struct {
	CronJob *batchv1.CronJob
}

type MBCPrimaryReconciler struct {
	overwriteMBCSchedule      string
	managedCephClusterID      string
	cronJobServiceAccountName string
	cronJobImage              string
	Events                    *ReconcilerEvents
}

func NewMBCPrimaryReconciler(
	overwriteMBCSchedule string,
	managedCephClusterID string,
	cronJobServiceAccountName string,
	cronJobImage string,
) *MBCPrimaryReconciler {
	return &MBCPrimaryReconciler{
		overwriteMBCSchedule:      overwriteMBCSchedule,
		managedCephClusterID:      managedCephClusterID,
		cronJobServiceAccountName: cronJobServiceAccountName,
		cronJobImage:              cronJobImage,
		Events:                    NewReconcilerEvents(),
	}
}

type MBCPrimaryReconcilerProvisionInput struct {
	MBC     *mantlev1.MantleBackupConfig
	PVCSC   *storagev1.StorageClass
	CronJob *batchv1.CronJob
}

func (r *MBCPrimaryReconciler) Provision(in *MBCPrimaryReconcilerProvisionInput) error {
	if !r.isResponsibleToStorageClass(in.PVCSC) {
		// This MBC is not for this controller.
		return nil
	}

	if !controllerutil.ContainsFinalizer(in.MBC, controller.MantleBackupConfigFinalizerName) {
		r.attachAnnotAndFinalizer(in.MBC)

		// We should return here because we need to create the CronJob after the
		// finalizer is attached.
		return nil
	}

	r.createOrUpdateCronJob(in.MBC, in.CronJob)

	return nil
}

func (r *MBCPrimaryReconciler) isResponsibleToStorageClass(sc *storagev1.StorageClass) bool {
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

	return clusterID == r.managedCephClusterID
}

func (r *MBCPrimaryReconciler) attachAnnotAndFinalizer(mbc *mantlev1.MantleBackupConfig) {
	if mbc.Annotations == nil {
		mbc.Annotations = make(map[string]string)
	}
	mbc.Annotations[controller.MantleBackupConfigAnnotationManagedClusterID] = r.managedCephClusterID
	controllerutil.AddFinalizer(mbc, controller.MantleBackupConfigFinalizerName)
}

func (r *MBCPrimaryReconciler) createOrUpdateCronJob(mbc *mantlev1.MantleBackupConfig, cronJob *batchv1.CronJob) {
	schedule := mbc.Spec.Schedule
	if r.overwriteMBCSchedule != "" {
		schedule = r.overwriteMBCSchedule
	}

	if cronJob == nil {
		cronJob = &batchv1.CronJob{}
	}

	cronJob.Name = controller.GetMBCronJobName(mbc)
	cronJob.Namespace = "FIXME"

	cronJob.Spec.Schedule = schedule
	cronJob.Spec.Suspend = ptr.To(mbc.Spec.Suspend)
	cronJob.Spec.ConcurrencyPolicy = batchv1.ForbidConcurrent
	var startingDeadlineSeconds int64 = 3600
	cronJob.Spec.StartingDeadlineSeconds = &startingDeadlineSeconds
	var backoffLimit int32 = 10
	cronJob.Spec.JobTemplate.Spec.BackoffLimit = &backoffLimit

	podSpec := &cronJob.Spec.JobTemplate.Spec.Template.Spec
	podSpec.ServiceAccountName = r.cronJobServiceAccountName
	podSpec.RestartPolicy = corev1.RestartPolicyOnFailure

	if len(podSpec.Containers) == 0 {
		podSpec.Containers = append(podSpec.Containers, corev1.Container{})
	}
	container := &podSpec.Containers[0]
	container.Name = "backup"
	container.Image = r.cronJobImage
	container.Command = []string{
		"/manager",
		"backup",
		"--name", mbc.GetName(),
		"--namespace", mbc.GetNamespace(),
	}
	container.ImagePullPolicy = corev1.PullIfNotPresent

	envName := "JOB_NAME"
	envIndex := slices.IndexFunc(container.Env, func(e corev1.EnvVar) bool {
		return e.Name == envName
	})
	if envIndex == -1 {
		container.Env = append(container.Env, corev1.EnvVar{Name: envName})
		envIndex = len(container.Env) - 1
	}
	env := &container.Env[envIndex]
	if env.ValueFrom == nil {
		env.ValueFrom = &corev1.EnvVarSource{}
	}
	if env.ValueFrom.FieldRef == nil {
		env.ValueFrom.FieldRef = &corev1.ObjectFieldSelector{}
	}
	env.ValueFrom.FieldRef.FieldPath = "metadata.labels['batch.kubernetes.io/job-name']"

	r.Events.Append(&CreateOrUpdateMBCCronJobEvent{CronJob: cronJob})
}

type MBCPrimaryReconcilerFinalizeInput struct {
	MBC     *mantlev1.MantleBackupConfig
	CronJob *batchv1.CronJob
}

func (r *MBCPrimaryReconciler) Finalize(in *MBCPrimaryReconcilerFinalizeInput) error {
	if !controllerutil.ContainsFinalizer(in.MBC, controller.MantleBackupConfigFinalizerName) {
		return nil
	}
	if in.MBC.Annotations[controller.MantleBackupConfigAnnotationManagedClusterID] != r.managedCephClusterID {
		return nil
	}

	if in.CronJob != nil { // CronJob still exists
		r.Events.Append(&DeleteMBCCronJobEvent{CronJob: in.CronJob})

		// We should return here because we need to wait until the CronJob is deleted.
		return nil
	}

	controllerutil.RemoveFinalizer(in.MBC, controller.MantleBackupConfigFinalizerName)

	return nil
}
