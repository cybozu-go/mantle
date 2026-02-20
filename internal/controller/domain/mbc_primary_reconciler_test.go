package domain_test

import (
	"testing"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type optionMBC func(*mantlev1.MantleBackupConfig)

func mbcWithAnnotAndFinalizer(sc *storagev1.StorageClass) optionMBC {
	return func(mbc *mantlev1.MantleBackupConfig) {
		mbc.Finalizers = append(mbc.Finalizers, domain.MantleBackupConfigFinalizerName)
		mbc.Annotations[domain.MantleBackupConfigAnnotationManagedClusterID] = sc.Parameters["clusterID"]
	}
}

func mbcWithSuspend(suspend bool) optionMBC {
	return func(mbc *mantlev1.MantleBackupConfig) {
		mbc.Spec.Suspend = suspend
	}
}

func newMBC(opts ...optionMBC) *mantlev1.MantleBackupConfig {
	mbc := &mantlev1.MantleBackupConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "mbc-name",
			Namespace:   "mbc-namespace",
			UID:         "mbc-uid",
			Annotations: map[string]string{},
		},
		Spec: mantlev1.MantleBackupConfigSpec{
			PVC:      "pvc-name",
			Schedule: "0 0 * * *",
			Expire:   "2w",
			Suspend:  false,
		},
	}
	for _, opt := range opts {
		opt(mbc)
	}

	return mbc
}

type optionSC func(*storagev1.StorageClass)

func scWithClusterID(clusterID string) optionSC {
	return func(sc *storagev1.StorageClass) {
		sc.Parameters["clusterID"] = clusterID
	}
}

func newSC(opts ...optionSC) *storagev1.StorageClass {
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "sc-name",
		},
		Provisioner: "hoge.rbd.csi.ceph.com",
		Parameters: map[string]string{
			"clusterID": "ceph-cluster-id",
		},
	}
	for _, opt := range opts {
		opt(sc)
	}

	return sc
}

type optionReconciler func(*domain.NewMBCPrimaryReconcilerInput)

func reconcilerWithCronJobInfo(serviceAccount, image, namespace string) optionReconciler {
	return func(in *domain.NewMBCPrimaryReconcilerInput) {
		in.CronJobImage = image
		in.CronJobNamespace = namespace
		in.CronJobServiceAccountName = serviceAccount
	}
}

func reconcilerWithOverwriteSchedule(schedule string) optionReconciler {
	return func(in *domain.NewMBCPrimaryReconcilerInput) {
		in.OverwriteMBCSchedule = schedule
	}
}

func newReconciler(sc *storagev1.StorageClass, opts ...optionReconciler) *domain.MBCPrimaryReconciler {
	in := &domain.NewMBCPrimaryReconcilerInput{
		OverwriteMBCSchedule:      "",
		ManagedCephClusterID:      sc.Parameters["clusterID"],
		CronJobServiceAccountName: "cron-job-service-account",
		CronJobImage:              "cron-job-image",
		CronJobNamespace:          "cron-job-namespace",
	}
	for _, opt := range opts {
		opt(in)
	}

	return domain.NewMBCPrimaryReconciler(in)
}

func TestMBCPrimaryReconciler_Provision_NotResponsibleStorageClass(t *testing.T) {
	// Arrange
	sc1 := newSC(scWithClusterID("ceph-cluster-id"))
	sc2 := newSC(scWithClusterID("different-ceph-cluster-id"))
	reconciler := newReconciler(sc1)
	mbc := newMBC()
	origMBC := mbc.DeepCopy()

	// Act
	err := reconciler.Provision(&domain.MBCPrimaryReconcilerProvisionInput{
		MBC:     mbc,
		PVCSC:   sc2,
		CronJob: nil,
	})

	// Assert
	require.NoError(t, err)
	require.Equal(t, origMBC, mbc)
	require.Empty(t, reconciler.Operations.TakeAll())
}

func TestMBCPrimaryReconciler_Provision_AttachAnnotAndFinalizer(t *testing.T) {
	// Arrange
	mbc := newMBC()
	sc := newSC()
	reconciler := newReconciler(sc)

	// Act
	err := reconciler.Provision(&domain.MBCPrimaryReconcilerProvisionInput{
		MBC:     mbc,
		PVCSC:   sc,
		CronJob: nil,
	})

	// Assert
	require.NoError(t, err)
	require.Empty(t, reconciler.Operations.TakeAll())
	require.True(t, controllerutil.ContainsFinalizer(mbc, domain.MantleBackupConfigFinalizerName))
	require.Equal(t, sc.Parameters["clusterID"], mbc.Annotations[domain.MantleBackupConfigAnnotationManagedClusterID])
}

func TestMBCPrimaryReconciler_Provision_CreateCronJob(t *testing.T) {
	// Arrange
	sc := newSC()
	mbc := newMBC(mbcWithAnnotAndFinalizer(sc))
	origMBC := mbc.DeepCopy()
	cronJobServiceAccount := "cron-job-service-account"
	cronJobImage := "cron-job-image"
	cronJobNamespace := "cron-job-namespace"
	reconciler := newReconciler(sc, reconcilerWithCronJobInfo(cronJobServiceAccount, cronJobImage, cronJobNamespace))

	// Act
	err := reconciler.Provision(&domain.MBCPrimaryReconcilerProvisionInput{
		MBC:     mbc,
		PVCSC:   sc,
		CronJob: nil,
	})

	// Assert
	require.NoError(t, err)
	require.Equal(t, origMBC, mbc)
	operations := reconciler.Operations.TakeAll()
	require.Len(t, operations, 1)
	operation, ok := operations[0].(*domain.CreateOrUpdateMBCCronJobOperation)
	require.True(t, ok)
	cronJob := operation.CronJob
	assert.True(t, cronJob.CreationTimestamp.IsZero())
	assert.Equal(t, domain.GetMBCCronJobName(mbc), cronJob.Name)
	assert.Equal(t, cronJobNamespace, cronJob.Namespace)
	assert.False(t, *cronJob.Spec.Suspend)
	pod := cronJob.Spec.JobTemplate.Spec.Template.Spec
	assert.Equal(t, cronJobServiceAccount, pod.ServiceAccountName)
	container := pod.Containers[0]
	assert.Equal(t, cronJobImage, container.Image)
}

func TestMBCPrimaryReconciler_Provision_UpdateCronJob(t *testing.T) {
	// Arrange
	sc := newSC()
	mbc := newMBC(mbcWithAnnotAndFinalizer(sc))
	origMBC := mbc.DeepCopy()
	reconciler := newReconciler(sc)
	err := reconciler.Provision(&domain.MBCPrimaryReconcilerProvisionInput{
		MBC:     mbc,
		PVCSC:   sc,
		CronJob: nil,
	})
	require.NoError(t, err)
	oldCronJob := reconciler.Operations.TakeAll()[0].(*domain.CreateOrUpdateMBCCronJobOperation).CronJob
	require.NotNil(t, oldCronJob)

	// Act
	oldCronJob.CreationTimestamp = metav1.Now()
	err = reconciler.Provision(&domain.MBCPrimaryReconcilerProvisionInput{
		MBC:     mbc,
		PVCSC:   sc,
		CronJob: oldCronJob,
	})

	// Assert
	require.NoError(t, err)
	require.Equal(t, origMBC, mbc)
	operations := reconciler.Operations.TakeAll()
	require.Len(t, operations, 1)
	operation, ok := operations[0].(*domain.CreateOrUpdateMBCCronJobOperation)
	require.True(t, ok)
	cronJob := operation.CronJob
	assert.False(t, cronJob.CreationTimestamp.IsZero())
	cronJob.CreationTimestamp = oldCronJob.CreationTimestamp
	assert.Equal(t, oldCronJob, cronJob)
}

func TestMBCPrimaryReconciler_Finalize_NoProvision(t *testing.T) {
	// Arrange
	sc := newSC()
	mbc := newMBC()
	origMBC := mbc.DeepCopy()
	reconciler := newReconciler(sc)

	// Act
	err := reconciler.Finalize(&domain.MBCPrimaryReconcilerFinalizeInput{
		MBC:     mbc,
		CronJob: nil,
	})

	// Assert
	require.NoError(t, err)
	require.Equal(t, origMBC, mbc)
	require.Empty(t, reconciler.Operations.TakeAll())
}

func TestMBCPrimaryReconciler_Finalize_RemoveFinalizer(t *testing.T) {
	// Arrange
	sc := newSC()
	mbc := newMBC(mbcWithAnnotAndFinalizer(sc))
	reconciler := newReconciler(sc)

	// Act
	err := reconciler.Finalize(&domain.MBCPrimaryReconcilerFinalizeInput{
		MBC:     mbc,
		CronJob: nil,
	})

	// Assert
	require.NoError(t, err)
	require.Empty(t, mbc.Finalizers)
	require.Empty(t, reconciler.Operations.TakeAll())
}

func TestMBCPrimaryReconciler_Finalize_RemoveCronJob(t *testing.T) {
	// Arrange
	sc := newSC()
	mbc := newMBC(mbcWithAnnotAndFinalizer(sc))
	origMBC := mbc.DeepCopy()
	reconciler := newReconciler(sc)
	err := reconciler.Provision(&domain.MBCPrimaryReconcilerProvisionInput{
		MBC:     mbc,
		PVCSC:   sc,
		CronJob: nil,
	})
	require.NoError(t, err)
	cronJob := reconciler.Operations.TakeAll()[0].(*domain.CreateOrUpdateMBCCronJobOperation).CronJob

	// Act
	err = reconciler.Finalize(&domain.MBCPrimaryReconcilerFinalizeInput{
		MBC:     mbc,
		CronJob: cronJob,
	})

	// Assert
	require.NoError(t, err)
	require.Equal(t, origMBC, mbc)
	ops := reconciler.Operations.TakeAll()
	require.Len(t, ops, 1)
	op, ok := ops[0].(*domain.DeleteMBCCronJobOperation)
	require.True(t, ok)
	require.Equal(t, cronJob, op.CronJob)
}

func TestMBCPrimaryReconciler_Finalize_NotResponsibleStorageClass(t *testing.T) {
	// Arrange
	sc1 := newSC(scWithClusterID("ceph-cluster-id"))
	sc2 := newSC(scWithClusterID("different-ceph-cluster-id"))
	reconciler := newReconciler(sc1)
	mbc := newMBC(mbcWithAnnotAndFinalizer(sc2))
	origMBC := mbc.DeepCopy()

	// Act
	err := reconciler.Finalize(&domain.MBCPrimaryReconcilerFinalizeInput{
		MBC:     mbc,
		CronJob: nil,
	})

	// Assert
	require.NoError(t, err)
	require.Equal(t, origMBC, mbc)
	require.Empty(t, reconciler.Operations.TakeAll())
}

func TestMBCPrimaryReconciler_Provision_OverwriteSchedule(t *testing.T) {
	// Arrange
	sc := newSC()
	mbc := newMBC(mbcWithAnnotAndFinalizer(sc))
	overwriteSchedule := "*/5 * * * *"
	reconciler := newReconciler(sc, reconcilerWithOverwriteSchedule(overwriteSchedule))

	// Act
	err := reconciler.Provision(&domain.MBCPrimaryReconcilerProvisionInput{
		MBC:     mbc,
		PVCSC:   sc,
		CronJob: nil,
	})

	// Assert
	require.NoError(t, err)
	operations := reconciler.Operations.TakeAll()
	require.Len(t, operations, 1)
	operation, ok := operations[0].(*domain.CreateOrUpdateMBCCronJobOperation)
	require.True(t, ok)
	cronJob := operation.CronJob
	assert.Equal(t, overwriteSchedule, cronJob.Spec.Schedule)
	assert.NotEqual(t, mbc.Spec.Schedule, cronJob.Spec.Schedule)
}

func TestMBCPrimaryReconciler_Provision_SuspendTrue(t *testing.T) {
	// Arrange
	sc := newSC()
	mbc := newMBC(mbcWithAnnotAndFinalizer(sc), mbcWithSuspend(true))
	reconciler := newReconciler(sc)

	// Act
	err := reconciler.Provision(&domain.MBCPrimaryReconcilerProvisionInput{
		MBC:     mbc,
		PVCSC:   sc,
		CronJob: nil,
	})

	// Assert
	require.NoError(t, err)
	operations := reconciler.Operations.TakeAll()
	require.Len(t, operations, 1)
	operation, ok := operations[0].(*domain.CreateOrUpdateMBCCronJobOperation)
	require.True(t, ok)
	cronJob := operation.CronJob
	assert.True(t, *cronJob.Spec.Suspend)
}
