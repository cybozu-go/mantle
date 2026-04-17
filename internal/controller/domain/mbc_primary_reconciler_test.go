package domain_test

import (
	"testing"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type optionMBC func(*mantlev1.MantleBackupConfig)

func mbcWithAnnotAndFinalizer(sc *storagev1.StorageClass) optionMBC {
	return func(mbc *mantlev1.MantleBackupConfig) {
		mbc.Finalizers = append(mbc.Finalizers, domain.MantleBackupConfigFinalizerName)
		mbc.Annotations[domain.MantleBackupConfigAnnotationManagedClusterID] = sc.Parameters["clusterID"]
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
	storageClass := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "sc-name",
		},
		Provisioner: "hoge.rbd.csi.ceph.com",
		Parameters: map[string]string{
			"clusterID": "ceph-cluster-id",
		},
	}
	for _, opt := range opts {
		opt(storageClass)
	}

	return storageClass
}

func newReconciler(sc *storagev1.StorageClass) *domain.MBCPrimaryReconciler {
	return domain.NewMBCPrimaryReconciler(sc.Parameters["clusterID"])
}

func TestProvision_Error_NotImplemented(t *testing.T) {
	t.Parallel()

	// Arrange
	r := newReconciler(newSC())

	// Act
	err := r.Provision()

	// Assert
	require.ErrorIs(t, err, domain.ErrNotImplemented)
}

// TestMBCPrimaryReconciler_Finalize_NoProvision tests that Finalize does
// nothing when the MBC was never provisioned (i.e., has no finalizer).
func TestMBCPrimaryReconciler_Finalize_NoProvision(t *testing.T) {
	t.Parallel()

	// Arrange
	mbc := newMBC()
	origMBC := mbc.DeepCopy()
	reconciler := newReconciler(newSC())

	// Act
	err := reconciler.Finalize(mbc, nil)

	// Assert
	require.NoError(t, err)
	require.Equal(t, origMBC, mbc)
	require.Empty(t, reconciler.Operations.TakeAll())
}

// TestMBCPrimaryReconciler_Finalize_RemoveCronJob tests that Finalize
// emits a DeleteMBCCronJobOperation when the CronJob still exists.
// The finalizer should not be removed until the CronJob is deleted.
func TestMBCPrimaryReconciler_Finalize_RemoveCronJob(t *testing.T) {
	t.Parallel()

	// Arrange
	sc := newSC()
	mbc := newMBC(mbcWithAnnotAndFinalizer(sc))
	origMBC := mbc.DeepCopy()
	reconciler := newReconciler(sc)
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      domain.GetMBCCronJobName(mbc),
			Namespace: "cronjob-namespace",
		},
	}

	// Act
	err := reconciler.Finalize(mbc, cronJob)

	// Assert
	require.NoError(t, err)
	require.Equal(t, origMBC, mbc)

	ops := reconciler.Operations.TakeAll()
	require.Len(t, ops, 1)
	op, ok := ops[0].(*domain.DeleteMBCCronJobOperation)
	require.True(t, ok)
	require.Equal(t, cronJob, op.CronJob)
}

// TestMBCPrimaryReconciler_Finalize_RemoveFinalizer tests that Finalize
// removes the finalizer when the CronJob has already been deleted
// (i.e., when cronJob is nil).
func TestMBCPrimaryReconciler_Finalize_RemoveFinalizer(t *testing.T) {
	t.Parallel()

	// Arrange
	sc := newSC()
	mbc := newMBC(mbcWithAnnotAndFinalizer(sc))
	reconciler := newReconciler(sc)

	// Act
	err := reconciler.Finalize(mbc, nil)

	// Assert
	require.NoError(t, err)
	require.Empty(t, mbc.Finalizers)
	require.Empty(t, reconciler.Operations.TakeAll())
}

// TestMBCPrimaryReconciler_Finalize_NotResponsibleStorageClass tests that
// Finalize does nothing when the MBC was provisioned by a different reconciler
// (identified by the managed cluster ID annotation).
func TestMBCPrimaryReconciler_Finalize_NotResponsibleStorageClass(t *testing.T) {
	t.Parallel()

	// Arrange
	sc1 := newSC(scWithClusterID("ceph-cluster-id"))
	sc2 := newSC(scWithClusterID("different-ceph-cluster-id"))
	reconciler := newReconciler(sc1)
	mbc := newMBC(mbcWithAnnotAndFinalizer(sc2))
	origMBC := mbc.DeepCopy()

	// Act
	err := reconciler.Finalize(mbc, nil)

	// Assert
	require.NoError(t, err)
	require.Equal(t, origMBC, mbc)
	require.Empty(t, reconciler.Operations.TakeAll())
}
