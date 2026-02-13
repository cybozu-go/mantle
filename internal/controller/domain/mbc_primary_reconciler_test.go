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

func newMBC() *mantlev1.MantleBackupConfig {
	return &mantlev1.MantleBackupConfig{
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
}

func newSC() *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "sc-name",
		},
		Provisioner: "hoge.rbd.csi.ceph.com",
		Parameters: map[string]string{
			"clusterID": "ceph-cluster-id",
		},
	}
}

func TestMBCPrimaryReconciler_Provision_AttachAnnotAndFinalizer(t *testing.T) {
	// Arrange
	mbc := newMBC()
	sc := newSC()
	reconciler := domain.NewMBCPrimaryReconciler(
		"",
		sc.Parameters["clusterID"],
		"cronjob-service-account",
		"cronjob-image",
		"cronjob-namespace",
	)

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
	require.Equal(t, "ceph-cluster-id", mbc.Annotations[domain.MantleBackupConfigAnnotationManagedClusterID])
}

func TestMBCPrimaryReconciler_Provision_CreateCronJob(t *testing.T) {
	// Arrange
	sc := newSC()
	mbc := newMBC()
	mbc.Finalizers = append(mbc.Finalizers, domain.MantleBackupConfigFinalizerName)
	mbc.Annotations[domain.MantleBackupConfigAnnotationManagedClusterID] = sc.Parameters["clusterID"]
	reconciler := domain.NewMBCPrimaryReconciler(
		"",
		sc.Parameters["clusterID"],
		"cronjob-service-account",
		"cronjob-image",
		"cronjob-namespace",
	)

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
	assert.True(t, cronJob.CreationTimestamp.IsZero())
	assert.Equal(t, domain.GetMBCCronJobName(mbc), cronJob.Name)
	assert.Equal(t, "cronjob-namespace", cronJob.Namespace)
	assert.False(t, *cronJob.Spec.Suspend)
	pod := cronJob.Spec.JobTemplate.Spec.Template.Spec
	assert.Equal(t, "cronjob-service-account", pod.ServiceAccountName)
	container := pod.Containers[0]
	assert.Equal(t, "cronjob-image", container.Image)
}
