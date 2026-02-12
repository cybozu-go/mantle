package domain_test

import (
	"testing"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/stretchr/testify/require"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestMBCPrimaryReconciler_Provision_AttachAnnotAndFinalizer(t *testing.T) {
	// Arrange
	reconciler := domain.NewMBCPrimaryReconciler(
		"",
		"ceph-cluster-id",
		"cronjob-service-account",
		"cronjob-image",
		"cronjob-namespace",
	)
	mbc := &mantlev1.MantleBackupConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mbc-name",
			Namespace: "mbc-namespace",
			UID:       "mbc-uid",
		},
		Spec: mantlev1.MantleBackupConfigSpec{
			PVC:      "pvc-name",
			Schedule: "0 0 * * *",
			Expire:   "2w",
			Suspend:  false,
		},
	}
	mbcPVCSC := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "sc-name",
		},
		Provisioner: "hoge.rbd.csi.ceph.com",
		Parameters: map[string]string{
			"clusterID": "ceph-cluster-id",
		},
	}

	// Act
	err := reconciler.Provision(&domain.MBCPrimaryReconcilerProvisionInput{
		MBC:     mbc,
		PVCSC:   mbcPVCSC,
		CronJob: nil,
	})

	// Assert
	require.NoError(t, err)
	require.Empty(t, reconciler.Events.TakeAll())
	require.True(t, controllerutil.ContainsFinalizer(mbc, domain.MantleBackupConfigFinalizerName))
	require.Equal(t, "ceph-cluster-id", mbc.Annotations[domain.MantleBackupConfigAnnotationManagedClusterID])
}
