package controller

import (
	"testing"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
)

func TestMantleBackupConfigPrimaryReconcileLogic_Reconcile_CreateOrUpdateCronJob(t *testing.T) {
	// Arrange
	logic := NewMantleBackupConfigPrimaryReconcileLogic("ceph-cluster-id", "")
	logic.SetRunningNamespace("controller-ns")
	mbc := &mantlev1.MantleBackupConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mbc-name",
			Namespace: "mbc-namespace",
			UID:       "mbc-uid",
			Finalizers: []string{
				"mantlebackupconfig.mantle.cybozu.io/finalizer",
			},
		},
		Spec: mantlev1.MantleBackupConfigSpec{
			PVC:      "pvc-name",
			Schedule: "0 0 * * *",
			Expire:   "2w",
			Suspend:  false,
		},
	}
	mbcPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc-name",
			Namespace: "mbc-namespace",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: ptr.To("sc-name"),
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
	ctrlResult, err := logic.Reconcile(mbc, mbcPVC, mbcPVCSC, nil)

	// Assert
	require.NoError(t, err)
	require.Equal(t, ctrl.Result{}, ctrlResult)
	require.Equal(t, []Event{
		CreateOrUpdateMantleBackupConfigCronJobEvent{
			mbcName:          "mbc-name",
			mbcNamespace:     "mbc-namespace",
			cronJobName:      "mbc-mbc-uid",
			cronJobNamespace: "controller-ns",
			schedule:         "0 0 * * *",
			suspend:          false,
		},
	}, logic.events)
}
