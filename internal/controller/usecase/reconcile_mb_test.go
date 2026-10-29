package usecase_test

import (
	"context"
	"slices"
	"testing"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/cybozu-go/mantle/internal/controller/infra"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
	"github.com/cybozu-go/mantle/internal/controller/usecase"
	"github.com/cybozu-go/mantle/test/util"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	mbManagedCephClusterID = "managed-ceph-cluster-id"
	mbOtherCephClusterID   = "other-ceph-cluster-id"
	mbName                 = "mb-name"
	mbPVCName              = "pvc-name"
)

type mbRunner interface {
	Run(ctx context.Context, backupNamespacedName types.NamespacedName) *reconcile.Result
}

func newTestMB(namespace string, opts ...func(*mantlev1.MantleBackup)) *mantlev1.MantleBackup {
	backup := &mantlev1.MantleBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:        mbName,
			Namespace:   namespace,
			Labels:      map[string]string{},
			Annotations: map[string]string{},
		},
		Spec: mantlev1.MantleBackupSpec{
			PVC:    mbPVCName,
			Expire: "2w",
		},
	}
	for _, opt := range opts {
		opt(backup)
	}

	return backup
}

func withMBClusterIDLabel(clusterID string) func(*mantlev1.MantleBackup) {
	return func(backup *mantlev1.MantleBackup) {
		backup.Labels[domain.LabelClusterID] = clusterID
	}
}

// createTestNamespace creates a namespace with a unique name in envtest, so that the
// tests running in parallel don't share MantleBackups.
func createTestNamespace(t *testing.T) string {
	t.Helper()

	namespace := util.GetUniqueName("ns-")
	err := k8sClient.Create(t.Context(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	})
	require.NoError(t, err)

	return namespace
}

// createTestMB creates the MantleBackup in envtest.
func createTestMB(t *testing.T, backup *mantlev1.MantleBackup) {
	t.Helper()

	err := k8sClient.Create(t.Context(), backup)
	require.NoError(t, err)
}

// createTestPVC creates the PVC mbPVCName in the namespace. The PVC belongs to the
// Ceph cluster clusterID, because its StorageClass is provisioned by RBD of the cluster.
func createTestPVC(t *testing.T, namespace, clusterID string) {
	t.Helper()

	storageClassName := util.GetUniqueName("sc-")
	err := k8sClient.Create(t.Context(), &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: storageClassName},
		Provisioner: "rook-ceph.rbd.csi.ceph.com",
		Parameters:  map[string]string{"clusterID": clusterID},
	})
	require.NoError(t, err)

	err = k8sClient.Create(t.Context(), &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: mbPVCName, Namespace: namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &storageClassName,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	})
	require.NoError(t, err)
}

type managedBackupCheckTestCase struct {
	name string
	// backupOpts are the options of the MantleBackup. A nil backupOpts means
	// that the MantleBackup does not exist.
	backupOpts []func(*mantlev1.MantleBackup)
	// pvcClusterID is the Ceph cluster ID of the PVC. A nil pvcClusterID means
	// that the PVC does not exist. The labeled MantleBackups have no PVC, so
	// looking up the PVC for them would requeue them.
	pvcClusterID   *string
	expectedResult *reconcile.Result
}

func managedBackupCheckTestCases() []managedBackupCheckTestCase {
	return []managedBackupCheckTestCase{
		{
			name:           "succeeds when the MantleBackup is not found",
			backupOpts:     nil,
			pvcClusterID:   nil,
			expectedResult: reconcile.Succeeded(),
		},
		{
			name: "skips the backup labeled for another cluster without the role-specific processing",
			backupOpts: []func(*mantlev1.MantleBackup){
				withMBClusterIDLabel(mbOtherCephClusterID),
			},
			pvcClusterID:   nil,
			expectedResult: reconcile.Succeeded(),
		},
		{
			name:           "skips the unlabeled backup whose PVC belongs to another cluster",
			backupOpts:     []func(*mantlev1.MantleBackup){},
			pvcClusterID:   new(mbOtherCephClusterID),
			expectedResult: reconcile.Succeeded(),
		},
		{
			name:           "requeues the unlabeled backup whose PVC is not found",
			backupOpts:     []func(*mantlev1.MantleBackup){},
			pvcClusterID:   nil,
			expectedResult: reconcile.Requeue(),
		},
		{
			name: "processes the backup labeled for the managed cluster without looking up the PVC",
			backupOpts: []func(*mantlev1.MantleBackup){
				withMBClusterIDLabel(mbManagedCephClusterID),
			},
			pvcClusterID:   nil,
			expectedResult: reconcile.ContinueWithLegacyReconcile(),
		},
		{
			name:           "processes the unlabeled backup whose PVC belongs to the managed cluster",
			backupOpts:     []func(*mantlev1.MantleBackup){},
			pvcClusterID:   new(mbManagedCephClusterID),
			expectedResult: reconcile.ContinueWithLegacyReconcile(),
		},
	}
}

// testManagedBackupCheck runs the use case that newRunner returns with the infra
// implementation on envtest, and checks that it processes only the MantleBackups
// managed by this controller. baseOpts are applied to every MantleBackup so that it
// passes the ownership guard of the role.
func testManagedBackupCheck(
	t *testing.T,
	newRunner func(k8sClient usecase.KubernetesClient) mbRunner,
	baseOpts []func(*mantlev1.MantleBackup),
) {
	t.Helper()

	for _, testCase := range managedBackupCheckTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			namespace := createTestNamespace(t)
			if testCase.backupOpts != nil {
				createTestMB(t, newTestMB(namespace, append(slices.Clone(baseOpts), testCase.backupOpts...)...))
			}

			if testCase.pvcClusterID != nil {
				createTestPVC(t, namespace, *testCase.pvcClusterID)
			}

			runner := newRunner(infra.NewKubernetesClient(k8sClient))

			// Act
			result := runner.Run(t.Context(), types.NamespacedName{Namespace: namespace, Name: mbName})

			// Assert
			require.Equal(t, testCase.expectedResult, result)
		})
	}
}
