package infra_test

import (
	"context"
	"errors"
	"testing"

	"github.com/cybozu-go/mantle/internal/controller/infra"
	"github.com/cybozu-go/mantle/test/util"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

var errGet = errors.New("get error")

// createTestNamespace creates a namespace with a unique name in envtest, so that the
// tests running in parallel don't share resources.
func createTestNamespace(t *testing.T) string {
	t.Helper()

	namespace := util.GetUniqueName("ns-")
	err := k8sClient.Create(t.Context(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	})
	require.NoError(t, err)

	return namespace
}

// createRBDPVC creates a PVC whose StorageClass is provisioned by RBD of the Ceph
// cluster clusterID. It returns the namespace and the name of the PVC.
func createRBDPVC(t *testing.T, clusterID string) (string, string) {
	t.Helper()

	storageClassName := util.GetUniqueName("sc-")
	err := k8sClient.Create(t.Context(), &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: storageClassName},
		Provisioner: "rook-ceph.rbd.csi.ceph.com",
		Parameters:  map[string]string{"clusterID": clusterID},
	})
	require.NoError(t, err)

	namespace := createTestNamespace(t)
	pvcName := util.GetUniqueName("pvc-")
	err = k8sClient.Create(t.Context(), &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: namespace},
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

	return namespace, pvcName
}

func TestKubernetesClient_GetCephClusterIDOfPVC_Found(t *testing.T) {
	t.Parallel()

	// Arrange
	namespace, pvcName := createRBDPVC(t, "ceph-cluster-id")
	kubernetesClient := infra.NewKubernetesClient(k8sClient)

	// Act
	clusterID, found, err := kubernetesClient.GetCephClusterIDOfPVC(t.Context(), namespace, pvcName)

	// Assert
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "ceph-cluster-id", clusterID)
}

func TestKubernetesClient_GetCephClusterIDOfPVC_NotFound(t *testing.T) {
	t.Parallel()

	// Arrange
	namespace := createTestNamespace(t)
	kubernetesClient := infra.NewKubernetesClient(k8sClient)

	// Act
	clusterID, found, err := kubernetesClient.GetCephClusterIDOfPVC(t.Context(), namespace, "pvc-name")

	// Assert
	require.NoError(t, err)
	require.False(t, found)
	require.Empty(t, clusterID)
}

func TestKubernetesClient_GetCephClusterIDOfPVC_GetError(t *testing.T) {
	t.Parallel()

	// Arrange
	kubernetesClient := infra.NewKubernetesClient(interceptor.NewClient(k8sClient, interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return errGet
		},
	}))

	// Act
	_, _, err := kubernetesClient.GetCephClusterIDOfPVC(t.Context(), "pvc-namespace", "pvc-name")

	// Assert
	require.ErrorIs(t, err, errGet)
}
