package infra_test

import (
	"context"
	"errors"
	"testing"

	"github.com/cybozu-go/mantle/internal/controller/infra"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

var errGet = errors.New("get error")

func newFakeClientBuilder(t *testing.T) *fake.ClientBuilder {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))

	return fake.NewClientBuilder().WithScheme(scheme)
}

func newRBDStorageClassAndPVC() (*storagev1.StorageClass, *corev1.PersistentVolumeClaim) {
	storageClass := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "sc-name"},
		Provisioner: "rook-ceph.rbd.csi.ceph.com",
		Parameters:  map[string]string{"clusterID": "ceph-cluster-id"},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-name", Namespace: "pvc-namespace"},
		Spec:       corev1.PersistentVolumeClaimSpec{StorageClassName: &storageClass.Name},
	}

	return storageClass, pvc
}

func TestKubernetesClient_GetCephClusterIDOfPVC_Found(t *testing.T) {
	t.Parallel()

	// Arrange
	storageClass, pvc := newRBDStorageClassAndPVC()
	k8sClient := infra.NewKubernetesClient(newFakeClientBuilder(t).WithObjects(storageClass, pvc).Build())

	// Act
	clusterID, found, err := k8sClient.GetCephClusterIDOfPVC(t.Context(), "pvc-namespace", "pvc-name")

	// Assert
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "ceph-cluster-id", clusterID)
}

func TestKubernetesClient_GetCephClusterIDOfPVC_NotFound(t *testing.T) {
	t.Parallel()

	// Arrange
	k8sClient := infra.NewKubernetesClient(newFakeClientBuilder(t).Build())

	// Act
	clusterID, found, err := k8sClient.GetCephClusterIDOfPVC(t.Context(), "pvc-namespace", "pvc-name")

	// Assert
	require.NoError(t, err)
	require.False(t, found)
	require.Empty(t, clusterID)
}

func TestKubernetesClient_GetCephClusterIDOfPVC_GetError(t *testing.T) {
	t.Parallel()

	// Arrange
	k8sClient := infra.NewKubernetesClient(newFakeClientBuilder(t).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
				return errGet
			},
		}).
		Build())

	// Act
	_, _, err := k8sClient.GetCephClusterIDOfPVC(t.Context(), "pvc-namespace", "pvc-name")

	// Assert
	require.ErrorIs(t, err, errGet)
}
