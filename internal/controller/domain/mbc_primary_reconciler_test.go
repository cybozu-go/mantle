package domain_test

import (
	"testing"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/go-openapi/testify/v2/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newReconciler() *domain.MBCPrimaryReconciler {
	return domain.NewMBCPrimaryReconciler("test-managed-ceph-cluster-id")
}

func TestProvision_Success(t *testing.T) {
	t.Parallel()

	// Arrange
	r := newReconciler()

	// Act
	err := r.Provision()

	// Assert
	require.NoError(t, err)
}

func TestFinalize_Success_NoManagedCephClusterIDAnnotation(t *testing.T) {
	t.Parallel()

	// Arrange
	reconciler := newReconciler()
	mbc := &mantlev1.MantleBackupConfig{ //nolint:exhaustruct
		ObjectMeta: metav1.ObjectMeta{ //nolint:exhaustruct
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

	// Act
	err := reconciler.Finalize(mbc, nil)

	// Assert
	require.NoError(t, err)
}
