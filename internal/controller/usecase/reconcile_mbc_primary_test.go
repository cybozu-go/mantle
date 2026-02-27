package usecase_test

import (
	"context"
	"testing"

	"github.com/cybozu-go/mantle/internal/controller/usecase"
	"github.com/go-openapi/testify/v2/require"
	"k8s.io/apimachinery/pkg/types"
)

func newUsecase() *usecase.ReconcileMBCPrimary {
	// FIXME envtest
	return usecase.NewReconcileMBCPrimary("managed-ceph-cluster-id", nil, "cronjob-namespace")
}

func TestProvision_Success(t *testing.T) {
	t.Parallel()

	// Arrange
	r := newUsecase()

	// Act
	err := r.Run(context.TODO(), types.NamespacedName{
		Name:      "mbc-name",
		Namespace: "mbc-namespace",
	})

	// Assert
	require.NoError(t, err)
}
