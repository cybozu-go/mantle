package usecase_test

import (
	"testing"

	"github.com/cybozu-go/mantle/internal/controller/usecase"
	"github.com/go-openapi/testify/v2/require"
)

func newUsecase() *usecase.ReconcileMBCPrimary {
	return usecase.NewReconcileMBCPrimary()
}

func TestRun_ProvisionPath_Success(t *testing.T) {
	t.Parallel()

	// Arrange
	r := newUsecase()

	// Act
	err := r.Run()

	// Assert
	require.NoError(t, err)
}
