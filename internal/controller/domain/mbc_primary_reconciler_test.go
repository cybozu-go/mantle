package domain_test

import (
	"testing"

	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/go-openapi/testify/v2/require"
)

func newReconciler() *domain.MBCPrimaryReconciler {
	return domain.NewMBCPrimaryReconciler()
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

func TestFinalize_Success(t *testing.T) {
	t.Parallel()

	// Arrange
	r := newReconciler()

	// Act
	err := r.Finalize()

	// Assert
	require.NoError(t, err)
}
