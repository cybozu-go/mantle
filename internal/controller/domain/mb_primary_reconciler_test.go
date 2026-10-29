package domain_test

import (
	"testing"

	"github.com/cybozu-go/mantle/internal/controller/domain"
)

func TestMBPrimaryReconciler_SkipBackupCreatedAsSecondary(t *testing.T) {
	t.Parallel()
	testOwnershipGuard(t,
		func() mbReconciler { return domain.NewMBPrimaryReconciler() },
		[]optionMB{mbWithRemoteUID()},
		nil,
	)
}

func TestMBPrimaryReconciler_Expire(t *testing.T) {
	t.Parallel()
	testExpire(t,
		func() (mbReconciler, *domain.ReconcilerOperations) {
			reconciler := domain.NewMBPrimaryReconciler()

			return reconciler, reconciler.Operations
		},
		nil,
	)
}
