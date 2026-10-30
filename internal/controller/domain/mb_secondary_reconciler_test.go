package domain_test

import (
	"testing"

	"github.com/cybozu-go/mantle/internal/controller/domain"
)

func TestMBSecondaryReconciler_SkipBackupNotCreatedAsSecondary(t *testing.T) {
	t.Parallel()
	testOwnershipGuard(t,
		func() mbReconciler { return domain.NewMBSecondaryReconciler() },
		nil,
		[]optionMB{mbWithRemoteUID()},
	)
}

func TestMBSecondaryReconciler_Expire(t *testing.T) {
	t.Parallel()
	testExpire(t,
		func() (mbReconciler, *domain.ReconcilerOperations) {
			reconciler := domain.NewMBSecondaryReconciler()

			return reconciler, reconciler.Operations
		},
		[]optionMB{mbWithRemoteUID()},
	)
}
