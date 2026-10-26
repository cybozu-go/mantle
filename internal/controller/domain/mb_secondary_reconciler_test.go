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
