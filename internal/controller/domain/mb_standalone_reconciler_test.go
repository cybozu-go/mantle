package domain_test

import (
	"testing"

	"github.com/cybozu-go/mantle/internal/controller/domain"
)

func TestMBStandaloneReconciler_SkipBackupCreatedAsSecondary(t *testing.T) {
	t.Parallel()
	testOwnershipGuard(t,
		func() mbReconciler { return domain.NewMBStandaloneReconciler() },
		[]optionMB{mbWithRemoteUID()},
		nil,
	)
}
