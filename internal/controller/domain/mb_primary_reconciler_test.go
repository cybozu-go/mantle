package domain_test

import (
	"testing"

	"github.com/cybozu-go/mantle/internal/controller/domain"
)

func TestMBPrimaryReconciler_SkipBackupCreatedAsSecondary(t *testing.T) {
	t.Parallel()
	testOwnershipGuard(t,
		func() mbReconciler {
			return domain.NewMBPrimaryReconciler(managedCephClusterID, newManagedPVCClusterIDGetter())
		},
		[]optionMB{mbWithRemoteUID()},
		nil,
	)
}

func TestMBPrimaryReconciler_CheckManagedBackup(t *testing.T) {
	t.Parallel()
	testManagedBackupCheck(t,
		func(pvcClusterIDGetter domain.PVCClusterIDGetter) mbReconciler {
			return domain.NewMBPrimaryReconciler(managedCephClusterID, pvcClusterIDGetter)
		},
		nil,
	)
}
