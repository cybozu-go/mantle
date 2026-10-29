package domain_test

import (
	"testing"

	"github.com/cybozu-go/mantle/internal/controller/domain"
)

func TestMBStandaloneReconciler_SkipBackupCreatedAsSecondary(t *testing.T) {
	t.Parallel()
	testOwnershipGuard(t,
		func() mbReconciler {
			return domain.NewMBStandaloneReconciler(managedCephClusterID, newManagedPVCClusterIDGetter())
		},
		[]optionMB{mbWithRemoteUID()},
		nil,
	)
}

func TestMBStandaloneReconciler_Expire(t *testing.T) {
	t.Parallel()
	testExpire(t,
		func() (mbReconciler, *domain.ReconcilerOperations) {
			reconciler := domain.NewMBStandaloneReconciler(managedCephClusterID, newManagedPVCClusterIDGetter())

			return reconciler, reconciler.Operations
		},
		nil,
	)
}

func TestMBStandaloneReconciler_CheckManagedBackup(t *testing.T) {
	t.Parallel()
	testManagedBackupCheck(t,
		func(pvcClusterIDGetter domain.PVCClusterIDGetter) mbReconciler {
			return domain.NewMBStandaloneReconciler(managedCephClusterID, pvcClusterIDGetter)
		},
		nil,
	)
}
