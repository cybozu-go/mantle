package usecase_test

import (
	"testing"

	"github.com/cybozu-go/mantle/internal/controller/usecase"
)

func TestReconcileMBPrimary_Run_ManagedBackupCheck(t *testing.T) {
	t.Parallel()
	testManagedBackupCheck(t,
		func(k8sClient usecase.KubernetesClient) mbRunner {
			return usecase.NewReconcileMBPrimary(k8sClient, mbManagedCephClusterID)
		},
		nil,
	)
}
