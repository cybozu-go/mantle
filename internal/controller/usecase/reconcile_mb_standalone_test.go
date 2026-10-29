package usecase_test

import (
	"testing"

	"github.com/cybozu-go/mantle/internal/controller/usecase"
)

func TestReconcileMBStandalone_Run_ManagedBackupCheck(t *testing.T) {
	t.Parallel()
	testManagedBackupCheck(t,
		func(k8sClient usecase.KubernetesClient) mbRunner {
			return usecase.NewReconcileMBStandalone(k8sClient, mbManagedCephClusterID)
		},
		nil,
	)
}
