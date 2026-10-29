package usecase_test

import (
	"testing"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/cybozu-go/mantle/internal/controller/usecase"
)

// withMBRemoteUID makes the MantleBackup look created as a secondary one, so that it
// passes the ownership guard of the secondary role.
func withMBRemoteUID() func(*mantlev1.MantleBackup) {
	return func(backup *mantlev1.MantleBackup) {
		backup.Annotations[domain.AnnotRemoteUID] = "remote-uid"
	}
}

func TestReconcileMBSecondary_Run_ManagedBackupCheck(t *testing.T) {
	t.Parallel()
	testManagedBackupCheck(t,
		func(k8sClient usecase.KubernetesClient) mbRunner {
			return usecase.NewReconcileMBSecondary(k8sClient, mbManagedCephClusterID)
		},
		[]func(*mantlev1.MantleBackup){withMBRemoteUID()},
	)
}
