package replication

import (
	"os"
	"testing"
	"time"

	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestMtest(t *testing.T) {
	if os.Getenv("E2ETEST") == "" {
		t.Skip("Run under e2e/")
	}

	RegisterFailHandler(Fail)

	SetDefaultEventuallyPollingInterval(time.Second)
	SetDefaultEventuallyTimeout(3 * time.Minute)
	EnforceDefaultTimeoutsWhenUsingContexts()

	RunSpecs(t, "replication test with multiple k8s clusters")
}

var _ = BeforeSuite(func() {
	By("waiting for mantle-controller to be ready", func() {
		Eventually(func() error {
			return CheckDeploymentReady(PrimaryK8sCluster, CephClusterNamespace, "mantle-controller")
		}).Should(Succeed())

		Eventually(func() error {
			return CheckDeploymentReady(SecondaryK8sCluster, CephClusterNamespace, "mantle-controller")
		}).Should(Succeed())
	})
})

var _ = Describe("Mantle", func() {
	Context("full backup", Label("full-backup"), testFullBackup)
	Context("incremental backup", Label("incremental-backup"), testIncrementalBackup)
	Context("backup failure", Label("backup-failure"), testBackupFailure)
	Context("change to primary", Label("change-to-primary"), testChangeToPrimary)
	Context("change to secondary", Label("change-to-secondary"), testChangeToSecondary)
	Context("misc", Label("misc"), testMisc)
})
