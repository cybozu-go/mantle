package singlek8s

import (
	_ "embed"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	cephCluster1Namespace = "rook-ceph"
	cephCluster2Namespace = "rook-ceph2"
)

func TestMtest(t *testing.T) {
	if os.Getenv("E2ETEST") == "" {
		t.Skip("Run under e2e/")
	}

	RegisterFailHandler(Fail)

	SetDefaultEventuallyPollingInterval(time.Second)
	SetDefaultEventuallyTimeout(3 * time.Minute)
	EnforceDefaultTimeoutsWhenUsingContexts()

	RunSpecs(t, "rbd backup system test")
}

var _ = ReportAfterSuite("Report on failure", func(report Report) {
	if !report.SuiteSucceeded {
		for _, clusterNS := range []string{cephCluster1Namespace, cephCluster2Namespace} {
			logs, err := getControllerLogs(clusterNS)
			if err != nil {
				GinkgoLogr.Error(err, "failed to get controller logs", "clusterNS", clusterNS)
				continue
			}
			GinkgoLogr.Info("controller pod logs", "clusterNS", clusterNS, "logs", logs)
		}
	}
})

var _ = Describe("Mantle", func() {
	Context("wait environment", waitEnvironment)
	Context("backup", backupTestSuite)
	Context("restore", restoreTestSuite)
	Context("multi Rook/Ceph env", multiRookCephTestSuite)
})

func waitEnvironment() {
	It("wait for mantle-controller to be ready", func() {
		Eventually(func() error {
			return checkDeploymentReady(cephCluster1Namespace, "mantle-controller")
		}).Should(Succeed())

		Eventually(func() error {
			return checkDeploymentReady(cephCluster2Namespace, "mantle2-controller")
		}).Should(Succeed())
	})
}
