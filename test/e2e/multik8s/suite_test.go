package multik8s

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
			return CheckDeploymentReady(PrimaryK8sCluster, CephCluster1Namespace, "mantle-controller")
		}).Should(Succeed())
		Eventually(func() error {
			return CheckDeploymentReady(SecondaryK8sCluster, CephCluster1Namespace, "mantle-controller")
		}).Should(Succeed())
	})

	By("setting up storage pools", func() {
		for _, clusterNo := range []int{PrimaryK8sCluster, SecondaryK8sCluster} {
			err := ApplyRBDPoolTemplate(clusterNo, CephCluster1Namespace)
			Expect(err).NotTo(HaveOccurred())
			err = ApplySCTemplate(clusterNo, SCName1, CephCluster1Namespace, CephCluster1Namespace)
			Expect(err).NotTo(HaveOccurred())
		}
	})
})
