package multik8s

import (
	_ "embed"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMtest(t *testing.T) {
	if os.Getenv("E2ETEST") == "" {
		t.Skip("Run under e2e/")
	}

	RegisterFailHandler(Fail)

	SetDefaultEventuallyPollingInterval(time.Second)
	SetDefaultEventuallyTimeout(3 * time.Minute)

	RunSpecs(t, "rbd backup system test with multiple k8s clusters")
}

var _ = Describe("Mantle", func() {
	Context("wait controller to be ready", waitControllerToBeReady)
	Context("replication test", replicationTestSuite)
})

func waitControllerToBeReady() {
	It("wait for mantle-controller to be ready", func() {
		Eventually(func() error {
			return checkDeploymentReady(primaryK8sCluster, "rook-ceph", "mantle-controller")
		}).Should(Succeed())

		Eventually(func() error {
			return checkDeploymentReady(primaryK8sCluster, "rook-ceph", "mantle-controller")
		}).Should(Succeed())
	})
}

func replicationTestSuite() {
	Describe("replication test", func() {
		It("should eventually set SyncedToRemote of a MantleBackup to True after it is created", func() {
			namespace := util.GetUniqueName("ns-")
			pvcName := util.GetUniqueName("pvc-")
			backupName := util.GetUniqueName("mb-")
			scName := util.GetUniqueName("sc-")
			poolName := util.GetUniqueName("pool-")

			By("setting up the environment")
			Eventually(func() error {
				return createNamespace(primaryK8sCluster, namespace)
			}).Should(Succeed())
			Eventually(func() error {
				return applyRBDPoolAndSCTemplate(primaryK8sCluster, cephClusterNamespace, poolName, scName)
			}).Should(Succeed())
			Eventually(func() error {
				return applyPVCTemplate(primaryK8sCluster, namespace, pvcName, scName)
			}).Should(Succeed())

			By("creating a MantleBackup object")
			Eventually(func() error {
				return applyMantleBackupTemplate(primaryK8sCluster, namespace, pvcName, backupName)
			}).Should(Succeed())

			By("checking MantleBackup's SyncedToRemote status")
			Eventually(func() error {
				mb, err := getMB(primaryK8sCluster, namespace, backupName)
				if err != nil {
					return err
				}
				cond := meta.FindStatusCondition(mb.Status.Conditions, mantlev1.BackupConditionSyncedToRemote)
				if cond == nil {
					return errors.New("couldn't find condition SyncedToRemote")
				}
				if cond.Status != metav1.ConditionTrue {
					return errors.New("status of SyncedToRemote condition is not True")
				}
				return nil
			}).Should(Succeed())
		})
	})
}
