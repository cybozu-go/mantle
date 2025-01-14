package replicationfailure

import (
	"os"
	"testing"
	"time"

	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
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

	RunSpecs(t, "replication failure test with multiple k8s clusters")
}

var _ = Describe("Mantle", func() {
	Context("wait controller to be ready", WaitControllerToBeReady)
	Context("replication failure test", replicationFailureTestSuite)
})

func replicationFailureTestSuite() {
	Describe("replication failure", func() {
		It("should handle removal of MantleBackup in primary k8s cluster during a full backup", func(ctx SpecContext) {
			namespace := util.GetUniqueName("ns-")
			pvcName := util.GetUniqueName("pvc-")
			backupName := util.GetUniqueName("mb-")

			SetupEnvironment(namespace)

			// Pause the object storage to make backups fail.
			PauseObjectStorage(ctx)
			defer ResumeObjectStorage(ctx)

			// Create MantleBackup M0.
			CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)
			_ = WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName)

			// Wait until an upload Job is created.
			WaitUploadJobCreated(ctx, PrimaryK8sCluster, namespace, backupName)

			primaryMB, err := GetMB(PrimaryK8sCluster, namespace, backupName)
			Expect(err).NotTo(HaveOccurred())
			secondaryMB, err := GetMB(SecondaryK8sCluster, namespace, backupName)
			Expect(err).NotTo(HaveOccurred())

			// Delete M0.
			DeleteMantleBackup(PrimaryK8sCluster, namespace, backupName)

			// Make sure M0 will disappear.
			WaitMantleBackupDeleted(ctx, PrimaryK8sCluster, namespace, backupName)
			// Make sure M0' will NOT disappear.
			EnsureMantleBackupExists(ctx, SecondaryK8sCluster, namespace, backupName)

			// Make sure the PVCs have no snapshots.
			EnsurePVCHasNoSnapshots(PrimaryK8sCluster, namespace, pvcName)
			EnsurePVCHasNoSnapshots(SecondaryK8sCluster, namespace, pvcName)

			// Make sure all unnecessary resources are removed.
			WaitTemporaryJobsDeleted(ctx, primaryMB, secondaryMB)
			WaitTemporaryPVCsDeleted(ctx, primaryMB, secondaryMB)
			WaitTemporarySecondaryPVsDeleted(ctx, secondaryMB)
		})
	})
}
