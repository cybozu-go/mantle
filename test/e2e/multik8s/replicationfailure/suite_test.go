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

		It("should handle removal of MantleBackup in secondary k8s cluster during a full backup", func(ctx SpecContext) {
			namespace := util.GetUniqueName("ns-")
			pvcName := util.GetUniqueName("pvc-")
			backupName := util.GetUniqueName("mb-")
			restoreName := util.GetUniqueName("mr-")

			SetupEnvironment(namespace)

			// Pause the object storage to make backups fail.
			PauseObjectStorage(ctx)
			defer ResumeObjectStorage(ctx)

			// Create MantleBackup M0.
			CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)
			writtenDataHash := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName)

			// Wait until an upload Job is created.
			WaitUploadJobCreated(ctx, PrimaryK8sCluster, namespace, backupName)

			primaryMB, err := GetMB(PrimaryK8sCluster, namespace, backupName)
			Expect(err).NotTo(HaveOccurred())
			secondaryMB0, err := GetMB(SecondaryK8sCluster, namespace, backupName)
			Expect(err).NotTo(HaveOccurred())

			// Delete M0'.
			DeleteMantleBackup(SecondaryK8sCluster, namespace, backupName)

			// Resume the object storage so that M0' will be deleted correctly after its finalization.
			ResumeObjectStorage(ctx)

			// M0' should be re-created and synced.
			WaitMantleBackupSynced(namespace, backupName)

			// Make sure the PVC in the primary and secondary cluster DOES have a snapshot.
			EnsurePVCHasSnapshot(PrimaryK8sCluster, namespace, pvcName, backupName)
			EnsurePVCHasSnapshot(SecondaryK8sCluster, namespace, pvcName, backupName)

			// Make sure we can restore correct data in the primary and secondary cluster.
			EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName, restoreName, writtenDataHash)
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName, restoreName, writtenDataHash)

			// Make sure all unnecessary resources are removed.
			secondaryMB1, err := GetMB(SecondaryK8sCluster, namespace, backupName)
			Expect(err).NotTo(HaveOccurred())
			WaitTemporaryResourcesDeleted(ctx, primaryMB, secondaryMB0)
			WaitTemporaryResourcesDeleted(ctx, primaryMB, secondaryMB1)
		})
	})
}
