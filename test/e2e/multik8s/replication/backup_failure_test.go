package replication

import (
	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup failure", Label("backup-failure"), func() {
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
		WaitUploadJobCreated(ctx, PrimaryK8sCluster, namespace, backupName, 0)

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
		WaitUploadJobCreated(ctx, PrimaryK8sCluster, namespace, backupName, 0)

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

	It("should handle removal of MantleBackup in primary k8s cluster during an incremental backup",
		func(ctx SpecContext) {
			namespace := util.GetUniqueName("ns-")
			pvcName := util.GetUniqueName("pvc-")
			backupName0 := util.GetUniqueName("mb-")
			backupName1 := util.GetUniqueName("mb-")
			restoreName0 := util.GetUniqueName("mr-")

			SetupEnvironment(namespace)

			// Create MantleBackup M0.
			CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)
			writtenDataHash0 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName0)
			WaitMantleBackupSynced(namespace, backupName0)

			// Pause the object storage to make backups fail.
			PauseObjectStorage(ctx)
			defer ResumeObjectStorage(ctx)

			// Create MantleBackup M1.
			CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)
			_ = WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName1)

			// Wait until an upload Job is created.
			WaitUploadJobCreated(ctx, PrimaryK8sCluster, namespace, backupName1, 0)

			primaryMB, err := GetMB(PrimaryK8sCluster, namespace, backupName1)
			Expect(err).NotTo(HaveOccurred())
			secondaryMB, err := GetMB(SecondaryK8sCluster, namespace, backupName1)
			Expect(err).NotTo(HaveOccurred())

			// Delete MantleBackup M1.
			DeleteMantleBackup(PrimaryK8sCluster, namespace, backupName1)

			// Make sure M1 will disappear.
			WaitMantleBackupDeleted(ctx, PrimaryK8sCluster, namespace, backupName1)

			// Make sure M0, M0', and M1' will NOT disappear.
			EnsureMantleBackupExists(ctx, PrimaryK8sCluster, namespace, backupName0)
			EnsureMantleBackupExists(ctx, SecondaryK8sCluster, namespace, backupName0)
			EnsureMantleBackupExists(ctx, SecondaryK8sCluster, namespace, backupName1)

			// Make sure we can restore correct data from M0 and M0'.
			EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)

			// Make sure all unnecessary resources are removed.
			WaitTemporaryJobsDeleted(ctx, primaryMB, secondaryMB)
			WaitTemporaryPVCsDeleted(ctx, primaryMB, secondaryMB)
			WaitTemporarySecondaryPVsDeleted(ctx, secondaryMB)
		})

	It("should handle removal of MantleBackup in secondary k8s cluster during an incremental backup",
		func(ctx SpecContext) {
			namespace := util.GetUniqueName("ns-")
			pvcName := util.GetUniqueName("pvc-")
			backupName0 := util.GetUniqueName("mb-")
			backupName1 := util.GetUniqueName("mb-")
			restoreName0 := util.GetUniqueName("mr-")
			restoreName1 := util.GetUniqueName("mr-")

			SetupEnvironment(namespace)

			// Create MantleBackup M0.
			CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)
			writtenDataHash0 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName0)
			WaitMantleBackupSynced(namespace, backupName0)

			// Pause the object storage to make backups fail.
			PauseObjectStorage(ctx)
			defer ResumeObjectStorage(ctx)

			// Create MantleBackup M1.
			CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)
			writtenDataHash1 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName1)

			// Wait until an upload Job is created.
			WaitUploadJobCreated(ctx, PrimaryK8sCluster, namespace, backupName1, 0)

			primaryMB1, err := GetMB(PrimaryK8sCluster, namespace, backupName1)
			Expect(err).NotTo(HaveOccurred())
			secondaryMB10, err := GetMB(SecondaryK8sCluster, namespace, backupName1)
			Expect(err).NotTo(HaveOccurred())

			// Delete MantleBackup M1'.
			DeleteMantleBackup(SecondaryK8sCluster, namespace, backupName1)

			// Resume the object storage so that M1' will be deleted correctly after its finalization.
			ResumeObjectStorage(ctx)

			// M1' should be re-created and synced.
			WaitMantleBackupSynced(namespace, backupName1)

			// Make sure the PVC in the primary and secondary cluster DOES have a snapshot.
			EnsurePVCHasSnapshot(PrimaryK8sCluster, namespace, pvcName, backupName1)
			EnsurePVCHasSnapshot(SecondaryK8sCluster, namespace, pvcName, backupName1)

			// Make sure we can restore correct data from M0, M0', M1, and M1'.
			EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
			EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)

			// Make sure all unnecessary resources are removed.
			secondaryMB11, err := GetMB(SecondaryK8sCluster, namespace, backupName1)
			Expect(err).NotTo(HaveOccurred())
			WaitTemporaryResourcesDeleted(ctx, primaryMB1, secondaryMB10)
			WaitTemporaryResourcesDeleted(ctx, primaryMB1, secondaryMB11)
		})
})
