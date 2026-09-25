package multik8s

import (
	"github.com/cybozu-go/mantle/internal/ceph"
	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// annotDiffFrom is the annotation the primary controller puts on a MantleBackup
// whose data is transferred incrementally. Its value is the name of the
// MantleBackup the incremental data is based on.
const annotDiffFrom = "mantle.cybozu.io/diff-from"

// rbdSnapshotNames returns the names of the given RBD snapshots.
func rbdSnapshotNames(snaps []ceph.RBDSnapshot) []string {
	names := make([]string, 0, len(snaps))
	for _, snap := range snaps {
		names = append(names, snap.Name)
	}

	return names
}

var _ = Describe("import job rollback", Label("import-rollback"), func() {
	// An import Job can be interrupted after it has partially applied
	// incremental data to the head of the destination image. The head of the
	// destination image then differs from the snapshot the incremental data is
	// based on, so it has to be rolled back before the data is applied again.
	// Otherwise the image would be silently corrupted.
	It("should not corrupt the destination image whose head was dirtied", func(ctx SpecContext) {
		namespace := util.GetUniqueName("ns-")
		pvcName := util.GetUniqueName("pvc-")
		backupName0 := util.GetUniqueName("mb-")
		backupName1 := util.GetUniqueName("mb-")
		restoreName0 := util.GetUniqueName("mr-")
		restoreName1 := util.GetUniqueName("mr-")

		SetupNamespaces(namespace)
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName, SCName1)

		// create M0 and wait until its import Job has gone.
		writtenDataHash0 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName0)
		WaitMantleBackupSynced(namespace, backupName0)

		primaryMB0, err := GetMB(PrimaryK8sCluster, namespace, backupName0)
		Expect(err).NotTo(HaveOccurred())
		secondaryMB0, err := GetMB(SecondaryK8sCluster, namespace, backupName0)
		Expect(err).NotTo(HaveOccurred())
		WaitTemporaryResourcesDeleted(ctx, primaryMB0, secondaryMB0)

		// Simulate an interrupted import Job by writing to the head of the
		// destination image in the secondary cluster.
		DirtyRBDImageHeadOfPVC(SecondaryK8sCluster, namespace, pvcName)

		// create M1. Its incremental data is based on the snapshot of M0, so
		// the import Job has to roll the dirtied head back before applying it.
		writtenDataHash1 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName1)
		WaitMantleBackupSynced(namespace, backupName1)

		primaryMB1, err := GetMB(PrimaryK8sCluster, namespace, backupName1)
		Expect(err).NotTo(HaveOccurred())
		secondaryMB1, err := GetMB(SecondaryK8sCluster, namespace, backupName1)
		Expect(err).NotTo(HaveOccurred())
		WaitTemporaryResourcesDeleted(ctx, primaryMB1, secondaryMB1)

		// Make sure both backups are still restorable in the secondary cluster.
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
	})

	// Deleting a MantleBackup only in the primary cluster leaves its snapshot
	// in the secondary cluster. The next incremental data is then based on an
	// older snapshot than the latest one of the destination image, so its head
	// doesn't match the base snapshot and must be rolled back before the
	// import.
	It("should not corrupt the destination image whose head is newer than the base snapshot",
		func(ctx SpecContext) {
			namespace := util.GetUniqueName("ns-")
			pvcName := util.GetUniqueName("pvc-")
			backupName0 := util.GetUniqueName("mb-")
			backupName1 := util.GetUniqueName("mb-")
			backupName2 := util.GetUniqueName("mb-")
			restoreName0 := util.GetUniqueName("mr-")
			restoreName1 := util.GetUniqueName("mr-")
			restoreName2 := util.GetUniqueName("mr-")

			SetupNamespaces(namespace)
			CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName, SCName1)

			// create M0 and M1 and wait until both are replicated.
			writtenDataHash0 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName0)
			WaitMantleBackupSynced(namespace, backupName0)

			writtenDataHash1 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName1)
			WaitMantleBackupSynced(namespace, backupName1)

			primaryMB1, err := GetMB(PrimaryK8sCluster, namespace, backupName1)
			Expect(err).NotTo(HaveOccurred())
			secondaryMB1, err := GetMB(SecondaryK8sCluster, namespace, backupName1)
			Expect(err).NotTo(HaveOccurred())
			WaitTemporaryResourcesDeleted(ctx, primaryMB1, secondaryMB1)

			// Delete M1 only in the primary cluster. The secondary copy of M1
			// stays, so the head of the destination image keeps the contents of
			// M1 while the primary cluster keeps only the snapshot of M0.
			DeleteMantleBackup(PrimaryK8sCluster, namespace, backupName1)
			WaitMantleBackupDeleted(ctx, PrimaryK8sCluster, namespace, backupName1)
			EnsureMantleBackupExists(ctx, SecondaryK8sCluster, namespace, backupName1)

			// Make sure the premise of this test holds: only the snapshot of
			// M0 is left in the primary cluster, while the destination image
			// in the secondary cluster still has the newer snapshot of M1.
			Eventually(ctx, func(g Gomega) {
				primarySnaps, err := ListRBDSnapshotsInPVC(PrimaryK8sCluster, namespace, pvcName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(rbdSnapshotNames(primarySnaps)).To(ConsistOf(backupName0))
				secondarySnaps, err := ListRBDSnapshotsInPVC(SecondaryK8sCluster, namespace, pvcName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(rbdSnapshotNames(secondarySnaps)).To(ConsistOf(backupName0, backupName1))
			}).Should(Succeed())

			// create M2. Its incremental data is based on the snapshot of M0,
			// which is no longer the latest snapshot of the destination image,
			// so the import Job has to roll the head back to M0.
			writtenDataHash2 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName2)

			// Make sure the incremental data of M2 is really based on M0. The
			// annotation is removed once the MantleBackup is synced, so it has
			// to be checked while the transfer is still in progress.
			Eventually(ctx, func(g Gomega) {
				mb, err := GetMB(PrimaryK8sCluster, namespace, backupName2)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(mb.GetAnnotations()).To(HaveKeyWithValue(annotDiffFrom, backupName0))
			}).Should(Succeed())

			WaitMantleBackupSynced(namespace, backupName2)

			primaryMB2, err := GetMB(PrimaryK8sCluster, namespace, backupName2)
			Expect(err).NotTo(HaveOccurred())
			secondaryMB2, err := GetMB(SecondaryK8sCluster, namespace, backupName2)
			Expect(err).NotTo(HaveOccurred())
			WaitTemporaryResourcesDeleted(ctx, primaryMB2, secondaryMB2)

			// Make sure every backup kept in the secondary cluster is still
			// restorable, including M1 whose snapshot the rollback must not
			// have damaged.
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName2, restoreName2, writtenDataHash2)
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
		})
})
