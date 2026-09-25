package multik8s

import (
	"slices"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/ceph"
	"github.com/cybozu-go/mantle/internal/controller"
	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The RBD image of the PVC created by these tests is much larger than the data
// written to it, so that the image always has holes, i.e. unallocated areas.
// The tests dirty the whole destination image and depend on the holes: an
// import Job never overwrites them because export-diff outputs nothing for
// them, so the dummy data written to them is removed only by the rollback the
// Job performs beforehand. Each test makes sure the holes really exist with
// EnsureRBDSnapshotHasHoles, without which the tests could silently stop
// detecting a missing rollback.
const (
	rollbackTestPVCSize     = "100Mi"
	rollbackTestMinHoleSize = 64 << 20
)

var _ = Describe("import job rollback", Label("import-rollback"), func() {
	// An import Job for an incremental backup must not corrupt the
	// destination RBD image when it's dirty, e.g., after interruption of
	// the Job.
	It("should not corrupt the destination RBD image when it's dirty", func(ctx SpecContext) {
		namespace := util.GetUniqueName("ns-")
		pvcName := util.GetUniqueName("pvc-")
		backupName0 := util.GetUniqueName("mb-")
		backupName1 := util.GetUniqueName("mb-")

		SetupNamespaces(namespace)
		CreatePVCWithSize(ctx, PrimaryK8sCluster, namespace, pvcName, SCName1, rollbackTestPVCSize)

		// create M0 and wait until its import Job has gone.
		WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName0)
		WaitMantleBackupSynced(namespace, backupName0)

		primaryMB0, err := GetMB(PrimaryK8sCluster, namespace, backupName0)
		Expect(err).NotTo(HaveOccurred())
		secondaryMB0, err := GetMB(SecondaryK8sCluster, namespace, backupName0)
		Expect(err).NotTo(HaveOccurred())
		WaitTemporaryResourcesDeleted(ctx, primaryMB0, secondaryMB0)

		EnsureRBDSnapshotHasHoles(PrimaryK8sCluster, namespace, pvcName, backupName0, rollbackTestMinHoleSize)
		EnsureBackupContentIdentical(namespace, pvcName, backupName0)

		// Dirty the destination RBD image in the secondary cluster to
		// simulate, e.g., an interrupted import Job.
		DirtyWholeRBDImageOfPVC(SecondaryK8sCluster, namespace, pvcName)
		EnsureRBDImageDirtySince(SecondaryK8sCluster, namespace, pvcName, backupName0, rollbackTestMinHoleSize)

		// create M1. Its incremental data is based on the snapshot of M0, so
		// the import Job has to roll the dirtied head back before applying it.
		// Note that the test also fails if the data is transferred as a full
		// backup, because the import Job then takes the initialsnap from the
		// dirtied image and rolls the image back to it.
		WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName1)
		WaitMantleBackupSynced(namespace, backupName1)

		primaryMB1, err := GetMB(PrimaryK8sCluster, namespace, backupName1)
		Expect(err).NotTo(HaveOccurred())
		secondaryMB1, err := GetMB(SecondaryK8sCluster, namespace, backupName1)
		Expect(err).NotTo(HaveOccurred())
		WaitTemporaryResourcesDeleted(ctx, primaryMB1, secondaryMB1)

		// Make sure the dummy data has gone and both backups have exactly the
		// same contents as the ones in the primary cluster.
		EnsureBackupContentIdentical(namespace, pvcName, backupName1)
		EnsureBackupContentIdentical(namespace, pvcName, backupName0)
		EnsureRBDImageCleanSince(SecondaryK8sCluster, namespace, pvcName, backupName1)
	})

	// An import Job for a full backup must not corrupt the destination RBD
	// image when it's dirty, e.g., after interruption of the Job.
	It("should not corrupt the destination RBD image of a full backup when it's dirty", func(ctx SpecContext) {
		namespace := util.GetUniqueName("ns-")
		pvcName := util.GetUniqueName("pvc-")
		backupName := util.GetUniqueName("mb-")

		SetupNamespaces(namespace)

		// make the import Job of the full data dirty the whole destination
		// image and then hang, to simulate an interrupted import Job.
		script := `#!/bin/bash
rbd_path=$(which rbd)
rbd(){
	if [ "$1" = "import-diff" -a -z "${FROM_SNAP_NAME}" ]; then
		image_size=$(${rbd_path} info --format json ${POOL_NAME}/${DST_IMAGE_NAME} |
			sed -e 's/.*"size":\([0-9]*\).*/\1/')
		${rbd_path} bench --io-type write --io-pattern full-seq \
			--io-size 4M --io-total ${image_size} ${POOL_NAME}/${DST_IMAGE_NAME}
		sleep infinity
	fi
	${rbd_path} "$@"
}
` + controller.EmbedJobImportScript
		ChangeComponentJobScript(ctx, SecondaryK8sCluster, controller.EnvImportJobScript,
			namespace, backupName, 0, &script)
		defer ChangeComponentJobScript(ctx, SecondaryK8sCluster, controller.EnvImportJobScript,
			namespace, backupName, 0, nil)

		// create M0, which is a full backup
		CreatePVCWithSize(ctx, PrimaryK8sCluster, namespace, pvcName, SCName1, rollbackTestPVCSize)
		WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName)

		By("waiting for the import Job to leave the initialsnap and the dirtied image")
		var secondaryMB *mantlev1.MantleBackup
		Eventually(ctx, func(g Gomega) {
			var err error
			secondaryMB, err = GetMB(SecondaryK8sCluster, namespace, backupName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(secondaryMB.IsSynced()).To(BeFalse())

			snaps, err := ListRBDSnapshotsInPVC(SecondaryK8sCluster, namespace, pvcName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(slices.ContainsFunc(snaps, func(snap ceph.RBDSnapshot) bool {
				return snap.Name == "initialsnap"
			})).To(BeTrue())

			dirty, err := IsRBDImageDirtySince(SecondaryK8sCluster, namespace, pvcName, "initialsnap")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(dirty).To(BeTrue())
		}, "10m", "5s").Should(Succeed())

		EnsureRBDImageDirtySince(SecondaryK8sCluster, namespace, pvcName, "initialsnap", rollbackTestMinHoleSize)

		// let the import Job run the original script again.
		ChangeComponentJobScript(ctx, SecondaryK8sCluster, controller.EnvImportJobScript,
			namespace, backupName, 0, nil)
		_, _, err := Kubectl(SecondaryK8sCluster, nil, "delete", "-n", CephCluster1Namespace,
			"job", controller.MakeImportJobName(secondaryMB, 0))
		Expect(err).NotTo(HaveOccurred())

		WaitMantleBackupSynced(namespace, backupName)

		primaryMB, err := GetMB(PrimaryK8sCluster, namespace, backupName)
		Expect(err).NotTo(HaveOccurred())
		secondaryMB, err = GetMB(SecondaryK8sCluster, namespace, backupName)
		Expect(err).NotTo(HaveOccurred())
		WaitTemporaryResourcesDeleted(ctx, primaryMB, secondaryMB)

		// Make sure the import Job deleted the initialsnap, the dummy data has
		// gone, and the backup has exactly the same contents as the one in the
		// primary cluster.
		EnsureRBDSnapshotHasHoles(PrimaryK8sCluster, namespace, pvcName, backupName, rollbackTestMinHoleSize)
		EnsurePVCHasNoSnapshot(SecondaryK8sCluster, namespace, pvcName, "initialsnap")
		EnsureBackupContentIdentical(namespace, pvcName, backupName)
		EnsureRBDImageCleanSince(SecondaryK8sCluster, namespace, pvcName, backupName)
	})
})
