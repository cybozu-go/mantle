package multik8s

import (
	"fmt"
	"slices"
	"strings"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/ceph"
	"github.com/cybozu-go/mantle/internal/controller"
	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const rollbackTestPVCSize = "100Mi"

var _ = Describe("import job rollback", Label("import-rollback"), func() {
	// An import Job for an incremental backup must not corrupt the
	// destination RBD image when it's dirty, e.g., after interruption of
	// the Job.
	It("should not corrupt the destination RBD image of an incremental backup when it's dirty", func(ctx SpecContext) {
		namespace := util.GetUniqueName("ns-")
		pvcName := util.GetUniqueName("pvc-")
		backupName0 := util.GetUniqueName("mb-")
		backupName1 := util.GetUniqueName("mb-")

		SetupNamespaces(namespace)
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName, SCName1)

		// create M0 and wait until its import Job has gone.
		WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackupAndWaitSynced(ctx, namespace, pvcName, backupName0)

		// Dirty the whole destination RBD image in the secondary cluster to
		// simulate, e.g., an interrupted import Job.
		DirtyWholeRBDImageOfPVC(SecondaryK8sCluster, namespace, pvcName)
		imageSize, err := GetRBDImageSizeOfPVC(SecondaryK8sCluster, namespace, pvcName)
		Expect(err).NotTo(HaveOccurred())
		EnsureRBDImageDirtySince(ctx, SecondaryK8sCluster, namespace, pvcName, backupName0, imageSize)

		// Create M1. Its incremental data is based on the snapshot of M0, so
		// the import Job has to roll the dirtied head back before applying it.
		// The incremental data overwrites only the areas changed after M0, so
		// the dummy data written to the other areas is removed only by the
		// rollback.
		WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackupAndWaitSynced(ctx, namespace, pvcName, backupName1)

		// Make sure the incremental data doesn't cover the whole image.
		// Otherwise, the test could silently stop detecting a missing
		// rollback.
		EnsureRBDSnapshotNotFullyChangedSince(ctx, PrimaryK8sCluster, namespace, pvcName, backupName1, backupName0)

		// Make sure the dummy data has gone, i.e., the backup has exactly the
		// same contents as the one in the primary cluster.
		EnsureBackupContentIdentical(namespace, pvcName, backupName1)
	})

	// An import Job for a full backup must not corrupt the destination RBD
	// image when it's dirty, e.g., after interruption of the Job.
	It("should not corrupt the destination RBD image of a full backup when it's dirty", func(ctx SpecContext) {
		namespace := util.GetUniqueName("ns-")
		pvcName := util.GetUniqueName("pvc-")
		backupName := util.GetUniqueName("mb-")

		SetupNamespaces(namespace)

		// create M0, which is a full backup. Make the PVC much larger than the
		// data written to it, so that its RBD image always has holes, i.e.
		// unallocated areas. The import Job never overwrites the holes because
		// export-diff outputs nothing for them, so the dummy data written to
		// them is removed only by the rollback the Job performs beforehand.
		// CreatePVC doesn't work here because WriteRandomDataToPV writes data
		// of almost the same size as DefaultPVCSize.
		CreatePVCWithSize(ctx, PrimaryK8sCluster, namespace, pvcName, SCName1, rollbackTestPVCSize)
		WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)

		// make the import Job of the full data dirty the whole destination
		// image and then hang, to simulate an interrupted import Job. The
		// destination image has the same size as the source one.
		imageSize, err := GetRBDImageSizeOfPVC(PrimaryK8sCluster, namespace, pvcName)
		Expect(err).NotTo(HaveOccurred())
		script := fmt.Sprintf(`#!/bin/bash
rbd_path=$(which rbd)
rbd(){
	if [ "$1" = "import-diff" -a -z "${FROM_SNAP_NAME}" ]; then
		${rbd_path} %s
		sleep infinity
	fi
	${rbd_path} "$@"
}
%s`, strings.Join(DirtyRBDImageArgs("${POOL_NAME}/${DST_IMAGE_NAME}", imageSize), " "),
			controller.EmbedJobImportScript)
		ChangeComponentJobScript(ctx, SecondaryK8sCluster, controller.EnvImportJobScript,
			namespace, backupName, 0, &script)
		defer ChangeComponentJobScript(ctx, SecondaryK8sCluster, controller.EnvImportJobScript,
			namespace, backupName, 0, nil)

		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName)

		// Make sure the holes really exist. Without them, the test could
		// silently stop detecting a missing rollback.
		EnsureRBDSnapshotHasHoles(ctx, PrimaryK8sCluster, namespace, pvcName, backupName)

		By("waiting for the import Job to create the initialsnap")
		var secondaryMB *mantlev1.MantleBackup
		Eventually(ctx, func(g Gomega) {
			var err error
			secondaryMB, err = GetMB(SecondaryK8sCluster, namespace, backupName)
			g.Expect(err).NotTo(HaveOccurred())

			snaps, err := ListRBDSnapshotsInPVC(SecondaryK8sCluster, namespace, pvcName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(slices.ContainsFunc(snaps, func(snap ceph.RBDSnapshot) bool {
				return snap.Name == "initialsnap"
			})).To(BeTrue())
		}, "10m", "5s").Should(Succeed())

		// Wait until the import Job has dirtied the whole image, including the
		// holes. The Job writes nothing after that, so it never writes the
		// dummy data at the same time as the import Job run later.
		EnsureRBDImageDirtySince(ctx, SecondaryK8sCluster, namespace, pvcName, "initialsnap", imageSize)

		// let the import Job run the original script again.
		ChangeComponentJobScript(ctx, SecondaryK8sCluster, controller.EnvImportJobScript,
			namespace, backupName, 0, nil)
		// Delete the Job in the foreground so that the controller never
		// creates the new one while the Pod of the old one is still alive.
		_, _, err = Kubectl(SecondaryK8sCluster, nil, "delete", "-n", CephCluster1Namespace,
			"--cascade=foreground", "job", controller.MakeImportJobName(secondaryMB, 0))
		Expect(err).NotTo(HaveOccurred())

		WaitMantleBackupSynced(namespace, backupName)

		primaryMB, err := GetMB(PrimaryK8sCluster, namespace, backupName)
		Expect(err).NotTo(HaveOccurred())
		secondaryMB, err = GetMB(SecondaryK8sCluster, namespace, backupName)
		Expect(err).NotTo(HaveOccurred())
		WaitTemporaryResourcesDeleted(ctx, primaryMB, secondaryMB)

		// Make sure the dummy data has gone, i.e., the backup has exactly the
		// same contents, including the holes, as the one in the primary cluster.
		EnsureBackupContentIdentical(namespace, pvcName, backupName)
	})
})
