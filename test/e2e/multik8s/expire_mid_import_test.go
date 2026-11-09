package multik8s

import (
	"encoding/json"
	"fmt"

	"github.com/cybozu-go/mantle/internal/ceph"
	"github.com/cybozu-go/mantle/internal/controller"
	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

var _ = Describe("expire during multi-part import", Label("lock"), func() {
	It("should stop the import Job, release the lock, and let a later MantleBackup for the same image proceed "+
		"after a multi-part MantleBackup expires while its import Job is still running", func(ctx SpecContext) {
		namespace := util.GetUniqueName("ns-")
		pvcName := util.GetUniqueName("pvc-")
		backupName0 := util.GetUniqueName("mb-")
		backupName1 := util.GetUniqueName("mb-")
		partNumSlow := 1

		SetupNamespaces(namespace)

		// Make part=1's import Job hang, so that we can reliably catch the
		// secondary MantleBackup mid-transfer (part 0 imported, part 1 still
		// running). The Job never finishes by itself, so the MantleBackup can
		// be finalized only if the controller stops the Job.
		script := fmt.Sprintf(`#!/bin/bash
if [ "${PART_NUM}" -eq %d ]; then
	sleep infinity
fi
%s`, partNumSlow, controller.EmbedJobImportScript)
		ChangeComponentJobScript(
			ctx,
			SecondaryK8sCluster,
			controller.EnvImportJobScript,
			namespace,
			backupName0,
			partNumSlow,
			&script,
		)
		defer ChangeComponentJobScript(
			ctx,
			SecondaryK8sCluster,
			controller.EnvImportJobScript,
			namespace,
			backupName0,
			partNumSlow,
			nil,
		)

		// Create a PVC with enough data that the backup is split into
		// multiple parts.
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName, SCName1)
		_ = WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
		pvc, err := GetPVC(PrimaryK8sCluster, namespace, pvcName)
		Expect(err).NotTo(HaveOccurred())
		numParts, err := GetNumberOfBackupParts(pvc.Spec.Resources.Requests.Storage())
		Expect(err).NotTo(HaveOccurred())
		Expect(numParts).To(BeNumerically(">", partNumSlow))

		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName0)

		// Wait until part=1's import Job is created on the secondary cluster,
		// i.e. part 0 has already been imported and part 1 is now in progress.
		WaitImportJobCreated(ctx, SecondaryK8sCluster, namespace, backupName0, partNumSlow)

		secondaryMB0, err := GetMB(SecondaryK8sCluster, namespace, backupName0)
		Expect(err).NotTo(HaveOccurred())
		Expect(secondaryMB0.IsSnapshotCaptured()).To(BeFalse())

		importJobName := controller.MakeImportJobName(secondaryMB0, partNumSlow)
		getImportPods := func() []corev1.Pod {
			GinkgoHelper()
			stdout, _, err := Kubectl(SecondaryK8sCluster, nil, "get", "pod", "-n", CephCluster1Namespace,
				"-l", "batch.kubernetes.io/job-name="+importJobName, "-o", "json")
			Expect(err).NotTo(HaveOccurred())
			var pods corev1.PodList
			err = json.Unmarshal(stdout, &pods)
			Expect(err).NotTo(HaveOccurred())

			return pods.Items
		}

		// Wait until the Pod of part=1's import Job is running, so that the
		// import is really in progress when the MantleBackup expires below.
		Eventually(ctx, func(g Gomega) {
			pods := getImportPods()
			g.Expect(pods).To(HaveLen(1))
			g.Expect(pods[0].Status.Phase).To(Equal(corev1.PodRunning))
		}).Should(Succeed())

		var pvStored corev1.PersistentVolume
		err = json.Unmarshal([]byte(secondaryMB0.Status.PVManifest), &pvStored)
		Expect(err).NotTo(HaveOccurred())
		poolName := pvStored.Spec.CSI.VolumeAttributes["pool"]
		imageName := pvStored.Spec.CSI.VolumeAttributes["imageName"]

		controllerPod, err := GetControllerPodName(SecondaryK8sCluster)
		Expect(err).NotTo(HaveOccurred())

		getLocks := func() []*ceph.RBDLock {
			GinkgoHelper()
			stdout, _, err := Kubectl(SecondaryK8sCluster, nil, "exec", "-n", CephCluster1Namespace, controllerPod, "--",
				"rbd", "-p", poolName, "--format", "json", "lock", "ls", imageName)
			Expect(err).NotTo(HaveOccurred())
			var locks []*ceph.RBDLock
			err = json.Unmarshal(stdout, &locks)
			Expect(err).NotTo(HaveOccurred())

			return locks
		}

		// The lock startImport took for backupName0's import should still be
		// held while part=1's import Job is still running.
		locks := getLocks()
		Expect(locks).To(HaveLen(1))
		Expect(locks[0].LockID).To(Equal(string(secondaryMB0.GetUID())))

		// Delete the primary MantleBackup first, so that once the secondary
		// copy is gone (below), the secondary controller has no primary
		// counterpart left to mirror and won't simply recreate and re-import
		// backupName0 from scratch.
		DeleteMantleBackup(PrimaryK8sCluster, namespace, backupName0)

		// Force the secondary MantleBackup to expire while its import is
		// still incomplete.
		ExpireMantleBackupNow(SecondaryK8sCluster, namespace, backupName0)

		// The secondary MantleBackup should eventually be finalized, although
		// part=1's import Job never finishes by itself. This happens only if
		// the controller deletes the Job in the middle of the import instead
		// of waiting for it to finish.
		WaitMantleBackupDeleted(ctx, SecondaryK8sCluster, namespace, backupName0)

		// The Job and its Pod must be already gone at this point: the
		// controller has to stop them before it finalizes the MantleBackup,
		// so that nothing writes to the volume after the lock is released.
		Expect(CheckJobExist(SecondaryK8sCluster, CephCluster1Namespace, importJobName)).To(BeFalse())
		Expect(getImportPods()).To(BeEmpty())

		// The lock should have been released as part of that finalization.
		Eventually(ctx, func(g Gomega) {
			g.Expect(getLocks()).To(BeEmpty())
		}).Should(Succeed())

		// Let the import Jobs run the original script again. Otherwise,
		// part=1's import Job of backupName1 would hang, too.
		ChangeComponentJobScript(
			ctx,
			SecondaryK8sCluster,
			controller.EnvImportJobScript,
			namespace,
			backupName0,
			partNumSlow,
			nil,
		)

		// A later MantleBackup for the same PVC should now be able to take
		// the lock and complete normally, proving it is not permanently
		// stuck behind the lock backupName0 would otherwise have stranded.
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName1)
		WaitMantleBackupSynced(namespace, backupName1)

		CleanupMantleBackups(namespace)
	})
})
