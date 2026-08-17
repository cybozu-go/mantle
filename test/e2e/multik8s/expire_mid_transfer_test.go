package multik8s

import (
	"encoding/json"
	"fmt"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/ceph"
	"github.com/cybozu-go/mantle/internal/controller"
	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

// --- Bug hypothesis under investigation ---------------------------------
//
// Hypothesis being checked (a large backup transferred in numbered parts,
// e.g. part 0, part 1, ... part 6, ...):
//  1. import Job progresses normally up through part 5.
//  2. at that point, the primary's MantleBackup gets deleted -- by
//     Spec.Expire elapsing (or something else) -- before part 6 is
//     exported/uploaded.
//  3. the secondary starts importing part 6 and takes a lock via
//     lockVolume().
//  4. reconcileImportJob() finds no part 6 data in the object storage
//     and requeues.
//     -> from step 4 onward, since the primary's MantleBackup is gone,
//     part 6 can never be uploaded, so the secondary requeues forever.
//
// mantlebackup_controller_test.go already proves steps 2 and 4 in
// isolation, by calling the relevant controller methods directly. This
// spec instead drives the real primary and secondary controllers,
// connected over the real gRPC replication path, in one continuous run --
// which additionally covers steps 1 and 3, and confirms the two isolated
// findings really do chain together end to end. It uses part 0/part 1
// instead of part 5/part 6 for speed; the mechanism is identical.
var _ = Describe("expire mid multi-part transfer", Label("investigate-expire-mid-transfer"), func() {
	It("should strand the secondary import forever, with the RBD lock still held, "+
		"once the primary expires mid-transfer", func(ctx SpecContext) {
		namespace := util.GetUniqueName("ns-")
		pvcName := util.GetUniqueName("pvc-")
		backupName := util.GetUniqueName("mb-")
		partNumStuck := 1

		SetupNamespaces(namespace)

		// (a) Make part 1's export permanently fail, so part 0 can complete
		// normally but part 1 never produces any export data at all. This
		// stands in for "part 6 never got exported/uploaded" in hypothesis
		// step 2 -- and, unlike pausing the object storage, it can't
		// accidentally also block part 0.
		script := fmt.Sprintf(`#!/bin/bash
rbd_path=$(which rbd)
rbd(){
	if [ ${PART_NUM} -eq %d ]; then
		return 1
	else
		${rbd_path} "$@"
	fi
}
%s`, partNumStuck, controller.EmbedJobExportScript)
		ChangeComponentJobScript(
			ctx, PrimaryK8sCluster, controller.EnvExportJobScript, namespace, backupName, partNumStuck, &script)
		defer ChangeComponentJobScript(
			ctx, PrimaryK8sCluster, controller.EnvExportJobScript, namespace, backupName, partNumStuck, nil)

		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName, SCName1)
		_ = WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName)

		// (b) Hypothesis step 1: confirm part 0 really does progress all
		// the way through import on the secondary, while part 1's export
		// keeps failing (i.e. never produces data).
		By("waiting for part 0 to fully import on the secondary, while part 1's export keeps failing")
		var primaryMB, secondaryMB *mantlev1.MantleBackup
		Eventually(ctx, func(g Gomega) {
			var err error
			primaryMB, err = GetMB(PrimaryK8sCluster, namespace, backupName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(primaryMB.IsSynced()).To(BeFalse())

			secondaryMB, err = GetMB(SecondaryK8sCluster, namespace, backupName)
			g.Expect(err).NotTo(HaveOccurred())

			importJob0, err := GetJob(SecondaryK8sCluster, CephCluster1Namespace, controller.MakeImportJobName(secondaryMB, 0))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(IsJobConditionTrue(importJob0.Status.Conditions, batchv1.JobComplete)).To(BeTrue())

			exportJob1, err := GetJob(
				PrimaryK8sCluster, CephCluster1Namespace, controller.MakeExportJobName(primaryMB, partNumStuck))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(IsJobConditionTrue(exportJob1.Status.Conditions, batchv1.JobComplete)).To(BeFalse())
		}).Should(Succeed())

		// Resolve the secondary's pool/image, to inspect RBD locks on it.
		var pv corev1.PersistentVolume
		Expect(json.Unmarshal([]byte(secondaryMB.Status.PVManifest), &pv)).To(Succeed())
		poolName := pv.Spec.CSI.VolumeAttributes["pool"]
		imageName := pv.Spec.CSI.VolumeAttributes["imageName"]
		controllerPod, err := GetControllerPodName(SecondaryK8sCluster)
		Expect(err).NotTo(HaveOccurred())
		secondaryUID := string(secondaryMB.GetUID())

		// (c) Hypothesis step 3: confirm the secondary really does hold an
		// RBD lock (keyed by its own UID) on the volume while importing --
		// taken by lockVolume() before reconcileImportJob() runs.
		By("confirming the secondary holds an RBD lock on the volume during import")
		Eventually(ctx, func(g Gomega) {
			locks := getRBDLocks(SecondaryK8sCluster, controllerPod, poolName, imageName)
			g.Expect(locks).To(ContainElement(HaveField("LockID", Equal(secondaryUID))))
		}).Should(Succeed())

		// (d) Hypothesis step 2: force the primary to expire right now, in
		// the middle of part 1's (permanently failing) transfer.
		ExpireMantleBackupNow(PrimaryK8sCluster, namespace, backupName)

		By("confirming the primary MantleBackup is deleted despite part 1 never finishing")
		Eventually(ctx, func(g Gomega) {
			_, err := GetMB(PrimaryK8sCluster, namespace, backupName)
			g.Expect(err).To(HaveOccurred())
		}).Should(Succeed())

		// (e) The proof (hypothesis step 4, and its consequence): with the
		// primary gone, part 1's data will never arrive. Prove the
		// secondary's import (i) never completes and (ii) never releases
		// the RBD lock it took in step (c) -- unlockVolume() is only
		// reached once reconcileImportJob() stops requeuing, which never
		// happens -- by observing both stay true over a window much longer
		// than the controller's normal resync period.
		By("proving the secondary requeues forever and never releases the RBD lock")
		Consistently(ctx, func(g Gomega) {
			mb, err := GetMB(SecondaryK8sCluster, namespace, backupName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(mb.IsSynced()).To(BeFalse())

			locks := getRBDLocks(SecondaryK8sCluster, controllerPod, poolName, imageName)
			g.Expect(locks).To(ContainElement(HaveField("LockID", Equal(secondaryUID))))
		}, "2m", "5s").Should(Succeed())
	})
})

func getRBDLocks(cluster int, controllerPod, poolName, imageName string) []*ceph.RBDLock {
	GinkgoHelper()
	stdout, stderr, err := Kubectl(cluster, nil, "exec", "-n", CephCluster1Namespace, controllerPod, "--",
		"rbd", "-p", poolName, "--format", "json", "lock", "ls", imageName)
	Expect(err).NotTo(HaveOccurred(), "stderr: %s", string(stderr))
	var locks []*ceph.RBDLock
	Expect(json.Unmarshal(stdout, &locks)).To(Succeed())

	return locks
}
