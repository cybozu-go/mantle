package multik8s

import (
	"fmt"

	"github.com/cybozu-go/mantle/internal/controller"
	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
)

var _ = Describe("multi-part backup", Label("multi-part"), func() {
	It("should succeed to back up if backup-transfer-part-size is changed during uploading", func(ctx SpecContext) {
		namespace := util.GetUniqueName("ns-")
		pvcName := util.GetUniqueName("pvc-")
		backupName := util.GetUniqueName("mb-")
		restoreName := util.GetUniqueName("mr-")

		SetupNamespaces(namespace)

		// Pause the object storage to make upload Jobs fail.
		PauseObjectStorage(ctx)
		defer ResumeObjectStorage(ctx)

		// Create MantleBackup M0.
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName, SCName1)
		writtenDataHash := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName)

		// Wait until an upload Job is created.
		WaitUploadJobCreated(ctx, PrimaryK8sCluster, namespace, backupName, 0)

		// Get the expected number of the backup parts before changing backup-transfer-part-size.
		pvc, err := GetPVC(PrimaryK8sCluster, namespace, pvcName)
		Expect(err).NotTo(HaveOccurred())
		numParts, err := GetNumberOfBackupParts(pvc.Spec.Resources.Requests.Storage())
		Expect(err).NotTo(HaveOccurred())

		// Change backup-transfer-part-size
		originalBackupTransferPartSize, err := GetBackupTransferPartSize()
		Expect(err).NotTo(HaveOccurred())
		Expect(originalBackupTransferPartSize.String()).To(Equal("3Mi"))
		ChangeBackupTransferPartSize("7Mi")
		defer ChangeBackupTransferPartSize(originalBackupTransferPartSize.String())
		newNumParts, err := GetNumberOfBackupParts(pvc.Spec.Resources.Requests.Storage())
		Expect(err).NotTo(HaveOccurred())
		Expect(newNumParts).NotTo(Equal(numParts))

		// Resume the process.
		ResumeObjectStorage(ctx)

		// Wait for MB to be synced.
		WaitMantleBackupSynced(namespace, backupName)

		// Make sure backups are correct.
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName, restoreName, writtenDataHash)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName, restoreName, writtenDataHash)
	})

	It("should complete a multi-part backup even when max-export-data-pvcs is 1", func(ctx SpecContext) {
		namespace := util.GetUniqueName("ns-")
		pvcName := util.GetUniqueName("pvc-")
		backupName := util.GetUniqueName("mb-")

		SetupNamespaces(namespace)

		maxExportDataPVCs := 1
		ChangeMaxExportDataPVCs(&maxExportDataPVCs)
		defer ChangeMaxExportDataPVCs(nil)

		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName, SCName1)
		_ = WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)

		// Make sure the backup will be split into multiple parts.
		pvc, err := GetPVC(PrimaryK8sCluster, namespace, pvcName)
		Expect(err).NotTo(HaveOccurred())
		numParts, err := GetNumberOfBackupParts(pvc.Spec.Resources.Requests.Storage())
		Expect(err).NotTo(HaveOccurred())
		Expect(numParts).To(BeNumerically(">", 1))

		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName)
		WaitMantleBackupSynced(namespace, backupName)

		primaryMB, err := GetMB(PrimaryK8sCluster, namespace, backupName)
		Expect(err).NotTo(HaveOccurred())
		secondaryMB, err := GetMB(SecondaryK8sCluster, namespace, backupName)
		Expect(err).NotTo(HaveOccurred())
		WaitTemporaryResourcesDeleted(ctx, primaryMB, secondaryMB)
	})

	It("should succeed in backup even if part=0 upload Job completes after part=1 upload Job does",
		func(ctx SpecContext) {
			namespace := util.GetUniqueName("ns-")
			pvcName := util.GetUniqueName("pvc-")
			backupName := util.GetUniqueName("mb-")
			restoreName := util.GetUniqueName("mr-")
			partNumSlow := 0
			partNumFast := 1

			SetupNamespaces(namespace)

			script := fmt.Sprintf(`#!/bin/bash
s5cmd_path=$(which s5cmd)
s5cmd(){
	if [ ${PART_NUM} -eq %d ]; then
		sleep 60
	fi
	${s5cmd_path} "$@"
}
%s`, partNumSlow, controller.EmbedJobUploadScript)
			ChangeComponentJobScript(
				ctx,
				PrimaryK8sCluster,
				controller.EnvUploadJobScript,
				namespace,
				backupName,
				partNumSlow,
				&script,
			)
			defer ChangeComponentJobScript(
				ctx,
				PrimaryK8sCluster,
				controller.EnvUploadJobScript,
				namespace,
				backupName,
				partNumSlow,
				nil,
			)

			CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName, SCName1)
			writtenDataHash := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName)

			By(fmt.Sprintf("waiting the situation where part=%d upload Job has not yet completed but "+
				"part=%d upload Job has already completed", partNumSlow, partNumFast))
			Eventually(ctx, func(g Gomega) {
				primaryMB, err := GetMB(PrimaryK8sCluster, namespace, backupName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(primaryMB.IsSynced()).To(BeFalse())

				jobSlow, err := GetJob(PrimaryK8sCluster, CephCluster1Namespace,
					controller.MakeUploadJobName(primaryMB, partNumSlow))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(IsJobConditionTrue(jobSlow.Status.Conditions, batchv1.JobComplete)).To(BeFalse())

				jobFast, err := GetJob(PrimaryK8sCluster, CephCluster1Namespace,
					controller.MakeUploadJobName(primaryMB, partNumFast))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(IsJobConditionTrue(jobFast.Status.Conditions, batchv1.JobComplete)).To(BeTrue())
			}).Should(Succeed())

			WaitMantleBackupSynced(namespace, backupName)

			primaryMB, err := GetMB(PrimaryK8sCluster, namespace, backupName)
			Expect(err).NotTo(HaveOccurred())
			secondaryMB, err := GetMB(SecondaryK8sCluster, namespace, backupName)
			Expect(err).NotTo(HaveOccurred())
			WaitTemporaryResourcesDeleted(ctx, primaryMB, secondaryMB)

			EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName, restoreName, writtenDataHash)
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName, restoreName, writtenDataHash)
		},
	)
})
