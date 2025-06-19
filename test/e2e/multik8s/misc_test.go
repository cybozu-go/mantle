package multik8s

import (
	"fmt"
	"strings"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller"
	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
)

var _ = Describe("metrics tests", func() {
	namespace := util.GetUniqueName("ns-")
	pvcName := util.GetUniqueName("pvc-")
	backupName := util.GetUniqueName("mb-")
	backupConfigName := util.GetUniqueName("mbc-")

	It("should setup", func(ctx SpecContext) {
		SetupEnvironment(namespace)
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName)
		WaitMantleBackupSynced(namespace, backupName)
		CreateMantleBackupConfig(PrimaryK8sCluster, namespace, pvcName, backupConfigName)
	})

	DescribeTable("metrics should be exposed",
		func(ctx SpecContext, metricName string) {
			Eventually(ctx, func(g Gomega) {
				controllerPod, err := GetControllerPodName(PrimaryK8sCluster)
				g.Expect(err).NotTo(HaveOccurred())
				stdout, _, err := Kubectl(PrimaryK8sCluster, nil, "exec", "-n", CephClusterNamespace, controllerPod, "--",
					"curl", "-s", "http://localhost:8080/metrics")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.Contains(string(stdout), metricName)).To(BeTrue())
			}).Should(Succeed())
		},
		Entry(`mantle_backup_duration_seconds_total`, `mantle_backup_duration_seconds_total`),
		Entry(`mantle_mantlebackupconfig_info`, `mantle_mantlebackupconfig_info`),
		Entry(`mantle_backup_duration_seconds_bucket`, `mantle_backup_duration_seconds_bucket`),
		Entry(`mantle_backup_duration_seconds_sum`, `mantle_backup_duration_seconds_sum`),
		Entry(`mantle_backup_duration_seconds_count`, `mantle_backup_duration_seconds_count`),
	)
})

var _ = Describe("miscellaneous tests", func() {
	It("should succeed to back up if backup-transfer-part-size is changed during uploading", func(ctx SpecContext) {
		namespace := util.GetUniqueName("ns-")
		pvcName := util.GetUniqueName("pvc-")
		backupName := util.GetUniqueName("mb-")
		restoreName := util.GetUniqueName("mr-")

		SetupEnvironment(namespace)

		// Pause the object storage to make upload Jobs fail.
		PauseObjectStorage(ctx)
		defer ResumeObjectStorage(ctx)

		// Create MantleBackup M0.
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)
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

	It("should succeed to back up two MantleBackups in parallel", func(ctx SpecContext) {
		namespace := util.GetUniqueName("ns-")
		pvcName1 := util.GetUniqueName("pvc-")
		pvcName2 := util.GetUniqueName("pvc-")
		backupName1 := util.GetUniqueName("mb-")
		backupName2 := util.GetUniqueName("mb-")
		restoreName1 := util.GetUniqueName("mr-")
		restoreName2 := util.GetUniqueName("mr-")

		SetupEnvironment(namespace)

		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName1)
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName2)
		writtenDataHash1 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName1)
		writtenDataHash2 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName2)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName1, backupName1)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName2, backupName2)
		WaitMantleBackupSynced(namespace, backupName1)
		WaitMantleBackupSynced(namespace, backupName2)

		primaryMB1, err := GetMB(PrimaryK8sCluster, namespace, backupName1)
		Expect(err).NotTo(HaveOccurred())
		secondaryMB1, err := GetMB(SecondaryK8sCluster, namespace, backupName1)
		Expect(err).NotTo(HaveOccurred())
		WaitTemporaryResourcesDeleted(ctx, primaryMB1, secondaryMB1)

		primaryMB2, err := GetMB(PrimaryK8sCluster, namespace, backupName2)
		Expect(err).NotTo(HaveOccurred())
		secondaryMB2, err := GetMB(SecondaryK8sCluster, namespace, backupName2)
		Expect(err).NotTo(HaveOccurred())
		WaitTemporaryResourcesDeleted(ctx, primaryMB2, secondaryMB2)

		Expect(secondaryMB1.IsReady()).To(BeTrue())
		Expect(secondaryMB2.IsReady()).To(BeTrue())

		snap, err := FindRBDSnapshotInPVC(SecondaryK8sCluster, namespace, pvcName1, backupName1)
		Expect(err).NotTo(HaveOccurred())
		Expect(secondaryMB1.Status.SnapID).NotTo(BeNil())
		Expect(*secondaryMB1.Status.SnapID).To(Equal(snap.Id))
		Expect(snap.Name).To(Equal(secondaryMB1.Name))

		snap, err = FindRBDSnapshotInPVC(SecondaryK8sCluster, namespace, pvcName2, backupName2)
		Expect(err).NotTo(HaveOccurred())
		Expect(secondaryMB2.Status.SnapID).NotTo(BeNil())
		Expect(*secondaryMB2.Status.SnapID).To(Equal(snap.Id))
		Expect(snap.Name).To(Equal(secondaryMB2.Name))

		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName2, restoreName2, writtenDataHash2)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName2, restoreName2, writtenDataHash2)
	})

	It("should succeed in backup even if part=0 upload Job completes after part=1 upload Job does",
		func(ctx SpecContext) {
			namespace := util.GetUniqueName("ns-")
			pvcName := util.GetUniqueName("pvc-")
			backupName := util.GetUniqueName("mb-")
			restoreName := util.GetUniqueName("mr-")
			partNumSlow := 0
			partNumFast := 1

			SetupEnvironment(namespace)

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

			CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)
			writtenDataHash := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName)

			By(fmt.Sprintf("waiting the situation where part=%d upload Job has not yet completed but "+
				"part=%d upload Job has already completed", partNumSlow, partNumFast))
			Eventually(ctx, func(g Gomega) {
				primaryMB, err := GetMB(PrimaryK8sCluster, namespace, backupName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(primaryMB.IsSynced()).To(BeFalse())

				jobSlow, err := GetJob(PrimaryK8sCluster, CephClusterNamespace,
					controller.MakeUploadJobName(primaryMB, partNumSlow))
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(IsJobConditionTrue(jobSlow.Status.Conditions, batchv1.JobComplete)).To(BeFalse())

				jobFast, err := GetJob(PrimaryK8sCluster, CephClusterNamespace,
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

	DescribeTable("Backups should succeed even if component Jobs temporarily fail",
		func(
			ctx SpecContext,
			clusterOfJob int,
			envName,
			originalScript string,
			makeJobName func(backup *mantlev1.MantleBackup, partNum int) string,
			extractPartNumFromJobName func(jobName string, backup *mantlev1.MantleBackup) (int, bool),

			// 0: reset the Job script to the original one.
			// 1: create an empty file named /mantle/ready-to-succeed.
			howJobRevived int,
		) {
			namespace := util.GetUniqueName("ns-")
			pvcName := util.GetUniqueName("pvc-")
			backupName := util.GetUniqueName("mb-")
			restoreName := util.GetUniqueName("mr-")
			partNumFailed := 1

			SetupEnvironment(namespace)

			// Make part=1 Job fail
			script := fmt.Sprintf(`#!/bin/bash
rbd_path=$(which rbd)
rbd(){
	if [ ! -f /mantle/ready-to-succeed -a ${PART_NUM} -eq %d ]; then
		return 1
	else
		${rbd_path} "$@"
	fi
}

s5cmd_path=$(which s5cmd)
s5cmd(){
	if [ ! -f /mantle/ready-to-succeed -a ${PART_NUM} -eq %d ]; then
		return 1
	else
		${s5cmd_path} "$@"
	fi
}
%s`, partNumFailed, partNumFailed, originalScript)
			ChangeComponentJobScript(ctx, clusterOfJob, envName, namespace, backupName, partNumFailed, &script)
			defer ChangeComponentJobScript(ctx, clusterOfJob, envName, namespace, backupName, partNumFailed, nil)

			CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)
			writtenDataHash := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName)

			By("waiting for the part=1 Job to fail")
			var backup *mantlev1.MantleBackup
			Eventually(ctx, func(g Gomega) {
				primaryMB, err := GetMB(PrimaryK8sCluster, namespace, backupName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(primaryMB.IsSynced()).To(BeFalse())

				backup, err = GetMB(clusterOfJob, namespace, backupName)
				g.Expect(err).NotTo(HaveOccurred())
				jobName := makeJobName(backup, partNumFailed)

				job, err := GetJob(clusterOfJob, CephClusterNamespace, jobName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(IsJobConditionTrue(job.Status.Conditions, batchv1.JobComplete)).To(BeFalse())
			}).Should(Succeed())

			By("ensuring the part=1 Job continues to fail")
			Consistently(ctx, func(g Gomega) {
				jobName := makeJobName(backup, partNumFailed)
				job, err := GetJob(clusterOfJob, CephClusterNamespace, jobName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(IsJobConditionTrue(job.Status.Conditions, batchv1.JobComplete)).To(BeFalse())
			}, "10s", "1s").Should(Succeed())

			// Make part=1 Job succeed
			switch howJobRevived {
			case 0: // reset the Job script to the original one.
				ChangeComponentJobScript(ctx, clusterOfJob, envName, namespace, backupName, partNumFailed, nil)
				_, _, err := Kubectl(clusterOfJob, nil, "delete", "-n", CephClusterNamespace,
					"job", makeJobName(backup, partNumFailed))
				Expect(err).NotTo(HaveOccurred())

			case 1: // create an empty file named /mantle/ready-to-succeed.
				By("creating a Job to create an empty file named /mantle/ready-to-succeed")
				jobName := util.GetUniqueName("job-")
				manifest := fmt.Sprintf(`
apiVersion: batch/v1
kind: Job
metadata:
  name: %s
  namespace: %s
spec:
  template:
    spec:
      containers:
      - name: ubuntu
        image: ubuntu:22.04
        command:
        - bash
        - -c
        - |
          touch /mantle/ready-to-succeed
        volumeMounts:
        - name: mantle
          mountPath: /mantle
      restartPolicy: Never
      volumes:
      - name: mantle
        persistentVolumeClaim:
          claimName: %s
`, jobName, CephClusterNamespace, controller.MakeExportDataPVCName(backup, partNumFailed))
				_, _, err := Kubectl(clusterOfJob, []byte(manifest), "apply", "-f", "-")
				Expect(err).NotTo(HaveOccurred())

				By("waiting for the Job to be completed")
				Eventually(ctx, func(g Gomega) {
					job, err := GetJob(clusterOfJob, CephClusterNamespace, jobName)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(IsJobConditionTrue(job.Status.Conditions, batchv1.JobComplete)).To(BeTrue())
				}).Should(Succeed())

				By("deleting the Job")
				_, _, err = Kubectl(clusterOfJob, nil, "delete", "-n", CephClusterNamespace, "job", jobName)
				Expect(err).NotTo(HaveOccurred())
			}

			WaitMantleBackupSynced(namespace, backupName)

			primaryMB, err := GetMB(PrimaryK8sCluster, namespace, backupName)
			Expect(err).NotTo(HaveOccurred())
			secondaryMB, err := GetMB(SecondaryK8sCluster, namespace, backupName)
			Expect(err).NotTo(HaveOccurred())
			WaitTemporaryResourcesDeleted(ctx, primaryMB, secondaryMB)

			EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName, restoreName, writtenDataHash)
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName, restoreName, writtenDataHash)
		},

		Entry(
			"export Job",
			PrimaryK8sCluster,
			controller.EnvExportJobScript,
			controller.EmbedJobExportScript,
			controller.MakeExportJobName,
			controller.ExtractPartNumFromExportJobName,
			0, // reset an export Job to the original one to revive the Job.
		),

		Entry(
			"upload Job",
			PrimaryK8sCluster,
			controller.EnvUploadJobScript,
			controller.EmbedJobUploadScript,
			controller.MakeUploadJobName,
			controller.ExtractPartNumFromUploadJobName,
			1, // create an empty file named /mantle/ready-to-succeed to revive the Job.
		),

		Entry(
			"import Job",
			SecondaryK8sCluster,
			controller.EnvImportJobScript,
			controller.EmbedJobImportScript,
			controller.MakeImportJobName,
			controller.ExtractPartNumFromImportJobName,
			0, // reset an import Job to the original one to revive the Job.
		),
	)
})
