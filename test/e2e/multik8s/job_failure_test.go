package multik8s

import (
	"fmt"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller"
	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
)

var _ = Describe("job failure", Label("job-failure"), func() {
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

			SetupNamespaces(namespace)

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

			CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName, SCName1)
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

				job, err := GetJob(clusterOfJob, CephCluster1Namespace, jobName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(IsJobConditionTrue(job.Status.Conditions, batchv1.JobComplete)).To(BeFalse())
			}).Should(Succeed())

			By("ensuring the part=1 Job continues to fail")
			Consistently(ctx, func(g Gomega) {
				jobName := makeJobName(backup, partNumFailed)
				job, err := GetJob(clusterOfJob, CephCluster1Namespace, jobName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(IsJobConditionTrue(job.Status.Conditions, batchv1.JobComplete)).To(BeFalse())
			}, "10s", "1s").Should(Succeed())

			// Make part=1 Job succeed
			switch howJobRevived {
			case 0: // reset the Job script to the original one.
				ChangeComponentJobScript(ctx, clusterOfJob, envName, namespace, backupName, partNumFailed, nil)
				_, _, err := Kubectl(clusterOfJob, nil, "delete", "-n", CephCluster1Namespace,
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
`, jobName, CephCluster1Namespace, controller.MakeExportDataPVCName(backup, partNumFailed))
				_, _, err := Kubectl(clusterOfJob, []byte(manifest), "apply", "-f", "-")
				Expect(err).NotTo(HaveOccurred())

				By("waiting for the Job to be completed")
				Eventually(ctx, func(g Gomega) {
					job, err := GetJob(clusterOfJob, CephCluster1Namespace, jobName)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(IsJobConditionTrue(job.Status.Conditions, batchv1.JobComplete)).To(BeTrue())
				}).Should(Succeed())

				By("deleting the Job")
				_, _, err = Kubectl(clusterOfJob, nil, "delete", "-n", CephCluster1Namespace, "job", jobName)
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
