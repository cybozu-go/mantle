package multik8s

import (
	"fmt"

	"github.com/cybozu-go/mantle/internal/controller"
	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

// nodeAffinityForHostname returns an affinity that requires Pods to be
// scheduled onto the node named hostname.
func nodeAffinityForHostname(hostname string) *corev1.Affinity {
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      "kubernetes.io/hostname",
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{hostname},
							},
						},
					},
				},
			},
		},
	}
}

// getSingleNodeName returns the name of the only node of the given cluster.
func getSingleNodeName(clusterNo int) string {
	GinkgoHelper()

	nodes, err := GetNodeList(clusterNo)
	Expect(err).NotTo(HaveOccurred())
	Expect(nodes.Items).To(HaveLen(1))

	return nodes.Items[0].GetName()
}

var _ = Describe("Job affinity tests", func() {
	It("should set the specified affinity to the export, upload, and import Jobs", func(ctx SpecContext) {
		namespace := util.GetUniqueName("ns-")
		pvcName := util.GetUniqueName("pvc-")
		backupName := util.GetUniqueName("mb-")
		restoreName := util.GetUniqueName("mr-")
		partNumSlow := 0

		SetupNamespaces(namespace)

		// The affinity must be satisfiable, otherwise the Pods of the Jobs stay
		// Pending and the backup never completes. Both clusters consist of a
		// single node, so pin the Pods to it.
		primaryAffinity := nodeAffinityForHostname(getSingleNodeName(PrimaryK8sCluster))
		secondaryAffinity := nodeAffinityForHostname(getSingleNodeName(SecondaryK8sCluster))

		ChangeJobAffinity(PrimaryK8sCluster, "--export-job-affinity", primaryAffinity)
		defer ChangeJobAffinity(PrimaryK8sCluster, "--export-job-affinity", nil)
		ChangeJobAffinity(PrimaryK8sCluster, "--upload-job-affinity", primaryAffinity)
		defer ChangeJobAffinity(PrimaryK8sCluster, "--upload-job-affinity", nil)
		ChangeJobAffinity(SecondaryK8sCluster, "--import-job-affinity", secondaryAffinity)
		defer ChangeJobAffinity(SecondaryK8sCluster, "--import-job-affinity", nil)

		// Make the import Job of part=0 sleep, so that the export, upload, and
		// import Jobs are all still alive while they are inspected below. The
		// Jobs are deleted only after the MantleBackup is synced.
		script := fmt.Sprintf(`#!/bin/bash
if [ ${PART_NUM} -eq %d ]; then
	sleep 60
fi
%s`, partNumSlow, controller.EmbedJobImportScript)
		ChangeComponentJobScript(
			ctx, SecondaryK8sCluster, controller.EnvImportJobScript, namespace, backupName, partNumSlow, &script)
		defer ChangeComponentJobScript(
			ctx, SecondaryK8sCluster, controller.EnvImportJobScript, namespace, backupName, partNumSlow, nil)

		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName, SCName1)
		writtenDataHash := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName)

		By("checking the affinity of the export and upload Jobs")
		WaitUploadJobCreated(ctx, PrimaryK8sCluster, namespace, backupName, partNumSlow)
		primaryMB, err := GetMB(PrimaryK8sCluster, namespace, backupName)
		Expect(err).NotTo(HaveOccurred())
		exportJob, err := GetJob(PrimaryK8sCluster, CephCluster1Namespace,
			controller.MakeExportJobName(primaryMB, partNumSlow))
		Expect(err).NotTo(HaveOccurred())
		Expect(exportJob.Spec.Template.Spec.Affinity).To(Equal(primaryAffinity))
		uploadJob, err := GetJob(PrimaryK8sCluster, CephCluster1Namespace,
			controller.MakeUploadJobName(primaryMB, partNumSlow))
		Expect(err).NotTo(HaveOccurred())
		Expect(uploadJob.Spec.Template.Spec.Affinity).To(Equal(primaryAffinity))

		By("checking the affinity of the import Job")
		Eventually(ctx, func(g Gomega) {
			secondaryMB, err := GetMB(SecondaryK8sCluster, namespace, backupName)
			g.Expect(err).NotTo(HaveOccurred())
			importJob, err := GetJob(SecondaryK8sCluster, CephCluster1Namespace,
				controller.MakeImportJobName(secondaryMB, partNumSlow))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(importJob.Spec.Template.Spec.Affinity).To(Equal(secondaryAffinity))
		}).Should(Succeed())

		By("making sure the backup completes even if the affinity is set")
		WaitMantleBackupSynced(namespace, backupName)
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName, restoreName, writtenDataHash)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName, restoreName, writtenDataHash)
	})
})
