package multik8s

import (
	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("compression", Label("compression"), func() {
	It("should import zstd-compressed export data on the secondary", func(ctx SpecContext) {
		namespace := util.GetUniqueName("ns-")
		pvcName := util.GetUniqueName("pvc-")
		backupName := util.GetUniqueName("mb-")
		restoreName := util.GetUniqueName("mr-")

		SetupNamespaces(namespace)
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName, SCName1)
		writtenDataHash := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)

		By("creating a MantleBackup with zstd transfer compression")
		err := ApplyMantleBackupTemplate(
			PrimaryK8sCluster,
			namespace,
			pvcName,
			backupName,
			"zstd",
		)
		Expect(err).NotTo(HaveOccurred())

		WaitMantleBackupSynced(namespace, backupName)
		primaryBackup, err := GetMB(PrimaryK8sCluster, namespace, backupName)
		Expect(err).NotTo(HaveOccurred())
		secondaryBackup, err := GetMB(SecondaryK8sCluster, namespace, backupName)
		Expect(err).NotTo(HaveOccurred())
		Expect(primaryBackup.Spec.TransferCompression).To(Equal("zstd"))
		Expect(secondaryBackup.Spec.TransferCompression).To(Equal("zstd"))
		Expect(secondaryBackup.IsSnapshotCaptured()).To(BeTrue())

		By("restoring the imported data on the secondary")
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName, restoreName, writtenDataHash)
	})
})
