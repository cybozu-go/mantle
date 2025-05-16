package multik8s

import (
	"slices"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("incremental backup", Label("incr-backup"), func() {
	It("should perform a correct incremental backup", func(ctx SpecContext) {
		namespace := util.GetUniqueName("ns-")
		pvcName := util.GetUniqueName("pvc-")
		backupName0 := util.GetUniqueName("mb-")
		backupName1 := util.GetUniqueName("mb-")
		restoreName0 := util.GetUniqueName("mr-")
		restoreName1 := util.GetUniqueName("mr-")

		SetupEnvironment(namespace)
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)

		// create M0.
		writtenDataHash0 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName0)
		WaitMantleBackupSynced(namespace, backupName0)

		primaryMB0, err := GetMB(PrimaryK8sCluster, namespace, backupName0)
		Expect(err).NotTo(HaveOccurred())
		secondaryMB0, err := GetMB(SecondaryK8sCluster, namespace, backupName0)
		Expect(err).NotTo(HaveOccurred())
		WaitTemporaryResourcesDeleted(ctx, primaryMB0, secondaryMB0)

		// create M1.
		writtenDataHash1 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName1)
		WaitMantleBackupSynced(namespace, backupName1)

		primaryMB1, err := GetMB(PrimaryK8sCluster, namespace, backupName1)
		Expect(err).NotTo(HaveOccurred())
		secondaryMB1, err := GetMB(SecondaryK8sCluster, namespace, backupName1)
		Expect(err).NotTo(HaveOccurred())
		WaitTemporaryResourcesDeleted(ctx, primaryMB1, secondaryMB1)

		// Make sure M1 and M1' have the same contents.
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)

		// Make sure M0 and M0' have the same contents.
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
	})

	It("should back up correctly if previous incremental MB is removed in the secondary cluster", func(ctx SpecContext) {
		namespace := util.GetUniqueName("ns-")
		pvcName := util.GetUniqueName("pvc-")
		backupName0 := util.GetUniqueName("mb-")
		backupName1 := util.GetUniqueName("mb-")
		backupName2 := util.GetUniqueName("mb-")
		restoreName0 := util.GetUniqueName("mr-")
		restoreName1 := util.GetUniqueName("mr-")
		restoreName2 := util.GetUniqueName("mr-")

		SetupEnvironment(namespace)
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)

		// create M0.
		writtenDataHash0 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName0)
		WaitMantleBackupSynced(namespace, backupName0)

		// create M1.
		writtenDataHash1 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName1)
		WaitMantleBackupSynced(namespace, backupName1)

		// remove M1'.
		_, _, err := Kubectl(SecondaryK8sCluster, nil, "delete", "mb", "-n", namespace, backupName1)
		Expect(err).NotTo(HaveOccurred())

		// create M2.
		writtenDataHash2 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName2)
		WaitMantleBackupSynced(namespace, backupName2)

		primaryMB2, err := GetMB(PrimaryK8sCluster, namespace, backupName2)
		Expect(err).NotTo(HaveOccurred())
		secondaryMB2, err := GetMB(SecondaryK8sCluster, namespace, backupName2)
		Expect(err).NotTo(HaveOccurred())
		WaitTemporaryResourcesDeleted(ctx, primaryMB2, secondaryMB2)

		// Make sure M2 and M2' have the same contents.
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName2, restoreName2, writtenDataHash2)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName2, restoreName2, writtenDataHash2)

		// Make sure M1 has the same contents.
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)

		// Make sure M0 and M0' have the same contents.
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)

		// Make sure M1' isn't re-created.
		mbList, err := GetObjectList[mantlev1.MantleBackupList](SecondaryK8sCluster, "mb", namespace)
		Expect(err).NotTo(HaveOccurred())
		Expect(slices.ContainsFunc(mbList.Items, func(mb mantlev1.MantleBackup) bool {
			return mb.GetName() == backupName1
		})).To(BeFalse())
	})

	It("should back up correctly if previous incremental MB is removed in the primary cluster", func(ctx SpecContext) {
		namespace := util.GetUniqueName("ns-")
		pvcName := util.GetUniqueName("pvc-")
		backupName0 := util.GetUniqueName("mb-")
		backupName1 := util.GetUniqueName("mb-")
		backupName2 := util.GetUniqueName("mb-")
		restoreName0 := util.GetUniqueName("mr-")
		restoreName1 := util.GetUniqueName("mr-")
		restoreName2 := util.GetUniqueName("mr-")

		SetupEnvironment(namespace)
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)

		// create M0.
		writtenDataHash0 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName0)
		WaitMantleBackupSynced(namespace, backupName0)

		// create M1.
		writtenDataHash1 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName1)
		WaitMantleBackupSynced(namespace, backupName1)

		// remove M1.
		_, _, err := Kubectl(PrimaryK8sCluster, nil, "delete", "mb", "-n", namespace, backupName1)
		Expect(err).NotTo(HaveOccurred())

		// create M2.
		writtenDataHash2 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName2)
		WaitMantleBackupSynced(namespace, backupName2)

		primaryMB2, err := GetMB(PrimaryK8sCluster, namespace, backupName2)
		Expect(err).NotTo(HaveOccurred())
		secondaryMB2, err := GetMB(SecondaryK8sCluster, namespace, backupName2)
		Expect(err).NotTo(HaveOccurred())
		WaitTemporaryResourcesDeleted(ctx, primaryMB2, secondaryMB2)

		// Make sure M2 and M2' have the same contents.
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName2, restoreName2, writtenDataHash2)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName2, restoreName2, writtenDataHash2)

		// Make sure M1' has the same contents.
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)

		// Make sure M0 and M0' have the same contents.
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
	})
})
