package replication

import (
	"github.com/cybozu-go/mantle/internal/controller"
	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func testChangeToSecondary() {
	It("should behave correctly if role is changed from standalone to secondary",
		testRoleChangeFromStandaloneToSecondary)
}

func testRoleChangeFromStandaloneToSecondary(ctx SpecContext) {
	var (
		namespace                                                                  string
		pvcName0, backupName00, writtenDataHash00                                  string
		pvcName1, backupName10, writtenDataHash10                                  string
		pvcName2, backupName20, backupName21, writtenDataHash20, writtenDataHash21 string
	)

	/*
		Overview of the test:

		 primary k8s cluster       | secondary k8s cluster
		===========================|==========================
		  role=primary             | role=secondary
		  PVC0, MB00 (created)     |
		                           | PVC0, MB00 (synced)
		                           | role=standalone (changed)
		                           | PVC1, MB10 (created)
		  (MB10 don't exist)       |
		                           | role=secondary (changed)
		  PVC2, MB20 (created)     |
		                           | PVC2, MB20 (synced)
		  MB21 (created)           |
		                           | MB21 (synced)
		  (MB10 don't exist)       |
	*/

	By("setting up the environment", func() {
		namespace = util.GetUniqueName("ns-")
		pvcName0 = util.GetUniqueName("pvc-")
		backupName00 = util.GetUniqueName("mb-")
		pvcName1 = util.GetUniqueName("pvc-")
		backupName10 = util.GetUniqueName("mb-")
		pvcName2 = util.GetUniqueName("pvc-")
		backupName20 = util.GetUniqueName("mb-")
		backupName21 = util.GetUniqueName("mb-")

		SetupEnvironment(namespace)
	})

	By("creating and restoring a MantleBackup resource", func() {
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName0)

		writtenDataHash00 = WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName0)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName0, backupName00)
		WaitMantleBackupSynced(namespace, backupName00)

		restoreName00 := util.GetUniqueName("mr-")
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName00, restoreName00, writtenDataHash00)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName00, restoreName00, writtenDataHash00)
	})

	By("changing the role from secondary to standalone", func() {
		By("changing the secondary mantle to standalone")
		err := ChangeClusterRole(SecondaryK8sCluster, controller.RoleStandalone)
		Expect(err).NotTo(HaveOccurred())
	})

	By("restoring the synced MantleBackup in the both clusters", func() {
		restoreName00 := util.GetUniqueName("mr-")
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName00, restoreName00, writtenDataHash00)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName00, restoreName00, writtenDataHash00)
	})

	By("creating a MantleBackup resource in the secondary k8s cluster", func() {
		CreatePVC(ctx, SecondaryK8sCluster, namespace, pvcName1)
		writtenDataHash10 = WriteRandomDataToPV(ctx, SecondaryK8sCluster, namespace, pvcName1)
		CreateMantleBackup(SecondaryK8sCluster, namespace, pvcName1, backupName10)
		WaitMantleBackupReadyToUse(SecondaryK8sCluster, namespace, backupName10)
	})

	By("ensuring the MantleBackup created by standalone mantle doesn't exist in the primary k8s cluster",
		func() {
			EnsureMantleBackupNotExist(ctx, PrimaryK8sCluster, namespace, backupName10)
		})

	By("changing the role from standalone to secondary", func() {
		err := ChangeClusterRole(SecondaryK8sCluster, controller.RoleSecondary)
		Expect(err).NotTo(HaveOccurred())
	})

	By("creating and synchronizing new MantleBackup resources", func() {
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName2)

		writtenDataHash20 = WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName2)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName2, backupName20)
		WaitMantleBackupSynced(namespace, backupName20)

		writtenDataHash21 = WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName2)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName2, backupName21)
		WaitMantleBackupSynced(namespace, backupName21)
	})

	By("restoring MantleBackups correctly", func() {
		restoreName00 := util.GetUniqueName("mr-")
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName00, restoreName00, writtenDataHash00)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName00, restoreName00, writtenDataHash00)

		restoreName10 := util.GetUniqueName("mr-")
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName10, restoreName10, writtenDataHash10)

		restoreName20 := util.GetUniqueName("mr-")
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName20, restoreName20, writtenDataHash20)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName20, restoreName20, writtenDataHash20)

		restoreName21 := util.GetUniqueName("mr-")
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName21, restoreName21, writtenDataHash21)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName21, restoreName21, writtenDataHash21)
	})

	By("ensuring the MantleBackup created by standalone mantle doesn't exist in the primary k8s cluster",
		func() {
			EnsureMantleBackupNotExist(ctx, PrimaryK8sCluster, namespace, backupName10)
		})
}
