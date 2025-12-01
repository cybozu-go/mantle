package multik8s

import (
	"github.com/cybozu-go/mantle/internal/controller"
	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("change to secondary", Label("change-to-secondary"), func() {
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

	It("should set up the environment", func(ctx SpecContext) {
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

	It("should create and restore a MantleBackup resource", func(ctx SpecContext) {
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName0)

		writtenDataHash00 = WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName0)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName0, backupName00)
		WaitMantleBackupSynced(namespace, backupName00)

		restoreName00 := util.GetUniqueName("mr-")
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName00, restoreName00, writtenDataHash00)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName00, restoreName00, writtenDataHash00)
	})

	It("should change the role from secondary to standalone", func() {
		By("changing the secondary mantle to standalone")
		err := ChangeClusterRole(SecondaryK8sCluster, controller.RoleStandalone)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should restore the synced MantleBackup in the both clusters", func(ctx SpecContext) {
		restoreName00 := util.GetUniqueName("mr-")
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName00, restoreName00, writtenDataHash00)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName00, restoreName00, writtenDataHash00)
	})

	It("should create a MantleBackup resource in the secondary k8s cluster", func(ctx SpecContext) {
		CreatePVC(ctx, SecondaryK8sCluster, namespace, pvcName1)
		writtenDataHash10 = WriteRandomDataToPV(ctx, SecondaryK8sCluster, namespace, pvcName1)
		CreateMantleBackup(SecondaryK8sCluster, namespace, pvcName1, backupName10)
		WaitMantleBackupSnapshotCaptured(SecondaryK8sCluster, namespace, backupName10)
		WaitMantleBackupVerified(SecondaryK8sCluster, namespace, backupName10)
	})

	It("should ensure the MantleBackup created by standalone mantle doesn't exist in the primary k8s cluster",
		func(ctx SpecContext) {
			EnsureMantleBackupNotExist(ctx, PrimaryK8sCluster, namespace, backupName10)
		})

	It("should change the role from standalone to secondary", func() {
		By("changing the standalone mantle to secondary")
		err := ChangeClusterRole(SecondaryK8sCluster, controller.RoleSecondary)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should create and synchronize new MantleBackup resources", func(ctx SpecContext) {
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName2)

		writtenDataHash20 = WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName2)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName2, backupName20)
		WaitMantleBackupSynced(namespace, backupName20)

		writtenDataHash21 = WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName2)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName2, backupName21)
		WaitMantleBackupSynced(namespace, backupName21)
	})

	It("should verify MantleBackups correctly", func(ctx SpecContext) {
		WaitMantleBackupVerified(PrimaryK8sCluster, namespace, backupName00)
		WaitMantleBackupVerified(SecondaryK8sCluster, namespace, backupName00)
		WaitMantleBackupVerified(PrimaryK8sCluster, namespace, backupName20)
		WaitMantleBackupVerified(SecondaryK8sCluster, namespace, backupName20)
		WaitMantleBackupVerified(PrimaryK8sCluster, namespace, backupName21)
		WaitMantleBackupVerified(SecondaryK8sCluster, namespace, backupName21)
	})

	It("should restore MantleBackups correctly", func(ctx SpecContext) {
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

	It("should ensure the MantleBackup created by standalone mantle doesn't exist in the primary k8s cluster",
		func(ctx SpecContext) {
			EnsureMantleBackupNotExist(ctx, PrimaryK8sCluster, namespace, backupName10)
		})
})
