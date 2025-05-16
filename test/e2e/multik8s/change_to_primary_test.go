package multik8s

import (
	"github.com/cybozu-go/mantle/internal/controller"
	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("change role from primary to standalone during full backup", Label("change-to-primary"), func() {
	It("should cancel a full backup if the role is changed from primary to standalone", func(ctx SpecContext) {
		namespace := util.GetUniqueName("ns-")
		pvcName := util.GetUniqueName("pvc-")
		backupName := util.GetUniqueName("mb-")
		restoreName := util.GetUniqueName("mr-")

		SetupEnvironment(namespace)

		PauseObjectStorage(ctx)
		defer ResumeObjectStorage(ctx)

		By("should create a MantleBackup resource")
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)
		writtenDataHash := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName)
		WaitMantleBackupReadyToUse(PrimaryK8sCluster, namespace, backupName)

		By("changing the primary mantle to standalone")
		err := ChangeClusterRole(PrimaryK8sCluster, controller.RoleStandalone)
		Expect(err).NotTo(HaveOccurred())

		By("checking the MantleBackup in the primary K8s cluster remains")
		Consistently(ctx, func(g Gomega) {
			_, err := GetMB(PrimaryK8sCluster, namespace, backupName)
			g.Expect(err).NotTo(HaveOccurred())
		}, "10s", "1s").Should(Succeed())

		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName, restoreName, writtenDataHash)

		By("changing the standalone mantle to primary")
		err = ChangeClusterRole(PrimaryK8sCluster, controller.RolePrimary)
		Expect(err).NotTo(HaveOccurred())

		ResumeObjectStorage(ctx)
		WaitMantleBackupSynced(namespace, backupName)
	})
})

var _ = Describe("change to primary", Label("change-to-primary"), func() {
	var (
		namespace                                                                  string
		pvcName0, backupName00, backupName01, writtenDataHash00, writtenDataHash01 string
		pvcName1, backupName10, writtenDataHash10                                  string
	)

	/*
		Overview of the test:

		 primary k8s cluster        | secondary k8s cluster
		============================|==========================
		  role=primary              | role=secondary
		  PVC0, MB00 (created)      |
		                            | PVC0, MB00 (synced)
		  role=standalone (changed) |
		  MB01, PVC1, MB10 (created)|
		  role=primary (changed)    |
		                            | MB01, PVC1, MB10 (synced)
	*/

	It("should replicate a MantleBackup resource", func(ctx SpecContext) {
		namespace = util.GetUniqueName("ns-")
		pvcName0 = util.GetUniqueName("pvc-")
		backupName00 = util.GetUniqueName("mb-")

		SetupEnvironment(namespace)
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName0)
		writtenDataHash00 = WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName0)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName0, backupName00)
		WaitMantleBackupSynced(namespace, backupName00)
	})

	It("should change the role from primary to standalone", func() {
		By("changing the primary mantle to standalone")
		err := ChangeClusterRole(PrimaryK8sCluster, controller.RoleStandalone)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should restore the synced MantleBackup in the both clusters", func(ctx SpecContext) {
		restoreName00 := util.GetUniqueName("mr-")
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName00, restoreName00, writtenDataHash00)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName00, restoreName00, writtenDataHash00)
	})

	It("should create a MantleBackup resource", func(ctx SpecContext) {
		backupName01 = util.GetUniqueName("mb-")
		writtenDataHash01 = WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName0)

		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName0, backupName01)

		pvcName1 = util.GetUniqueName("pvc-")
		backupName10 = util.GetUniqueName("mb-")

		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName1)
		writtenDataHash10 = WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName1)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName1, backupName10)
	})

	It("should change the role from standalone to primary", func() {
		By("changing the standalone mantle to primary")
		err := ChangeClusterRole(PrimaryK8sCluster, controller.RolePrimary)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should synchronize MantleBackups correctly", func() {
		WaitMantleBackupSynced(namespace, backupName01)
		WaitMantleBackupSynced(namespace, backupName10)
	})

	It("should restore MantleBackups correctly", func(ctx SpecContext) {
		restoreName00 := util.GetUniqueName("mr-")
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName00, restoreName00, writtenDataHash00)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName00, restoreName00, writtenDataHash00)

		restoreName01 := util.GetUniqueName("mr-")
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName01, restoreName01, writtenDataHash01)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName01, restoreName01, writtenDataHash01)

		restoreName10 := util.GetUniqueName("mr-")
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName10, restoreName10, writtenDataHash10)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName10, restoreName10, writtenDataHash10)
	})
})
