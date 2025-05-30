package multik8s

import (
	"encoding/json"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller"
	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("change to standalone", Label("change-to-standalone"), func() {
	var namespace, pvcName, backupName string

	It("should replicate a MantleBackup resource", func(ctx SpecContext) {
		namespace = util.GetUniqueName("ns-")
		pvcName = util.GetUniqueName("pvc-")
		backupName = util.GetUniqueName("mb-")

		SetupEnvironment(namespace)
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName)
		WaitMantleBackupSynced(namespace, backupName)
	})

	It("should change the roles to standalone", func() {
		By("changing the primary mantle to standalone")
		err := ChangeClusterRole(PrimaryK8sCluster, controller.RoleStandalone)
		Expect(err).NotTo(HaveOccurred())
		By("changing the secondary mantle to standalone")
		err = ChangeClusterRole(SecondaryK8sCluster, controller.RoleStandalone)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should delete MantleBackup created by primary mantle from standalone mantle", func(ctx SpecContext) {
		By("deleting the MantleBackup in the primary cluster")
		_, _, err := Kubectl(PrimaryK8sCluster, nil, "delete", "mb", "-n", namespace, backupName, "--wait=false")
		Expect(err).NotTo(HaveOccurred())

		By("checking that the MantleBackup is actually deleted")
		Eventually(ctx, func(g Gomega) {
			stdout, _, err := Kubectl(PrimaryK8sCluster, nil, "get", "mb", "-n", namespace, "-o", "json")
			g.Expect(err).NotTo(HaveOccurred())
			var mbs mantlev1.MantleBackupList
			err = json.Unmarshal(stdout, &mbs)
			g.Expect(err).NotTo(HaveOccurred())
			found := false
			for _, mb := range mbs.Items {
				if mb.GetName() == backupName {
					found = true
				}
			}
			g.Expect(found).To(BeFalse())
		}).Should(Succeed())
	})

	It("should NOT delete MantleBackup created by secondary mantle from standalone mantle", func(ctx SpecContext) {
		By("deleting the MantleBackup in the secondary cluster")
		_, _, err := Kubectl(SecondaryK8sCluster, nil, "delete", "mb", "-n", namespace, backupName, "--wait=false")
		Expect(err).NotTo(HaveOccurred())

		By("checking that the MantleBackup is NOT deleted")
		Consistently(ctx, func(g Gomega) {
			stdout, _, err := Kubectl(SecondaryK8sCluster, nil, "get", "mb", "-n", namespace, "-o", "json")
			g.Expect(err).NotTo(HaveOccurred())
			var mbs mantlev1.MantleBackupList
			err = json.Unmarshal(stdout, &mbs)
			g.Expect(err).NotTo(HaveOccurred())
			found := false
			for _, mb := range mbs.Items {
				if mb.GetName() == backupName {
					found = true
				}
			}
			g.Expect(found).To(BeTrue())
		}, "10s", "1s").Should(Succeed())
	})

	It("should change their roles back to primary/secondary", func() {
		By("reverting the standalone mantle back to primary in the primary K8s cluster")
		err := ChangeClusterRole(PrimaryK8sCluster, controller.RolePrimary)
		Expect(err).NotTo(HaveOccurred())
		By("reverting the standalone mantle back to secondary in the secondary K8s cluster")
		err = ChangeClusterRole(SecondaryK8sCluster, controller.RoleSecondary)
		Expect(err).NotTo(HaveOccurred())
	})
})
