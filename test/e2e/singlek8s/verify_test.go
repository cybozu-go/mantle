package singlek8s

import (
	"encoding/json"
	"fmt"
	"slices"

	"github.com/cybozu-go/mantle/internal/controller"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

type verifyTest struct {
	poolName         string
	storageClassName string
	tenantNamespace  string
}

func verifyTestSuite() {
	test := &verifyTest{}

	Describe("verification", func() {
		BeforeEach(test.setupEnv)
		It("should handle verification failure correctly", test.testVerificationFailure)
		It("should handle skip verification correctly", test.testVerificationSkip)
	})
}

func (test *verifyTest) setupEnv() {
	test.poolName = util.GetUniqueName("pool-")
	test.storageClassName = util.GetUniqueName("sc-")
	test.tenantNamespace = util.GetUniqueName("ns-")

	By("setting up the test environment", func() {
		_, _ = fmt.Fprintf(GinkgoWriter, "%+v\n", *test)
	})

	By("creating common resources", func() {
		err := createNamespace(test.tenantNamespace)
		Expect(err).NotTo(HaveOccurred())

		err = applyRBDPoolAndSCTemplate(cephCluster1Namespace, test.poolName, test.storageClassName)
		Expect(err).NotTo(HaveOccurred())
	})
}

func (test *verifyTest) testVerificationFailure(ctx SpecContext) {
	pvcName := util.GetUniqueName("test-pvc-")
	mantleBackupName := util.GetUniqueName("mantle-backup-")
	mantleRestoreName := util.GetUniqueName("mantle-restore-")

	By(fmt.Sprintf("Creating PVC, PV and RBD image (%s)", pvcName))
	err := applyPVCTemplate(test.tenantNamespace, pvcName, test.storageClassName)
	Expect(err).NotTo(HaveOccurred())

	By("creating a MantleBackup that fails in verification")
	err = applyMantleBackupTemplate(test.tenantNamespace, pvcName, mantleBackupName)
	Expect(err).NotTo(HaveOccurred())

	By("creating a MantleRestore that uses the above MantleBackup and never becomes ready")
	err = applyMantleRestoreTemplate(test.tenantNamespace, mantleRestoreName, mantleBackupName)
	Expect(err).NotTo(HaveOccurred())

	By("confirming that the MantleBackup fails in verification")
	Eventually(ctx, func(g Gomega) {
		// Get MB
		mb, err := getMB(test.tenantNamespace, mantleBackupName)
		Expect(err).NotTo(HaveOccurred())

		// List verify Pods
		verifyJobName := controller.MakeVerifyJobName(mb)
		stdout, _, err := kubectl("get", "pod", "-n", cephCluster1Namespace, "-l", "job-name="+verifyJobName, "-o", "json")
		g.Expect(err).NotTo(HaveOccurred())
		podList := corev1.PodList{}
		err = json.Unmarshal(stdout, &podList)
		g.Expect(err).NotTo(HaveOccurred())

		// Find a Pod with Failed phase
		index := slices.IndexFunc(podList.Items, func(p corev1.Pod) bool {
			return p.Status.Phase == corev1.PodFailed
		})
		g.Expect(index).NotTo(Equal(-1))
	}).Should(Succeed())

	By("confirming that the MantleBackup doesn't have Verified condition")
	mb, err := getMB(test.tenantNamespace, mantleBackupName)
	Expect(err).NotTo(HaveOccurred())
	Expect(mb.IsVerified()).To(BeFalse())

	By("confirming that the MantleRestore never becomes ready")
	Expect(isMantleRestoreReady(test.tenantNamespace, mantleRestoreName)).To(BeFalse())
}

func (test *verifyTest) testVerificationSkip(ctx SpecContext) {
	pvcName := util.GetUniqueName("test-pvc-")
	mantleBackupName := util.GetUniqueName("mantle-backup-")
	mantleRestoreName := util.GetUniqueName("mantle-restore-")

	By(fmt.Sprintf("Creating PVC, PV and RBD image (%s)", pvcName))
	err := applyPVCTemplate(test.tenantNamespace, pvcName, test.storageClassName)
	Expect(err).NotTo(HaveOccurred())

	By("creating a MantleBackup that fails in verification")
	err = applyMantleBackupTemplate(test.tenantNamespace, pvcName, mantleBackupName)
	Expect(err).NotTo(HaveOccurred())

	By("annotating the MantleBackup to skip verification")
	_, _, err = kubectl("annotate", "mb", "-n", test.tenantNamespace, mantleBackupName,
		"mantle.cybozu.io/skip-verify=true")
	Expect(err).NotTo(HaveOccurred())

	By("creating a MantleRestore that uses the above MantleBackup")
	err = applyMantleRestoreTemplate(test.tenantNamespace, mantleRestoreName, mantleBackupName)
	Expect(err).NotTo(HaveOccurred())

	By("confirming that the MantleRestore becomes ready")
	Eventually(ctx, func(g Gomega) {
		g.Expect(isMantleRestoreReady(test.tenantNamespace, mantleRestoreName)).To(BeTrue())
	}).Should(Succeed())
}
