package singlek8s

import (
	"encoding/json"
	"fmt"
	"slices"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
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
	// Get MB
	mb, err := getMB(test.tenantNamespace, mantleBackupName)
	Expect(err).NotTo(HaveOccurred())
	Eventually(ctx, func(g Gomega) {
		// List verify Pods
		verifyJobName := controller.MakeVerifyJobName(mb)
		stdout, _, err := kubectl("get", "pod", "-n", cephCluster1Namespace,
			"-l", "batch.kubernetes.io/job-name="+verifyJobName, "-o", "json")
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

	By("confirming that the MantleBackup has Verified=False condition eventually")
	Eventually(ctx, func(g Gomega) {
		mb, err := getMB(test.tenantNamespace, mantleBackupName)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(mb.IsVerified()).To(BeFalse())
		g.Expect(meta.IsStatusConditionFalse(mb.Status.Conditions, mantlev1.BackupConditionVerified)).To(BeTrue())
	}).Should(Succeed())

	By("confirming that the number of the verify Pods is equal to or less than 1", func() {
		verifyJobName := controller.MakeVerifyJobName(mb)
		stdout, _, err := kubectl("get", "pod", "-n", cephCluster1Namespace,
			"-l", "batch.kubernetes.io/job-name="+verifyJobName, "-o", "json")
		Expect(err).NotTo(HaveOccurred())
		podList := corev1.PodList{}
		err = json.Unmarshal(stdout, &podList)
		Expect(err).NotTo(HaveOccurred())
		Expect(len(podList.Items)).To(BeNumerically("<=", 1))
	})

	By("confirming that the verify Job is deleted", func() {
		Eventually(ctx, func(g Gomega) {
			found, err := checkJobExists(cephCluster1Namespace, controller.MakeVerifyJobName(mb))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(found).To(BeFalse())
		}).Should(Succeed())
	})

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

	// We check that the verification indeed failed after we confirm the
	// MantleRestore is ready, so that we can test that the restoration process
	// can run concurrently with the verification process.
	By("confirming that the MantleBackup has Verified=False condition eventually")
	Eventually(ctx, func(g Gomega) {
		mb, err := getMB(test.tenantNamespace, mantleBackupName)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(mb.IsVerifiedFalse()).To(BeTrue())
	}).Should(Succeed())

	By("confirming that the verify Job is deleted eventually", func() {
		mb, err := getMB(test.tenantNamespace, mantleBackupName)
		Expect(err).NotTo(HaveOccurred())

		Eventually(ctx, func(g Gomega) {
			found, err := checkJobExists(cephCluster1Namespace, controller.MakeVerifyJobName(mb))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(found).To(BeFalse())
		}).Should(Succeed())
	})
}
