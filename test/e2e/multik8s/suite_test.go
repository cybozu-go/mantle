package multik8s

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cybozu-go/mantle/internal/controller"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

func TestMtest(t *testing.T) {
	if os.Getenv("E2ETEST") == "" {
		t.Skip("Run under e2e/")
	}

	RegisterFailHandler(Fail)

	SetDefaultEventuallyPollingInterval(time.Second)
	SetDefaultEventuallyTimeout(3 * time.Minute)
	EnforceDefaultTimeoutsWhenUsingContexts()

	RunSpecs(t, "rbd backup system test with multiple k8s clusters")
}

var _ = Describe("Mantle", func() {
	Context("wait controller to be ready", waitControllerToBeReady)
	Context("replication test", replicationTestSuite)
	Context("change to standalone", changeToStandalone)
})

func waitControllerToBeReady() {
	It("wait for mantle-controller to be ready", func() {
		Eventually(func() error {
			return checkDeploymentReady(primaryK8sCluster, "rook-ceph", "mantle-controller")
		}).Should(Succeed())

		Eventually(func() error {
			return checkDeploymentReady(primaryK8sCluster, "rook-ceph", "mantle-controller")
		}).Should(Succeed())
	})
}

func setupEnvironment(namespace, pvcName string) {
	GinkgoHelper()
	By("setting up the environment")
	Eventually(func() error {
		return createNamespace(primaryK8sCluster, namespace)
	}).Should(Succeed())
	Eventually(func() error {
		return createNamespace(secondaryK8sCluster, namespace)
	}).Should(Succeed())
	Eventually(func() error {
		return applyRBDPoolAndSCTemplate(primaryK8sCluster, cephClusterNamespace)
	}).Should(Succeed())
	Eventually(func() error {
		return applyRBDPoolAndSCTemplate(secondaryK8sCluster, cephClusterNamespace)
	}).Should(Succeed())
	Eventually(func() error {
		return applyPVCTemplate(primaryK8sCluster, namespace, pvcName)
	}).Should(Succeed())
}

func writeRandomDataToPV(ctx context.Context, namespace, pvcName string) string {
	GinkgoHelper()
	By("writing some random data to PV(C)")
	writeJobName := util.GetUniqueName("job-")
	Eventually(ctx, func() error {
		return applyWriteJobTemplate(primaryK8sCluster, namespace, writeJobName, pvcName)
	}).Should(Succeed())
	Eventually(ctx, func(g Gomega) {
		job, err := getJob(primaryK8sCluster, namespace, writeJobName)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(IsJobConditionTrue(job.Status.Conditions, batchv1.JobComplete)).To(BeTrue())
	}).Should(Succeed())
	stdout, _, err := kubectl(primaryK8sCluster, nil, "logs", "-n", namespace, "job/"+writeJobName)
	Expect(err).NotTo(HaveOccurred())
	Expect(len(stdout)).NotTo(Equal(0))
	return string(stdout)
}

func createMantleBackup(namespace, pvcName, backupName string) {
	GinkgoHelper()
	By("creating a MantleBackup object")
	Eventually(func() error {
		return applyMantleBackupTemplate(primaryK8sCluster, namespace, pvcName, backupName)
	}).Should(Succeed())
}

func waitMantleBackupSynced(namespace, backupName string) {
	GinkgoHelper()
	By("checking MantleBackup's SyncedToRemote status")
	Eventually(func() error {
		mb, err := getMB(primaryK8sCluster, namespace, backupName)
		if err != nil {
			return err
		}
		if !meta.IsStatusConditionTrue(mb.Status.Conditions, mantlev1.BackupConditionSyncedToRemote) {
			return errors.New("status of SyncedToRemote condition is not True")
		}
		return nil
	}, "10m", "1s").Should(Succeed())
}

func ensureTemporaryResourcesRemoved(ctx context.Context) {
	GinkgoHelper()
	By("checking all temporary Jobs related to export and import of RBD images are removed")
	primaryJobList, err := getObjectList[batchv1.JobList](primaryK8sCluster, "job", cephClusterNamespace)
	Expect(err).NotTo(HaveOccurred())
	Expect(slices.ContainsFunc(primaryJobList.Items, func(job batchv1.Job) bool {
		n := job.GetName()
		return strings.HasPrefix(n, "mantle-export-") ||
			strings.HasPrefix(n, "mantle-upload-")
	})).To(BeFalse())
	secondaryJobList, err := getObjectList[batchv1.JobList](secondaryK8sCluster, "job", cephClusterNamespace)
	Expect(err).NotTo(HaveOccurred())
	Expect(slices.ContainsFunc(secondaryJobList.Items, func(job batchv1.Job) bool {
		n := job.GetName()
		return strings.HasPrefix(n, "mantle-import-") ||
			strings.HasPrefix(n, "mantle-discard-")
	})).To(BeFalse())

	By("checking all temporary PVCs related to export and import of RBD images are removed")
	primaryPVCList, err := getObjectList[corev1.PersistentVolumeClaimList](
		primaryK8sCluster, "pvc", cephClusterNamespace)
	Expect(err).NotTo(HaveOccurred())
	Expect(slices.ContainsFunc(primaryPVCList.Items, func(pvc corev1.PersistentVolumeClaim) bool {
		n := pvc.GetName()
		return strings.HasPrefix(n, "mantle-export-")
	})).To(BeFalse())
	secondaryPVCList, err := getObjectList[corev1.PersistentVolumeClaimList](
		secondaryK8sCluster, "pvc", cephClusterNamespace)
	Expect(err).NotTo(HaveOccurred())
	Expect(slices.ContainsFunc(secondaryPVCList.Items, func(pvc corev1.PersistentVolumeClaim) bool {
		n := pvc.GetName()
		return strings.HasPrefix(n, "mantle-discard-")
	})).To(BeFalse())

	By("checking all temporary PVs related to export and import of RBD images are removed")
	secondaryPVList, err := getObjectList[corev1.PersistentVolumeList](secondaryK8sCluster, "pv", cephClusterNamespace)
	Expect(err).NotTo(HaveOccurred())
	Expect(slices.ContainsFunc(secondaryPVList.Items, func(pv corev1.PersistentVolume) bool {
		n := pv.GetName()
		return strings.HasPrefix(n, "mantle-discard-")
	})).To(BeFalse())

	By("checking all temporary objects in the object storage related to export and import of RBD images are removed")
	objectStorageClient, err := createObjectStorageClient(ctx)
	Expect(err).NotTo(HaveOccurred())
	listOutput, err := objectStorageClient.listObjects(ctx)
	Expect(err).NotTo(HaveOccurred())
	Expect(len(listOutput.Contents)).To(Equal(0))
}

func ensureCorrectRestoration(
	clusterNo int,
	ctx context.Context,
	namespace, backupName, restoreName, writtenDataHash string,
) {
	GinkgoHelper()
	mountDeployName := util.GetUniqueName("deploy-")
	clusterName := "primary"
	if clusterNo == secondaryK8sCluster {
		clusterName = "secondary"
	}
	By(fmt.Sprintf("%s: %s: creating MantleRestore by using the MantleBackup replicated above",
		clusterName, backupName))
	Eventually(ctx, func() error {
		return applyMantleRestoreTemplate(clusterNo, namespace, restoreName, backupName)
	}).Should(Succeed())
	By(fmt.Sprintf("%s: %s: checking the MantleRestore can be ready to use", clusterName, backupName))
	Eventually(ctx, func(g Gomega) {
		mr, err := getMR(clusterNo, namespace, restoreName)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(meta.IsStatusConditionTrue(mr.Status.Conditions, "ReadyToUse")).To(BeTrue())
	}).Should(Succeed())
	By(fmt.Sprintf("%s: %s: checking the MantleRestore has the correct contents", clusterName, backupName))
	Eventually(ctx, func(g Gomega) {
		err := applyMountDeployTemplate(clusterNo, namespace, mountDeployName, restoreName)
		g.Expect(err).NotTo(HaveOccurred())
		stdout, _, err := kubectl(clusterNo, nil, "exec", "-n", namespace, "deploy/"+mountDeployName, "--",
			"bash", "-c", "sha256sum /volume/data | awk '{print $1}'")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(string(stdout)).To(Equal(writtenDataHash))
	}).Should(Succeed())
}

func replicationTestSuite() {
	Describe("replication test", func() {
		It("should correctly replicate PVC and MantleBackup resources", func(ctx SpecContext) {
			namespace := util.GetUniqueName("ns-")
			pvcName := util.GetUniqueName("pvc-")
			backupName := util.GetUniqueName("mb-")
			restoreName := util.GetUniqueName("mr-")

			setupEnvironment(namespace, pvcName)
			writtenDataHash := writeRandomDataToPV(ctx, namespace, pvcName)
			createMantleBackup(namespace, pvcName, backupName)
			waitMantleBackupSynced(namespace, backupName)

			By("checking PVC is replicated")
			Eventually(func() error {
				primaryPVC, err := getPVC(primaryK8sCluster, namespace, pvcName)
				if err != nil {
					return err
				}

				pvc, err := getPVC(secondaryK8sCluster, namespace, pvcName)
				if err != nil {
					return err
				}
				if pvc.Annotations == nil ||
					pvc.Annotations["mantle.cybozu.io/remote-uid"] != string(primaryPVC.GetUID()) {
					return errors.New("invalid remote-uid annotation")
				}
				primaryPVC.Spec.VolumeName = ""
				pvc.Spec.VolumeName = ""
				if !reflect.DeepEqual(primaryPVC.Spec, pvc.Spec) {
					return errors.New("spec not equal")
				}
				if pvc.Status.Phase != corev1.ClaimBound {
					return errors.New("pvc not bound")
				}

				return nil
			}).Should(Succeed())

			By("checking MantleBackup is replicated")
			Eventually(func() error {
				primaryPVC, err := getPVC(primaryK8sCluster, namespace, pvcName)
				if err != nil {
					return err
				}
				secondaryPVC, err := getPVC(secondaryK8sCluster, namespace, pvcName)
				if err != nil {
					return err
				}
				primaryMB, err := getMB(primaryK8sCluster, namespace, backupName)
				if err != nil {
					return err
				}

				secondaryMB, err := getMB(secondaryK8sCluster, namespace, backupName)
				if err != nil {
					return err
				}
				if !controllerutil.ContainsFinalizer(secondaryMB, "mantlebackup.mantle.cybozu.io/finalizer") {
					return errors.New("finalizer not found")
				}
				if secondaryMB.Labels == nil ||
					secondaryMB.Labels["mantle.cybozu.io/local-backup-target-pvc-uid"] != string(secondaryPVC.GetUID()) ||
					secondaryMB.Labels["mantle.cybozu.io/remote-backup-target-pvc-uid"] != string(primaryPVC.GetUID()) {
					return errors.New("local/remote-backup-target-pvc-uid label not matched")
				}
				if secondaryMB.Annotations == nil ||
					secondaryMB.Annotations["mantle.cybozu.io/remote-uid"] != string(primaryMB.GetUID()) {
					return errors.New("remote-uid not matched")
				}
				if !reflect.DeepEqual(primaryMB.Spec, secondaryMB.Spec) {
					return errors.New("spec not equal")
				}
				if secondaryMB.Status.CreatedAt.IsZero() {
					return errors.New(".Status.CreatedAt is zero")
				}
				if !meta.IsStatusConditionTrue(secondaryMB.Status.Conditions, "ReadyToUse") {
					return errors.New("ReadyToUse of .Status.Conditions is not True")
				}

				return nil
			}).Should(Succeed())

			ensureTemporaryResourcesRemoved(ctx)
			ensureCorrectRestoration(primaryK8sCluster, ctx, namespace, backupName, restoreName, writtenDataHash)
			ensureCorrectRestoration(secondaryK8sCluster, ctx, namespace, backupName, restoreName, writtenDataHash)
		})

		It("should back up correctly if previous MB is deleted in the secondary cluster", func(ctx SpecContext) {
			namespace := util.GetUniqueName("ns-")
			pvcName := util.GetUniqueName("pvc-")
			backupName0 := util.GetUniqueName("mb-")
			backupName1 := util.GetUniqueName("mb-")
			restoreName0 := util.GetUniqueName("mr-")
			restoreName1 := util.GetUniqueName("mr-")

			setupEnvironment(namespace, pvcName)
			writtenDataHash0 := writeRandomDataToPV(ctx, namespace, pvcName)

			// create M0.
			createMantleBackup(namespace, pvcName, backupName0)
			waitMantleBackupSynced(namespace, backupName0)

			// remove M0'.
			_, _, err := kubectl(secondaryK8sCluster, nil, "delete", "mb", "-n", namespace, backupName0)
			Expect(err).NotTo(HaveOccurred())

			// create M1.
			writtenDataHash1 := writeRandomDataToPV(ctx, namespace, pvcName)
			createMantleBackup(namespace, pvcName, backupName1)
			waitMantleBackupSynced(namespace, backupName1)
			ensureTemporaryResourcesRemoved(ctx)

			// Make sure M1 and M1' have the same contents.
			ensureCorrectRestoration(primaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)
			ensureCorrectRestoration(secondaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)

			// Make sure M0 can be used for restoration.
			ensureCorrectRestoration(primaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
		})

		It("should back up correctly if previous MB is deleted in the primary cluster", func(ctx SpecContext) {
			namespace := util.GetUniqueName("ns-")
			pvcName := util.GetUniqueName("pvc-")
			backupName0 := util.GetUniqueName("mb-")
			backupName1 := util.GetUniqueName("mb-")
			restoreName0 := util.GetUniqueName("mr-")
			restoreName1 := util.GetUniqueName("mr-")

			setupEnvironment(namespace, pvcName)
			writtenDataHash0 := writeRandomDataToPV(ctx, namespace, pvcName)

			// create M0.
			createMantleBackup(namespace, pvcName, backupName0)
			waitMantleBackupSynced(namespace, backupName0)

			// remove M0.
			_, _, err := kubectl(primaryK8sCluster, nil, "delete", "mb", "-n", namespace, backupName0)
			Expect(err).NotTo(HaveOccurred())

			// create M1.
			writtenDataHash1 := writeRandomDataToPV(ctx, namespace, pvcName)
			createMantleBackup(namespace, pvcName, backupName1)
			waitMantleBackupSynced(namespace, backupName1)
			ensureTemporaryResourcesRemoved(ctx)

			// Make sure M1 and M1' have the same contents.
			ensureCorrectRestoration(primaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)
			ensureCorrectRestoration(secondaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)

			// Make sure M0' can be used for restoration.
			ensureCorrectRestoration(secondaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
		})

		It("should perform a correct incremental backup", func(ctx SpecContext) {
			namespace := util.GetUniqueName("ns-")
			pvcName := util.GetUniqueName("pvc-")
			backupName0 := util.GetUniqueName("mb-")
			backupName1 := util.GetUniqueName("mb-")
			restoreName0 := util.GetUniqueName("mr-")
			restoreName1 := util.GetUniqueName("mr-")

			setupEnvironment(namespace, pvcName)

			// create M0.
			writtenDataHash0 := writeRandomDataToPV(ctx, namespace, pvcName)
			createMantleBackup(namespace, pvcName, backupName0)
			waitMantleBackupSynced(namespace, backupName0)
			ensureTemporaryResourcesRemoved(ctx)

			// create M1.
			writtenDataHash1 := writeRandomDataToPV(ctx, namespace, pvcName)
			createMantleBackup(namespace, pvcName, backupName1)
			waitMantleBackupSynced(namespace, backupName1)
			ensureTemporaryResourcesRemoved(ctx)

			// Make sure M1 and M1' have the same contents.
			ensureCorrectRestoration(primaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)
			ensureCorrectRestoration(secondaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)

			// Make sure M0 and M0' have the same contents.
			ensureCorrectRestoration(primaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
			ensureCorrectRestoration(secondaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
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

			setupEnvironment(namespace, pvcName)

			// create M0.
			writtenDataHash0 := writeRandomDataToPV(ctx, namespace, pvcName)
			createMantleBackup(namespace, pvcName, backupName0)
			waitMantleBackupSynced(namespace, backupName0)

			// create M1.
			writtenDataHash1 := writeRandomDataToPV(ctx, namespace, pvcName)
			createMantleBackup(namespace, pvcName, backupName1)
			waitMantleBackupSynced(namespace, backupName1)

			// remove M1'.
			_, _, err := kubectl(secondaryK8sCluster, nil, "delete", "mb", "-n", namespace, backupName1)
			Expect(err).NotTo(HaveOccurred())

			// create M2.
			writtenDataHash2 := writeRandomDataToPV(ctx, namespace, pvcName)
			createMantleBackup(namespace, pvcName, backupName2)
			waitMantleBackupSynced(namespace, backupName2)
			ensureTemporaryResourcesRemoved(ctx)

			// Make sure M2 and M2' have the same contents.
			ensureCorrectRestoration(primaryK8sCluster, ctx, namespace, backupName2, restoreName2, writtenDataHash2)
			ensureCorrectRestoration(secondaryK8sCluster, ctx, namespace, backupName2, restoreName2, writtenDataHash2)

			// Make sure M1 has the same contents.
			ensureCorrectRestoration(primaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)

			// Make sure M0 and M0' have the same contents.
			ensureCorrectRestoration(primaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
			ensureCorrectRestoration(secondaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)

			// Make sure M1' isn't re-created.
			mbList, err := getObjectList[mantlev1.MantleBackupList](secondaryK8sCluster, "mb", namespace)
			Expect(err).NotTo(HaveOccurred())
			Expect(slices.ContainsFunc(mbList.Items, func(mb mantlev1.MantleBackup) bool {
				return mb.GetName() == backupName1
			})).To(BeFalse())
		})
	})
}

func changeToStandalone() {
	Describe("change to standalone", func() {
		var namespace, pvcName, backupName string

		It("should replicate a MantleBackup resource", func() {
			namespace = util.GetUniqueName("ns-")
			pvcName = util.GetUniqueName("pvc-")
			backupName = util.GetUniqueName("mb-")

			setupEnvironment(namespace, pvcName)
			createMantleBackup(namespace, pvcName, backupName)
			waitMantleBackupSynced(namespace, backupName)
		})

		It("should change the roles to standalone", func() {
			By("changing the primary mantle to standalone")
			err := changeClusterRole(primaryK8sCluster, controller.RoleStandalone)
			Expect(err).NotTo(HaveOccurred())
			By("changing the secondary mantle to standalone")
			err = changeClusterRole(secondaryK8sCluster, controller.RoleStandalone)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should delete MantleBackup created by primary mantle from standalone mantle", func(ctx SpecContext) {
			By("deleting the MantleBackup in the primary cluster")
			_, _, err := kubectl(primaryK8sCluster, nil, "delete", "mb", "-n", namespace, backupName, "--wait=false")
			Expect(err).NotTo(HaveOccurred())

			By("checking that the MantleBackup is actually deleted")
			Eventually(ctx, func(g Gomega) {
				stdout, _, err := kubectl(primaryK8sCluster, nil, "get", "mb", "-n", namespace, "-o", "json")
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
			_, _, err := kubectl(secondaryK8sCluster, nil, "delete", "mb", "-n", namespace, backupName, "--wait=false")
			Expect(err).NotTo(HaveOccurred())

			By("checking that the MantleBackup is NOT deleted")
			Consistently(ctx, func(g Gomega) {
				stdout, _, err := kubectl(secondaryK8sCluster, nil, "get", "mb", "-n", namespace, "-o", "json")
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
	})
}
