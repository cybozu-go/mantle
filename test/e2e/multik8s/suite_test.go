package multik8s

import (
	_ "embed"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	corev1 "k8s.io/api/core/v1"
)

func TestMtest(t *testing.T) {
	if os.Getenv("E2ETEST") == "" {
		t.Skip("Run under e2e/")
	}

	RegisterFailHandler(Fail)

	SetDefaultEventuallyPollingInterval(time.Second)
	SetDefaultEventuallyTimeout(3 * time.Minute)

	RunSpecs(t, "rbd backup system test with multiple k8s clusters")
}

var _ = Describe("Mantle", func() {
	Context("wait controller to be ready", waitControllerToBeReady)
	Context("replication test", replicationTestSuite)
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

func replicationTestSuite() {
	Describe("replication test", func() {
		It("should correctly replicate PVC and MantleBackup resources", func() {
			namespace := util.GetUniqueName("ns-")
			pvcName := util.GetUniqueName("pvc-")
			backupName := util.GetUniqueName("mb-")
			scName := util.GetUniqueName("sc-")
			poolName := util.GetUniqueName("pool-")

			By("setting up the environment")
			Eventually(func() error {
				return createNamespace(primaryK8sCluster, namespace)
			}).Should(Succeed())
			Eventually(func() error {
				return createNamespace(secondaryK8sCluster, namespace)
			}).Should(Succeed())
			Eventually(func() error {
				return applyRBDPoolAndSCTemplate(primaryK8sCluster, cephClusterNamespace, poolName, scName)
			}).Should(Succeed())
			Eventually(func() error {
				return applyRBDPoolAndSCTemplate(secondaryK8sCluster, cephClusterNamespace, poolName, scName)
			}).Should(Succeed())
			Eventually(func() error {
				return applyPVCTemplate(primaryK8sCluster, namespace, pvcName, scName)
			}).Should(Succeed())

			By("creating a MantleBackup object")
			Eventually(func() error {
				return applyMantleBackupTemplate(primaryK8sCluster, namespace, pvcName, backupName)
			}).Should(Succeed())

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
			}, "5m", "1s").Should(Succeed())

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
				if secondaryMB.Status.SnapID != nil {
					return errors.New(".Status.SapID is incorrectly populated")
				}
				if !meta.IsStatusConditionTrue(secondaryMB.Status.Conditions, "ReadyToUse") {
					return errors.New("ReadyToUse of .Status.Conditions is not True")
				}

				return nil
			}).Should(Succeed())
		})
	})
}
