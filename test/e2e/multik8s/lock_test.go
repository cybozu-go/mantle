package multik8s

import (
	"encoding/json"
	"time"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/ceph"
	"github.com/cybozu-go/mantle/internal/controller"
	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)

var _ = Describe("Locking", Label("lock"), func() {
	namespace := util.GetUniqueName("ns-")
	pvcName := util.GetUniqueName("pvc-")
	backupName0 := util.GetUniqueName("mb-")
	backupName1 := util.GetUniqueName("mb-")
	dummyLockID := "dummy-lock-id"
	var controllerPod string
	var poolName, imageName string

	It("should setup environment", func(ctx SpecContext) {
		SetupEnvironment(namespace)
		// Create PVC and MantleBackup in the primary cluster
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName0)
		WaitMantleBackupSynced(namespace, backupName0)
		var err error
		controllerPod, err = GetControllerPodName(SecondaryK8sCluster)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should lock the volume in the secondary cluster", func() {
		mb0, err := GetMB(SecondaryK8sCluster, namespace, backupName0)
		Expect(err).NotTo(HaveOccurred())

		pvStored := corev1.PersistentVolume{}
		err = json.Unmarshal([]byte(mb0.Status.PVManifest), &pvStored)
		Expect(err).NotTo(HaveOccurred())
		poolName = pvStored.Spec.CSI.VolumeAttributes["pool"]
		imageName = pvStored.Spec.CSI.VolumeAttributes["imageName"]

		// locked
		_, _, err = Kubectl(SecondaryK8sCluster, nil, "exec", "-n", CephClusterNamespace, controllerPod, "--",
			"rbd", "-p", poolName, "lock", "add", imageName, dummyLockID)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should create additional backup and wait for the log", func(ctx SpecContext) {
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName1)
		var mb1 *mantlev1.MantleBackup
		Eventually(func() error {
			var err error
			mb1, err = GetMB(SecondaryK8sCluster, namespace, backupName1)
			return err
		}).Should(Succeed())

		err := WaitControllerLog(ctx, SecondaryK8sCluster,
			"the volume is locked by another process.*"+string(mb1.GetUID()),
			3*time.Minute)
		Expect(err).NotTo(HaveOccurred())
	})

	It("checks that the jobs are not created", func(ctx SpecContext) {
		mb1, err := GetMB(SecondaryK8sCluster, namespace, backupName1)
		Expect(err).NotTo(HaveOccurred())
		Expect(
			CheckJobExist(SecondaryK8sCluster, CephClusterNamespace, controller.MantleZeroOutJobPrefix+string(mb1.GetUID())),
		).To(BeFalse())
		Expect(
			CheckJobExist(SecondaryK8sCluster, CephClusterNamespace, controller.MantleImportJobPrefix+string(mb1.GetUID())),
		).To(BeFalse())
	})

	It("should unlock the volume in the secondary cluster", func() {
		stdout, _, err := Kubectl(SecondaryK8sCluster, nil, "exec", "-n", CephClusterNamespace, controllerPod, "--",
			"rbd", "-p", poolName, "--format", "json", "lock", "ls", imageName)
		Expect(err).NotTo(HaveOccurred())
		var locks []*ceph.RBDLock
		err = json.Unmarshal(stdout, &locks)
		Expect(err).NotTo(HaveOccurred())
		Expect(locks).To(HaveLen(1))

		// unlock
		_, _, err = Kubectl(SecondaryK8sCluster, nil, "exec", "-n", CephClusterNamespace, controllerPod, "--",
			"rbd", "-p", poolName, "lock", "rm", imageName, dummyLockID, locks[0].Locker)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should resume backup creation and complete it", func() {
		WaitMantleBackupSynced(namespace, backupName1)
	})

	It("should not exist locks after backup completion", func() {
		stdout, _, err := Kubectl(SecondaryK8sCluster, nil, "exec", "-n", CephClusterNamespace, controllerPod, "--",
			"rbd", "-p", poolName, "--format", "json", "lock", "ls", imageName)
		Expect(err).NotTo(HaveOccurred())
		var locks []*ceph.RBDLock
		err = json.Unmarshal(stdout, &locks)
		Expect(err).NotTo(HaveOccurred())
		Expect(locks).To(HaveLen(0))
	})
})
