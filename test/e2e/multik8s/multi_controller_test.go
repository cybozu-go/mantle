package multik8s

import (
	"context"
	"fmt"
	"time"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller"
	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// This test verifies that, when two Ceph clusters (rook-ceph and rook-ceph2)
// each run their own mantle-controller, backups of the two clusters are
// replicated independently and are never mixed up between the controllers.
//
// The two logical Ceph clusters share the same physical Ceph cluster in this
// e2e environment; they are distinguished only by the StorageClass clusterID
// (== the Ceph cluster namespace, which is also each controller's
// managedCephClusterID).
var _ = Describe("multi controller", Label("multi-controller"), Ordered, func() {
	type target struct {
		scName      string
		cephNS      string
		otherCephNS string
		pvcName     string
		backupName  string
		restoreName string
		writtenHash string
		primaryMB   *mantlev1.MantleBackup
		secondaryMB *mantlev1.MantleBackup
	}

	var namespace string
	var targets []*target

	It("should set up the fake second Ceph cluster ID (rook-ceph2)", func() {
		SetupFakeClusterID()
	})

	It("should create PVCs and MantleBackups for both Ceph clusters", func(ctx SpecContext) {
		namespace = util.GetUniqueName("ns-")
		targets = []*target{
			{
				scName:      SCName1,
				cephNS:      CephCluster1Namespace,
				otherCephNS: CephCluster2Namespace,
				pvcName:     util.GetUniqueName("pvc-"),
				backupName:  util.GetUniqueName("mb-"),
				restoreName: util.GetUniqueName("mr-"),
			},
			{
				scName:      SCName2,
				cephNS:      CephCluster2Namespace,
				otherCephNS: CephCluster1Namespace,
				pvcName:     util.GetUniqueName("pvc-"),
				backupName:  util.GetUniqueName("mb-"),
				restoreName: util.GetUniqueName("mr-"),
			},
		}

		SetupNamespaces(namespace)

		// Create a PVC and a MantleBackup for each Ceph cluster in the primary
		// k8s cluster. The two workloads are created back-to-back so that both
		// controllers are reconciling at the same time.
		for _, t := range targets {
			CreatePVC(ctx, PrimaryK8sCluster, namespace, t.pvcName, t.scName)
			t.writtenHash = WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, t.pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, t.pvcName, t.backupName)
		}
	})

	It("should label the primary MantleBackups with their own Ceph cluster-id", func() {
		for _, t := range targets {
			pvc, err := GetPVC(PrimaryK8sCluster, namespace, t.pvcName)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func(g Gomega) {
				mb, err := GetMB(PrimaryK8sCluster, namespace, t.backupName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(mb.GetLabels()).To(
					HaveKeyWithValue("mantle.cybozu.io/cluster-id", t.cephNS))
				g.Expect(mb.GetLabels()).To(
					HaveKeyWithValue("mantle.cybozu.io/local-backup-target-pvc-uid", string(pvc.GetUID())))
			}).Should(Succeed())
		}
	})

	It("should replicate both MantleBackups to the secondary independently", func() {
		for _, t := range targets {
			WaitMantleBackupSynced(namespace, t.backupName)

			primaryPVC, err := GetPVC(PrimaryK8sCluster, namespace, t.pvcName)
			Expect(err).NotTo(HaveOccurred())
			primaryMB, err := GetMB(PrimaryK8sCluster, namespace, t.backupName)
			Expect(err).NotTo(HaveOccurred())
			secondaryPVC, err := GetPVC(SecondaryK8sCluster, namespace, t.pvcName)
			Expect(err).NotTo(HaveOccurred())
			secondaryMB, err := GetMB(SecondaryK8sCluster, namespace, t.backupName)
			Expect(err).NotTo(HaveOccurred())

			t.primaryMB = primaryMB
			t.secondaryMB = secondaryMB

			// check labels
			Expect(secondaryMB.GetLabels()).To(
				HaveKeyWithValue("mantle.cybozu.io/cluster-id", t.cephNS))
			Expect(secondaryMB.GetLabels()).To(
				HaveKeyWithValue("mantle.cybozu.io/local-backup-target-pvc-uid", string(secondaryPVC.GetUID())))
			Expect(secondaryMB.GetLabels()).To(
				HaveKeyWithValue("mantle.cybozu.io/remote-backup-target-pvc-uid", string(primaryPVC.GetUID())))
			Expect(secondaryMB.GetAnnotations()).To(
				HaveKeyWithValue("mantle.cybozu.io/remote-uid", string(primaryMB.GetUID())))
			Expect(secondaryMB.IsSnapshotCaptured()).To(BeTrue())
		}
	})

	It("should run verify in the correct Ceph cluster without leaking into the other one", func(ctx SpecContext) {
		ctx2, cancel := context.WithCancel(ctx)
		defer cancel()

		// The verify job runs in its own Ceph cluster's namespace. This goroutine keeps checking that
		// neither controller creates the verify Job or PVC in the OTHER Ceph cluster's namespace.
		go func() {
			defer GinkgoRecover()
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx2.Done():
					return
				case <-ticker.C:
					for _, t := range targets {
						for _, mb := range []*mantlev1.MantleBackup{t.primaryMB, t.secondaryMB} {
							for _, cluster := range []int{PrimaryK8sCluster, SecondaryK8sCluster} {
								Expect(CheckJobExist(cluster, t.otherCephNS, controller.MakeVerifyJobName(mb))).To(BeFalse())
								Expect(CheckPVCExist(cluster, t.otherCephNS, controller.MakeVerifyPVCName(mb))).To(BeFalse())
							}
						}
					}
				}
			}
		}()

		for _, t := range targets {
			WaitMantleBackupVerified(PrimaryK8sCluster, namespace, t.backupName)
			WaitMantleBackupVerified(SecondaryK8sCluster, namespace, t.backupName)
		}
	})

	It("should restore both MantleBackups in both clusters", func(ctx SpecContext) {
		for _, t := range targets {
			EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, t.backupName, t.restoreName, t.writtenHash)
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, t.backupName, t.restoreName, t.writtenHash)
		}
	})

	It("should clean up all temporary resources in the owning Ceph cluster", func(ctx SpecContext) {
		for _, t := range targets {
			WaitTemporaryResourcesDeleted(ctx, t.primaryMB, t.secondaryMB)
		}
	})

	It("should delete a cluster1 backup without affecting cluster2", func(ctx SpecContext) {
		del := targets[0]  // rook-ceph
		keep := targets[1] // rook-ceph2

		By(fmt.Sprintf("ensuring the %s snapshot exists on the primary before deletion", del.backupName))
		EnsurePVCHasSnapshot(PrimaryK8sCluster, namespace, del.pvcName, del.backupName)

		By(fmt.Sprintf("deleting the %s MantleBackup on the primary", del.backupName))
		DeleteMantleBackup(PrimaryK8sCluster, namespace, del.backupName)
		EnsureMantleBackupNotExist(ctx, PrimaryK8sCluster, namespace, del.backupName)

		By(fmt.Sprintf("ensuring the %s snapshot is deleted on the primary", del.backupName))
		EnsurePVCHasNoSnapshot(PrimaryK8sCluster, namespace, del.pvcName, del.backupName)

		By(fmt.Sprintf("ensuring the %s MantleBackup is untouched in both clusters", keep.backupName))
		EnsureMantleBackupExists(ctx, PrimaryK8sCluster, namespace, keep.backupName)
		EnsurePVCHasSnapshot(PrimaryK8sCluster, namespace, keep.pvcName, keep.backupName)
		EnsureMantleBackupExists(ctx, SecondaryK8sCluster, namespace, keep.backupName)
		EnsurePVCHasSnapshot(SecondaryK8sCluster, namespace, keep.pvcName, keep.backupName)
		EnsureMantleBackupExists(ctx, SecondaryK8sCluster, namespace, del.backupName)
		EnsurePVCHasSnapshot(SecondaryK8sCluster, namespace, del.pvcName, del.backupName)

		By(fmt.Sprintf("ensuring the %s MantleBackup is still restorable", keep.backupName))
		restoreName := util.GetUniqueName("mr-")
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, keep.backupName, restoreName, keep.writtenHash)
	})
})
