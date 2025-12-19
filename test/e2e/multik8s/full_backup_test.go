package multik8s

import (
	"errors"
	"fmt"
	"reflect"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/kubelet/events"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

var _ = Describe("full backup", Label("full-backup"), func() {
	It("should correctly replicate PVC and MantleBackup resources",
		// This test case is intended to be run with various backup transfer
		// part size. This label is attached mainly for CI. See also
		// e2e-multiple-k8s-clusters.yaml workflow.
		Label("various-transfer-part-size"),
		func(ctx SpecContext) {
			namespace := util.GetUniqueName("ns-")
			podName := util.GetUniqueName("pod-")
			pvcName := util.GetUniqueName("pvc-")
			backupName := util.GetUniqueName("mb-")
			restoreName := util.GetUniqueName("mr-")

			SetupEnvironment(namespace)
			CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)
			writtenDataHash := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName)
			WaitMantleBackupSynced(namespace, backupName)

			By("checking PVC is replicated")
			Eventually(func() error {
				primaryPVC, err := GetPVC(PrimaryK8sCluster, namespace, pvcName)
				if err != nil {
					return err
				}
				primaryMB, err := GetMB(PrimaryK8sCluster, namespace, backupName)
				if err != nil {
					return err
				}

				pvc, err := GetPVC(SecondaryK8sCluster, namespace, pvcName)
				if err != nil {
					return err
				}
				if pvc.Annotations == nil ||
					pvc.Annotations["mantle.cybozu.io/remote-uid"] != string(primaryPVC.GetUID()) {
					return errors.New("invalid remote-uid annotation")
				}
				pvcSize, ok := pvc.Spec.Resources.Requests.Storage().AsInt64()
				if !ok {
					return errors.New("storage size is not set")
				}
				if pvcSize != *primaryMB.Status.SnapSize {
					return errors.New("storage size not equal to the snapshot size")
				}
				primaryPVC.Spec.VolumeName = ""
				delete(primaryPVC.Spec.Resources.Requests, corev1.ResourceStorage)
				pvc.Spec.VolumeName = ""
				delete(pvc.Spec.Resources.Requests, corev1.ResourceStorage)
				if !reflect.DeepEqual(primaryPVC.Spec, pvc.Spec) {
					return errors.New("spec not equal")
				}
				if pvc.Status.Phase != corev1.ClaimBound {
					return errors.New("pvc not bound")
				}

				return nil
			}).Should(Succeed())

			By("checking MantleBackup is replicated")
			var primaryMB, secondaryMB *mantlev1.MantleBackup
			Eventually(func() error {
				primaryPVC, err := GetPVC(PrimaryK8sCluster, namespace, pvcName)
				if err != nil {
					return err
				}
				secondaryPVC, err := GetPVC(SecondaryK8sCluster, namespace, pvcName)
				if err != nil {
					return err
				}
				primaryMB, err = GetMB(PrimaryK8sCluster, namespace, backupName)
				if err != nil {
					return err
				}

				secondaryMB, err = GetMB(SecondaryK8sCluster, namespace, backupName)
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
				if !secondaryMB.IsReady() {
					return errors.New("ReadyToUse of .Status.Conditions is not True")
				}

				// Check if snapshots are created correctly in the secondary Rook/Ceph cluster
				snap, err := FindRBDSnapshotInPVC(SecondaryK8sCluster,
					secondaryPVC.GetNamespace(), secondaryPVC.GetName(), secondaryMB.GetName())
				if err != nil {
					return fmt.Errorf("failed to find snapshot in PVC: %w", err)
				}
				if secondaryMB.Status.SnapID == nil || snap.Id != *secondaryMB.Status.SnapID {
					return errors.New("invalid .status.snapID of secondary MB")
				}

				return nil
			}).Should(Succeed())

			// Make sure verification step has completed in both clusters.
			WaitMantleBackupVerified(PrimaryK8sCluster, namespace, primaryMB.GetName())
			WaitMantleBackupVerified(SecondaryK8sCluster, namespace, secondaryMB.GetName())

			// Make sure snapshots are correctly created.
			Eventually(func(g Gomega) {
				primarySnaps, err := ListRBDSnapshotsInPVC(PrimaryK8sCluster, namespace, pvcName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(primarySnaps).To(HaveLen(1))
				g.Expect(primarySnaps[0].Name).To(Equal(backupName))
				secondarySnaps, err := ListRBDSnapshotsInPVC(SecondaryK8sCluster, namespace, pvcName)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(secondarySnaps).To(HaveLen(1)) // Middle snapshots should be deleted.
				g.Expect(secondarySnaps[0].Name).To(Equal(backupName))
			}).Should(Succeed())

			By("ensuring the PVC can be attached to the pod in the primary cluster")
			CreatePod(PrimaryK8sCluster, namespace, podName, pvcName)
			Eventually(func() error {
				pods, err := GetPodList(PrimaryK8sCluster, namespace)
				if err != nil {
					return err
				}
				for _, pod := range pods.Items {
					if pod.Name == podName && pod.Status.Phase == corev1.PodRunning {
						return nil
					}
				}
				return errors.New("pod not found or not running")
			}).Should(Succeed())

			By("ensuring the PVC can not be attached to any pods in the secondary cluster")
			CreatePod(SecondaryK8sCluster, namespace, podName, pvcName)
			Eventually(func() error {
				eventList, err := GetEventList(SecondaryK8sCluster, namespace)
				if err != nil {
					return err
				}
				for _, event := range eventList.Items {
					if event.InvolvedObject.Namespace == namespace && event.InvolvedObject.Name == podName {
						if event.Reason == events.FailedAttachVolume {
							return nil
						}
					}
				}
				return errors.New("expected event not found")
			}).Should(Succeed())

			WaitTemporaryResourcesDeleted(ctx, primaryMB, secondaryMB)
			EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName, restoreName, writtenDataHash)
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName, restoreName, writtenDataHash)
		},
	)

	//nolint:dupl
	It("should back up correctly if previous MB is deleted in the secondary cluster", func(ctx SpecContext) {
		namespace := util.GetUniqueName("ns-")
		pvcName := util.GetUniqueName("pvc-")
		backupName0 := util.GetUniqueName("mb-")
		backupName1 := util.GetUniqueName("mb-")
		restoreName0 := util.GetUniqueName("mr-")
		restoreName1 := util.GetUniqueName("mr-")

		SetupEnvironment(namespace)
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)
		writtenDataHash0 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)

		// create M0.
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName0)
		WaitMantleBackupSynced(namespace, backupName0)

		// remove M0'.
		_, _, err := Kubectl(SecondaryK8sCluster, nil, "delete", "mb", "-n", namespace, backupName0)
		Expect(err).NotTo(HaveOccurred())

		// create M1.
		writtenDataHash1 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName1)
		WaitMantleBackupSynced(namespace, backupName1)

		primaryMB1, err := GetMB(PrimaryK8sCluster, namespace, backupName1)
		Expect(err).NotTo(HaveOccurred())
		secondaryMB1, err := GetMB(SecondaryK8sCluster, namespace, backupName1)
		Expect(err).NotTo(HaveOccurred())
		WaitTemporaryResourcesDeleted(ctx, primaryMB1, secondaryMB1)

		// Make sure verification step has completed in both clusters.
		WaitMantleBackupVerified(PrimaryK8sCluster, namespace, backupName0)
		WaitMantleBackupVerified(PrimaryK8sCluster, namespace, backupName1)
		WaitMantleBackupVerified(SecondaryK8sCluster, namespace, backupName1)

		// Make sure M1 and M1' have the same contents.
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)

		// Make sure M0 can be used for restoration.
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
	})

	//nolint:dupl
	It("should back up correctly if previous MB is deleted in the primary cluster", func(ctx SpecContext) {
		namespace := util.GetUniqueName("ns-")
		pvcName := util.GetUniqueName("pvc-")
		backupName0 := util.GetUniqueName("mb-")
		backupName1 := util.GetUniqueName("mb-")
		restoreName0 := util.GetUniqueName("mr-")
		restoreName1 := util.GetUniqueName("mr-")

		SetupEnvironment(namespace)
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)
		writtenDataHash0 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)

		// create M0.
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName0)
		WaitMantleBackupSynced(namespace, backupName0)

		// remove M0.
		_, _, err := Kubectl(PrimaryK8sCluster, nil, "delete", "mb", "-n", namespace, backupName0)
		Expect(err).NotTo(HaveOccurred())

		// create M1.
		writtenDataHash1 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName1)
		WaitMantleBackupSynced(namespace, backupName1)

		primaryMB1, err := GetMB(PrimaryK8sCluster, namespace, backupName1)
		Expect(err).NotTo(HaveOccurred())
		secondaryMB1, err := GetMB(SecondaryK8sCluster, namespace, backupName1)
		Expect(err).NotTo(HaveOccurred())
		WaitTemporaryResourcesDeleted(ctx, primaryMB1, secondaryMB1)

		// Make sure verification step has completed in both clusters.
		WaitMantleBackupVerified(SecondaryK8sCluster, namespace, backupName0)
		WaitMantleBackupVerified(PrimaryK8sCluster, namespace, backupName1)
		WaitMantleBackupVerified(SecondaryK8sCluster, namespace, backupName1)

		// Make sure M1 and M1' have the same contents.
		EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)

		// Make sure M0' can be used for restoration.
		EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
	})
})
