package replication

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller"
	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestMtest(t *testing.T) {
	if os.Getenv("E2ETEST") == "" {
		t.Skip("Run under e2e/")
	}

	RegisterFailHandler(Fail)

	SetDefaultEventuallyPollingInterval(time.Second)
	SetDefaultEventuallyTimeout(3 * time.Minute)
	EnforceDefaultTimeoutsWhenUsingContexts()

	RunSpecs(t, "replication test with multiple k8s clusters")
}

var _ = Describe("Mantle", func() {
	Context("wait controller to be ready", WaitControllerToBeReady)
	Context("replication test", replicationTestSuite)
})

func replicationTestSuite() { //nolint:gocyclo
	Describe("replication test", func() {
		It("should correctly replicate PVC and MantleBackup resources",
			// This test case is intended to be run with various backup transfer
			// part size. This label is attached mainly for CI. See also
			// e2e-multiple-k8s-clusters.yaml workflow.
			Label("various-transfer-part-size"),
			func(ctx SpecContext) {
				namespace := util.GetUniqueName("ns-")
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

					pvc, err := GetPVC(SecondaryK8sCluster, namespace, pvcName)
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
					if !meta.IsStatusConditionTrue(secondaryMB.Status.Conditions, "ReadyToUse") {
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

				// Make sure snapshots are correctly created.
				primarySnaps, err := ListRBDSnapshotsInPVC(PrimaryK8sCluster, namespace, pvcName)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(primarySnaps)).To(Equal(1))
				Expect(primarySnaps[0].Name).To(Equal(backupName))
				secondarySnaps, err := ListRBDSnapshotsInPVC(SecondaryK8sCluster, namespace, pvcName)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(secondarySnaps)).To(Equal(1)) // middle snapshots should be deleted.
				Expect(secondarySnaps[0].Name).To(Equal(backupName))

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

			// Make sure M1 and M1' have the same contents.
			EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)

			// Make sure M0' can be used for restoration.
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
		})

		It("should perform a correct incremental backup", func(ctx SpecContext) {
			namespace := util.GetUniqueName("ns-")
			pvcName := util.GetUniqueName("pvc-")
			backupName0 := util.GetUniqueName("mb-")
			backupName1 := util.GetUniqueName("mb-")
			restoreName0 := util.GetUniqueName("mr-")
			restoreName1 := util.GetUniqueName("mr-")

			SetupEnvironment(namespace)
			CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)

			// create M0.
			writtenDataHash0 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName0)
			WaitMantleBackupSynced(namespace, backupName0)

			primaryMB0, err := GetMB(PrimaryK8sCluster, namespace, backupName0)
			Expect(err).NotTo(HaveOccurred())
			secondaryMB0, err := GetMB(SecondaryK8sCluster, namespace, backupName0)
			Expect(err).NotTo(HaveOccurred())
			WaitTemporaryResourcesDeleted(ctx, primaryMB0, secondaryMB0)

			// create M1.
			writtenDataHash1 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName1)
			WaitMantleBackupSynced(namespace, backupName1)

			primaryMB1, err := GetMB(PrimaryK8sCluster, namespace, backupName1)
			Expect(err).NotTo(HaveOccurred())
			secondaryMB1, err := GetMB(SecondaryK8sCluster, namespace, backupName1)
			Expect(err).NotTo(HaveOccurred())
			WaitTemporaryResourcesDeleted(ctx, primaryMB1, secondaryMB1)

			// Make sure M1 and M1' have the same contents.
			EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)

			// Make sure M0 and M0' have the same contents.
			EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
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

			SetupEnvironment(namespace)
			CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)

			// create M0.
			writtenDataHash0 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName0)
			WaitMantleBackupSynced(namespace, backupName0)

			// create M1.
			writtenDataHash1 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName1)
			WaitMantleBackupSynced(namespace, backupName1)

			// remove M1'.
			_, _, err := Kubectl(SecondaryK8sCluster, nil, "delete", "mb", "-n", namespace, backupName1)
			Expect(err).NotTo(HaveOccurred())

			// create M2.
			writtenDataHash2 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName2)
			WaitMantleBackupSynced(namespace, backupName2)

			primaryMB2, err := GetMB(PrimaryK8sCluster, namespace, backupName2)
			Expect(err).NotTo(HaveOccurred())
			secondaryMB2, err := GetMB(SecondaryK8sCluster, namespace, backupName2)
			Expect(err).NotTo(HaveOccurred())
			WaitTemporaryResourcesDeleted(ctx, primaryMB2, secondaryMB2)

			// Make sure M2 and M2' have the same contents.
			EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName2, restoreName2, writtenDataHash2)
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName2, restoreName2, writtenDataHash2)

			// Make sure M1 has the same contents.
			EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)

			// Make sure M0 and M0' have the same contents.
			EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)

			// Make sure M1' isn't re-created.
			mbList, err := GetObjectList[mantlev1.MantleBackupList](SecondaryK8sCluster, "mb", namespace)
			Expect(err).NotTo(HaveOccurred())
			Expect(slices.ContainsFunc(mbList.Items, func(mb mantlev1.MantleBackup) bool {
				return mb.GetName() == backupName1
			})).To(BeFalse())
		})

		It("should back up correctly if previous incremental MB is removed in the primary cluster", func(ctx SpecContext) {
			namespace := util.GetUniqueName("ns-")
			pvcName := util.GetUniqueName("pvc-")
			backupName0 := util.GetUniqueName("mb-")
			backupName1 := util.GetUniqueName("mb-")
			backupName2 := util.GetUniqueName("mb-")
			restoreName0 := util.GetUniqueName("mr-")
			restoreName1 := util.GetUniqueName("mr-")
			restoreName2 := util.GetUniqueName("mr-")

			SetupEnvironment(namespace)
			CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)

			// create M0.
			writtenDataHash0 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName0)
			WaitMantleBackupSynced(namespace, backupName0)

			// create M1.
			writtenDataHash1 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName1)
			WaitMantleBackupSynced(namespace, backupName1)

			// remove M1.
			_, _, err := Kubectl(PrimaryK8sCluster, nil, "delete", "mb", "-n", namespace, backupName1)
			Expect(err).NotTo(HaveOccurred())

			// create M2.
			writtenDataHash2 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName2)
			WaitMantleBackupSynced(namespace, backupName2)

			primaryMB2, err := GetMB(PrimaryK8sCluster, namespace, backupName2)
			Expect(err).NotTo(HaveOccurred())
			secondaryMB2, err := GetMB(SecondaryK8sCluster, namespace, backupName2)
			Expect(err).NotTo(HaveOccurred())
			WaitTemporaryResourcesDeleted(ctx, primaryMB2, secondaryMB2)

			// Make sure M2 and M2' have the same contents.
			EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName2, restoreName2, writtenDataHash2)
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName2, restoreName2, writtenDataHash2)

			// Make sure M1' has the same contents.
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)

			// Make sure M0 and M0' have the same contents.
			EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName0, restoreName0, writtenDataHash0)
		})

		It("should succeed to back up if backup-transfer-part-size is changed during uploading", func(ctx SpecContext) {
			namespace := util.GetUniqueName("ns-")
			pvcName := util.GetUniqueName("pvc-")
			backupName := util.GetUniqueName("mb-")
			restoreName := util.GetUniqueName("mr-")

			SetupEnvironment(namespace)

			// Pause the object storage to make upload Jobs fail.
			PauseObjectStorage(ctx)
			defer ResumeObjectStorage(ctx)

			// Create MantleBackup M0.
			CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)
			writtenDataHash := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName)

			// Wait until an upload Job is created.
			WaitUploadJobCreated(ctx, PrimaryK8sCluster, namespace, backupName, 0)

			// Get the expected number of the backup parts before changing backup-transfer-part-size.
			pvc, err := GetPVC(PrimaryK8sCluster, namespace, pvcName)
			Expect(err).NotTo(HaveOccurred())
			numParts, err := GetNumberOfBackupParts(pvc.Spec.Resources.Requests.Storage())
			Expect(err).NotTo(HaveOccurred())

			// Change backup-transfer-part-size
			originalBackupTransferPartSize, err := GetBackupTransferPartSize()
			Expect(err).NotTo(HaveOccurred())
			Expect(originalBackupTransferPartSize.String()).To(Equal("3Mi"))
			ChangeBackupTransferPartSize("7Mi")
			defer ChangeBackupTransferPartSize(originalBackupTransferPartSize.String())
			newNumParts, err := GetNumberOfBackupParts(pvc.Spec.Resources.Requests.Storage())
			Expect(err).NotTo(HaveOccurred())
			Expect(newNumParts).NotTo(Equal(numParts))

			// Resume the process.
			ResumeObjectStorage(ctx)

			// Wait for MB to be synced.
			WaitMantleBackupSynced(namespace, backupName)

			// Make sure backups are correct.
			EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName, restoreName, writtenDataHash)
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName, restoreName, writtenDataHash)
		})

		It("should succeed to back up two MantleBackups in parallel", func(ctx SpecContext) {
			namespace := util.GetUniqueName("ns-")
			pvcName1 := util.GetUniqueName("pvc-")
			pvcName2 := util.GetUniqueName("pvc-")
			backupName1 := util.GetUniqueName("mb-")
			backupName2 := util.GetUniqueName("mb-")
			restoreName1 := util.GetUniqueName("mr-")
			restoreName2 := util.GetUniqueName("mr-")

			SetupEnvironment(namespace)

			CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName1)
			CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName2)
			writtenDataHash1 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName1)
			writtenDataHash2 := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName2)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName1, backupName1)
			CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName2, backupName2)
			WaitMantleBackupSynced(namespace, backupName1)
			WaitMantleBackupSynced(namespace, backupName2)

			primaryMB1, err := GetMB(PrimaryK8sCluster, namespace, backupName1)
			Expect(err).NotTo(HaveOccurred())
			secondaryMB1, err := GetMB(SecondaryK8sCluster, namespace, backupName1)
			Expect(err).NotTo(HaveOccurred())
			WaitTemporaryResourcesDeleted(ctx, primaryMB1, secondaryMB1)

			primaryMB2, err := GetMB(PrimaryK8sCluster, namespace, backupName2)
			Expect(err).NotTo(HaveOccurred())
			secondaryMB2, err := GetMB(SecondaryK8sCluster, namespace, backupName2)
			Expect(err).NotTo(HaveOccurred())
			WaitTemporaryResourcesDeleted(ctx, primaryMB2, secondaryMB2)

			Expect(meta.IsStatusConditionTrue(secondaryMB1.Status.Conditions, "ReadyToUse")).To(BeTrue())
			Expect(meta.IsStatusConditionTrue(secondaryMB2.Status.Conditions, "ReadyToUse")).To(BeTrue())

			snap, err := FindRBDSnapshotInPVC(SecondaryK8sCluster, namespace, pvcName1, backupName1)
			Expect(err).NotTo(HaveOccurred())
			Expect(secondaryMB1.Status.SnapID).NotTo(BeNil())
			Expect(*secondaryMB1.Status.SnapID).To(Equal(snap.Id))
			Expect(snap.Name).To(Equal(secondaryMB1.Name))

			snap, err = FindRBDSnapshotInPVC(SecondaryK8sCluster, namespace, pvcName2, backupName2)
			Expect(err).NotTo(HaveOccurred())
			Expect(secondaryMB2.Status.SnapID).NotTo(BeNil())
			Expect(*secondaryMB2.Status.SnapID).To(Equal(snap.Id))
			Expect(snap.Name).To(Equal(secondaryMB2.Name))

			EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)
			EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName2, restoreName2, writtenDataHash2)
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName1, restoreName1, writtenDataHash1)
			EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName2, restoreName2, writtenDataHash2)
		})

		It("should succeed in backup even if part=0 upload Job completes after part=1 upload Job does",
			func(ctx SpecContext) {
				namespace := util.GetUniqueName("ns-")
				pvcName := util.GetUniqueName("pvc-")
				backupName := util.GetUniqueName("mb-")
				restoreName := util.GetUniqueName("mr-")
				partNumSlow := 0
				partNumFast := 1

				SetupEnvironment(namespace)

				script := fmt.Sprintf(`#!/bin/bash
s5cmd_path=$(which s5cmd)
s5cmd(){
	if [ ${PART_NUM} -eq %d ]; then
		sleep 60
	fi
	${s5cmd_path} "$@"
}
%s`, partNumSlow, controller.EmbedJobUploadScript)
				ChangeComponentJobScript(
					ctx,
					PrimaryK8sCluster,
					controller.EnvUploadJobScript,
					namespace,
					backupName,
					partNumSlow,
					&script,
				)

				CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)
				writtenDataHash := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
				CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName)

				By(fmt.Sprintf("waiting the situation where part=%d upload Job has not yet completed but "+
					"part=%d upload Job has already completed", partNumSlow, partNumFast))
				Eventually(ctx, func(g Gomega) {
					primaryMB, err := GetMB(PrimaryK8sCluster, namespace, backupName)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(meta.IsStatusConditionTrue(primaryMB.Status.Conditions, mantlev1.BackupConditionSyncedToRemote)).
						To(BeFalse())

					jobSlow, err := GetJob(PrimaryK8sCluster, CephClusterNamespace,
						controller.MakeUploadJobName(primaryMB, partNumSlow))
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(IsJobConditionTrue(jobSlow.Status.Conditions, batchv1.JobComplete)).To(BeFalse())

					jobFast, err := GetJob(PrimaryK8sCluster, CephClusterNamespace,
						controller.MakeUploadJobName(primaryMB, partNumFast))
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(IsJobConditionTrue(jobFast.Status.Conditions, batchv1.JobComplete)).To(BeTrue())
				}).Should(Succeed())

				WaitMantleBackupSynced(namespace, backupName)

				primaryMB, err := GetMB(PrimaryK8sCluster, namespace, backupName)
				Expect(err).NotTo(HaveOccurred())
				secondaryMB, err := GetMB(SecondaryK8sCluster, namespace, backupName)
				Expect(err).NotTo(HaveOccurred())
				WaitTemporaryResourcesDeleted(ctx, primaryMB, secondaryMB)

				EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName, restoreName, writtenDataHash)
				EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName, restoreName, writtenDataHash)
			},
		)

		DescribeTable("Backups should succeed even if component Jobs temporarily fail",
			func(
				ctx SpecContext,
				clusterOfJob int,
				envName,
				originalScript string,
				makeJobName func(backup *mantlev1.MantleBackup, partNum int) string,
				extractPartNumFromJobName func(jobName string, backup *mantlev1.MantleBackup) (int, bool),
			) {
				namespace := util.GetUniqueName("ns-")
				pvcName := util.GetUniqueName("pvc-")
				backupName := util.GetUniqueName("mb-")
				restoreName := util.GetUniqueName("mr-")
				partNumFailed := 1

				SetupEnvironment(namespace)

				// Make part=1 Job fail
				script := fmt.Sprintf(`#!/bin/bash
rbd_path=$(which rbd)
rbd(){
	if [ ${PART_NUM} -eq %d ]; then
		return 1
	else
		${rbd_path} "$@"
	fi
}

s5cmd_path=$(which s5cmd)
s5cmd(){
	if [ ${PART_NUM} -eq %d ]; then
		return 1
	else
		${s5cmd_path} "$@"
	fi
}
%s`, partNumFailed, partNumFailed, originalScript)
				ChangeComponentJobScript(ctx, clusterOfJob, envName, namespace, backupName, partNumFailed, &script)

				CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName)
				writtenDataHash := WriteRandomDataToPV(ctx, PrimaryK8sCluster, namespace, pvcName)
				CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName)

				By("waiting for the part=1 Job to fail")
				jobName := ""
				Eventually(ctx, func(g Gomega) {
					primaryMB, err := GetMB(PrimaryK8sCluster, namespace, backupName)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(meta.IsStatusConditionTrue(primaryMB.Status.Conditions, mantlev1.BackupConditionSyncedToRemote)).
						To(BeFalse())

					backup, err := GetMB(clusterOfJob, namespace, backupName)
					g.Expect(err).NotTo(HaveOccurred())
					jobName = makeJobName(backup, partNumFailed)

					job, err := GetJob(clusterOfJob, CephClusterNamespace, jobName)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(IsJobConditionTrue(job.Status.Conditions, batchv1.JobComplete)).To(BeFalse())
				}).Should(Succeed())

				By("ensuring the part=1 Job continues to fail")
				Consistently(ctx, func(g Gomega) {
					job, err := GetJob(clusterOfJob, CephClusterNamespace, jobName)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(IsJobConditionTrue(job.Status.Conditions, batchv1.JobComplete)).To(BeFalse())
				}, "10s", "1s").Should(Succeed())

				// Make part=1 Job succeed
				ChangeComponentJobScript(ctx, clusterOfJob, envName, namespace, backupName, partNumFailed, nil)

				By(fmt.Sprintf("deleting Jobs having part number larger than %d to proceed reconciliation", partNumFailed))
				backup, err := GetMB(clusterOfJob, namespace, backupName)
				Expect(err).NotTo(HaveOccurred())
				jobList, err := GetJobList(clusterOfJob, CephClusterNamespace)
				Expect(err).NotTo(HaveOccurred())
				for _, job := range jobList.Items {
					partNum, ok := extractPartNumFromJobName(job.GetName(), backup)
					if !ok || partNum < partNumFailed {
						continue
					}
					_, _, err = Kubectl(clusterOfJob, nil, "delete", "-n", CephClusterNamespace, "job", job.GetName())
					Expect(err).NotTo(HaveOccurred())
				}

				WaitMantleBackupSynced(namespace, backupName)

				primaryMB, err := GetMB(PrimaryK8sCluster, namespace, backupName)
				Expect(err).NotTo(HaveOccurred())
				secondaryMB, err := GetMB(SecondaryK8sCluster, namespace, backupName)
				Expect(err).NotTo(HaveOccurred())
				WaitTemporaryResourcesDeleted(ctx, primaryMB, secondaryMB)

				EnsureCorrectRestoration(PrimaryK8sCluster, ctx, namespace, backupName, restoreName, writtenDataHash)
				EnsureCorrectRestoration(SecondaryK8sCluster, ctx, namespace, backupName, restoreName, writtenDataHash)
			},

			Entry(
				"export Job",
				PrimaryK8sCluster,
				controller.EnvExportJobScript,
				controller.EmbedJobExportScript,
				controller.MakeExportJobName,
				controller.ExtractPartNumFromExportJobName,
			),

			Entry(
				"upload Job",
				PrimaryK8sCluster,
				controller.EnvUploadJobScript,
				controller.EmbedJobUploadScript,
				controller.MakeUploadJobName,
				controller.ExtractPartNumFromUploadJobName,
			),

			Entry(
				"import Job",
				SecondaryK8sCluster,
				controller.EnvImportJobScript,
				controller.EmbedJobImportScript,
				controller.MakeImportJobName,
				controller.ExtractPartNumFromImportJobName,
			),
		)

		It("should get metrics from the controller pod in the primary cluster", func(ctx SpecContext) {
			metrics := []string{
				`mantle_backup_creation_duration_seconds_count`,
				`mantle_backup_creation_duration_seconds_sum`,
			}
			ensureMetricsAreExposed(metrics)
		})
	})
}

func ensureMetricsAreExposed(metrics []string) {
	GinkgoHelper()
	controllerPod, err := GetControllerPodName(PrimaryK8sCluster)
	Expect(err).NotTo(HaveOccurred())

	stdout, _, err := Kubectl(PrimaryK8sCluster, nil, "exec", "-n", CephClusterNamespace, controllerPod, "--",
		"curl", "-s", "http://localhost:8080/metrics")
	Expect(err).NotTo(HaveOccurred())
	for _, metric := range metrics {
		Expect(strings.Contains(string(stdout), metric)).To(BeTrue())
	}
}
