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
	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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

					// Check if .status.LargestCompleted{Export,Import,Upload}PartNum are correct
					backupTransferPartSize, err := GetBackupTransferPartSize()
					if err != nil {
						return fmt.Errorf("failed to get backup transfer part size :%w", err)
					}
					pvcSize := primaryPVC.Spec.Resources.Requests.Storage()
					expectedNumOfParts := 1
					if pvcSize.Cmp(*backupTransferPartSize) > 0 { // pvcSize > backupTransferPartSize
						backupTransferPartSize, ok := backupTransferPartSize.AsInt64()
						if !ok {
							return errors.New("failed to convert backup transfer part size to i64")
						}
						pvcSizeI64, ok := pvcSize.AsInt64()
						if !ok {
							return errors.New("failed to convert pvc size to i64")
						}
						expectedNumOfParts = int(pvcSizeI64 / backupTransferPartSize)
						if pvcSizeI64%backupTransferPartSize != 0 {
							expectedNumOfParts++
						}
					}

					if primaryMB.Status.LargestCompletedExportPartNum == nil {
						return fmt.Errorf(
							".status.largestCompletedExportPartNum of the primary MB is not correct: expected %d: got nil",
							expectedNumOfParts,
						)
					} else if *primaryMB.Status.LargestCompletedExportPartNum != expectedNumOfParts-1 {
						return fmt.Errorf(
							".status.largestCompletedExportPartNum of the primary MB is not correct: expected %d: got %d",
							expectedNumOfParts,
							*primaryMB.Status.LargestCompletedExportPartNum,
						)
					}

					if primaryMB.Status.LargestCompletedUploadPartNum == nil {
						return fmt.Errorf(
							".status.largestCompletedUploadPartNum of the primary MB is not correct: expected %d: got nil",
							expectedNumOfParts,
						)
					} else if *primaryMB.Status.LargestCompletedUploadPartNum != expectedNumOfParts-1 {
						return fmt.Errorf(
							".status.largestCompletedUploadPartNum of the primary MB is not correct: expected %d: got %d",
							expectedNumOfParts,
							*primaryMB.Status.LargestCompletedUploadPartNum,
						)
					}

					if secondaryMB.Status.LargestCompletedImportPartNum == nil {
						return fmt.Errorf(
							".status.largestCompletedImportPartNum of the secondary MB is not correct: expected %d: got nil",
							expectedNumOfParts,
						)
					} else if *secondaryMB.Status.LargestCompletedImportPartNum != expectedNumOfParts-1 {
						return fmt.Errorf(
							".status.largestCompletedImportPartNum of the secondary MB is not correct: expected %d: got %d",
							expectedNumOfParts,
							*secondaryMB.Status.LargestCompletedImportPartNum,
						)
					}

					return nil
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
