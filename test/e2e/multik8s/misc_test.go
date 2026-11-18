package multik8s

import (
	"fmt"
	"strings"

	. "github.com/cybozu-go/mantle/test/e2e/multik8s/testutil"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("metrics tests", func() {
	namespace := util.GetUniqueName("ns-")
	pvcName := util.GetUniqueName("pvc-")
	backupName := util.GetUniqueName("mb-")
	backupConfigName := util.GetUniqueName("mbc-")

	It("should setup", func(ctx SpecContext) {
		SetupNamespaces(namespace)
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName, SCName1)
		CreateMantleBackup(PrimaryK8sCluster, namespace, pvcName, backupName)
		WaitMantleBackupSynced(namespace, backupName)
		CreateMantleBackupConfig(PrimaryK8sCluster, namespace, pvcName, backupConfigName)
	})

	DescribeTable("metrics should be exposed",
		func(ctx SpecContext, metricName string) {
			Eventually(ctx, func(g Gomega) {
				controllerPod, err := GetControllerPodName(PrimaryK8sCluster)
				g.Expect(err).NotTo(HaveOccurred())
				stdout, _, err := Kubectl(PrimaryK8sCluster, nil, "exec", "-n", CephCluster1Namespace, controllerPod, "--",
					"curl", "-s", "http://localhost:8080/metrics")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.Contains(string(stdout), metricName)).To(BeTrue())
			}).Should(Succeed())
		},
		Entry(`mantle_mantlebackupconfig_info`, `mantle_mantlebackupconfig_info`),
		Entry(`mantle_backup_sync_duration_seconds_bucket`, `mantle_backup_sync_duration_seconds_bucket`),
		Entry(`mantle_backup_sync_duration_seconds_sum`, `mantle_backup_sync_duration_seconds_sum`),
		Entry(`mantle_backup_sync_duration_seconds_count`, `mantle_backup_sync_duration_seconds_count`),
		Entry(`mantle_backup_snapshot_duration_seconds_bucket`, `mantle_backup_snapshot_duration_seconds_bucket`),
		Entry(`mantle_backup_snapshot_duration_seconds_sum`, `mantle_backup_snapshot_duration_seconds_sum`),
		Entry(`mantle_backup_snapshot_duration_seconds_count`, `mantle_backup_snapshot_duration_seconds_count`),
		Entry(`mantle_backup_snapshot_duration_seconds_count of the PVC`,
			fmt.Sprintf(`mantle_backup_snapshot_duration_seconds_count{persistentvolumeclaim=%q,resource_namespace=%q}`,
				pvcName, namespace)),
		Entry(`mantle_backup_exported_diff_size_bytes`, `mantle_backup_exported_diff_size_bytes`),
		Entry(`mantle_backup_last_creation_timestamp_seconds of the PVC`,
			fmt.Sprintf(`mantle_backup_last_creation_timestamp_seconds{persistentvolumeclaim=%q,resource_namespace=%q}`,
				pvcName, namespace)),
		Entry(`mantle_backup_last_snapshot_creation_timestamp_seconds of the PVC`,
			fmt.Sprintf(`mantle_backup_last_snapshot_creation_timestamp_seconds{persistentvolumeclaim=%q,resource_namespace=%q}`,
				pvcName, namespace)),
	)

	// This metric is only exported while a backup is unsynced, so the entries above cannot check it.
	It("should expose mantle_backup_oldest_unsynced_creation_timestamp_seconds only while a backup is unsynced",
		func(ctx SpecContext) {
			unsyncedPVCName := util.GetUniqueName("pvc-")
			unsyncedBackupName := util.GetUniqueName("mb-")
			metricName := fmt.Sprintf(
				`mantle_backup_oldest_unsynced_creation_timestamp_seconds{persistentvolumeclaim=%q,resource_namespace=%q}`,
				unsyncedPVCName, namespace)
			getMetrics := func(g Gomega) string {
				controllerPod, err := GetControllerPodName(PrimaryK8sCluster)
				g.Expect(err).NotTo(HaveOccurred())
				stdout, _, err := Kubectl(PrimaryK8sCluster, nil, "exec", "-n", CephCluster1Namespace, controllerPod, "--",
					"curl", "-s", "http://localhost:8080/metrics")
				g.Expect(err).NotTo(HaveOccurred())

				return string(stdout)
			}

			// Pause the object storage, so that the backup stops before it is synced.
			PauseObjectStorage(ctx)
			defer ResumeObjectStorage(ctx)

			CreatePVC(ctx, PrimaryK8sCluster, namespace, unsyncedPVCName, SCName1)
			CreateMantleBackup(PrimaryK8sCluster, namespace, unsyncedPVCName, unsyncedBackupName)
			WaitMantleBackupSnapshotCaptured(PrimaryK8sCluster, namespace, unsyncedBackupName)

			Eventually(ctx, func(g Gomega) {
				g.Expect(strings.Contains(getMetrics(g), metricName)).To(BeTrue())
			}).Should(Succeed())

			ResumeObjectStorage(ctx)
			WaitMantleBackupSynced(namespace, unsyncedBackupName)

			Eventually(ctx, func(g Gomega) {
				g.Expect(strings.Contains(getMetrics(g), metricName)).To(BeFalse())
			}).Should(Succeed())
		})
})

var _ = Describe("miscellaneous tests", func() {
	It("should succeed to back up two MantleBackups in parallel", func(ctx SpecContext) {
		namespace := util.GetUniqueName("ns-")
		pvcName1 := util.GetUniqueName("pvc-")
		pvcName2 := util.GetUniqueName("pvc-")
		backupName1 := util.GetUniqueName("mb-")
		backupName2 := util.GetUniqueName("mb-")
		restoreName1 := util.GetUniqueName("mr-")
		restoreName2 := util.GetUniqueName("mr-")

		SetupNamespaces(namespace)

		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName1, SCName1)
		CreatePVC(ctx, PrimaryK8sCluster, namespace, pvcName2, SCName1)
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

		Expect(secondaryMB1.IsSnapshotCaptured()).To(BeTrue())
		Expect(secondaryMB2.IsSnapshotCaptured()).To(BeTrue())

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
})
