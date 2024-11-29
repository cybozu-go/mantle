package controller

import (
	"time"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

var _ = Describe("garbage collector", func() {
	var err error

	Context("isMantleRestoreAlreadyDeleted", func() {
		doTest :=
			func(
				ctx SpecContext,
				existMR bool,
				pvModifier func(pv *corev1.PersistentVolume),
				expectError, expectDeleted bool,
			) {
				runner := NewGarbageCollectorRunner(k8sClient, 1*time.Second, "rook-ceph")
				ns := resMgr.CreateNamespace()

				var mr mantlev1.MantleRestore
				mr.SetName("mr")
				mr.SetNamespace(ns)
				mr.Spec.Backup = "dummy"
				if existMR {
					err = k8sClient.Create(ctx, &mr)
					Expect(err).NotTo(HaveOccurred())
					err = k8sClient.Get(ctx, types.NamespacedName{Name: mr.GetName(), Namespace: mr.GetNamespace()}, &mr)
					Expect(err).NotTo(HaveOccurred())
				}

				var pv corev1.PersistentVolume
				pv.SetAnnotations(map[string]string{
					PVAnnotationRestoredBy:          string(mr.GetUID()),
					PVAnnotationRestoredByName:      "mr",
					PVAnnotationRestoredByNamespace: ns,
				})
				pv.Spec.CSI = &corev1.CSIPersistentVolumeSource{
					VolumeAttributes: map[string]string{"clusterID": "rook-ceph"},
				}
				if pvModifier != nil {
					pvModifier(&pv)
				}

				deleted, err := runner.isMantleRestoreAlreadyDeleted(ctx, &pv)
				if expectError {
					Expect(err).To(HaveOccurred())
				} else {
					Expect(err).NotTo(HaveOccurred())
				}
				Expect(deleted).To(Equal(expectDeleted))
			}

		DescribeTable("isMantleRestoreAlreadyDeleted",
			doTest,
			Entry("MantleRestore exists", true, nil, false, false),
			Entry("MantleRestore does NOT exist", false, nil, false, true),
			Entry(
				"MantleRestore exists, but the PV's annotation has unexpected MR's UID",
				true,
				func(pv *corev1.PersistentVolume) {
					pv.Annotations[PVAnnotationRestoredBy] = "unexpected-uid"
				},
				false, true,
			),
			Entry(
				"PV annotation of restored-by missing",
				true,
				func(pv *corev1.PersistentVolume) {
					delete(pv.Annotations, PVAnnotationRestoredBy)
				},
				true, false,
			),
			Entry(
				"PV annotation of restored-by-name missing",
				true,
				func(pv *corev1.PersistentVolume) {
					delete(pv.Annotations, PVAnnotationRestoredByName)
				},
				true, false,
			),
			Entry(
				"PV annotation of restored-by-namespace missing",
				true,
				func(pv *corev1.PersistentVolume) {
					delete(pv.Annotations, PVAnnotationRestoredByNamespace)
				},
				true, false,
			),
			Entry(
				"cluster ID does not match",
				true,
				func(pv *corev1.PersistentVolume) {
					pv.Spec.CSI.VolumeAttributes["clusterID"] = "different-cluster-id"
				},
				false, false,
			),
		)
	})

	Context("deleteOrphanedPVs", func() {
		It("should remove only orphaned PVs", func(ctx SpecContext) {
			runner := NewGarbageCollectorRunner(k8sClient, 1*time.Second, resMgr.ClusterID)
			ns := resMgr.CreateNamespace()

			// Create MantleRestore mr1
			mr1 := mantlev1.MantleRestore{}
			mr1.SetName("mr1")
			mr1.SetNamespace(ns)
			mr1.Spec.Backup = "dummy"
			err = k8sClient.Create(ctx, &mr1)
			Expect(err).NotTo(HaveOccurred())
			err = k8sClient.Get(ctx, types.NamespacedName{Name: mr1.GetName(), Namespace: mr1.GetNamespace()}, &mr1)
			Expect(err).NotTo(HaveOccurred())

			// Create PV pv1 referring to mr1
			pv1, _, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
			Expect(err).NotTo(HaveOccurred())
			pv1.SetLabels(map[string]string{
				labelRestoringPVKey: labelRestoringPVValue,
			})
			pv1.SetAnnotations(map[string]string{
				PVAnnotationRestoredBy:          string(mr1.GetUID()),
				PVAnnotationRestoredByName:      mr1.GetName(),
				PVAnnotationRestoredByNamespace: mr1.GetNamespace(),
			})
			err = k8sClient.Update(ctx, pv1)
			Expect(err).NotTo(HaveOccurred())

			// Create PV pv2 not referring to any MantleRestores
			pv2, _, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
			Expect(err).NotTo(HaveOccurred())
			pv2.SetLabels(map[string]string{
				labelRestoringPVKey: labelRestoringPVValue,
			})
			pv2.SetAnnotations(map[string]string{
				PVAnnotationRestoredBy:          "non-existing-uid",
				PVAnnotationRestoredByName:      "non-existing-name",
				PVAnnotationRestoredByNamespace: "non-existing-namespace",
			})
			err = k8sClient.Update(ctx, pv2)
			Expect(err).NotTo(HaveOccurred())

			// No MantleRestore referring to pv2 exists, so pv2 is orphaned and should be removed.

			// Perform deleteOrphanedPVs.
			err = runner.deleteOrphanedPVs(ctx)
			Expect(err).NotTo(HaveOccurred())

			// pv1 should NOT be deleted.
			err = k8sClient.Get(ctx, types.NamespacedName{Name: pv1.GetName()}, pv1)
			Expect(err).NotTo(HaveOccurred())
			Expect(pv1.DeletionTimestamp.IsZero()).To(BeTrue())

			// pv2 should be deleted.
			err = k8sClient.Get(ctx, types.NamespacedName{Name: pv2.GetName()}, pv2)
			Expect(err).NotTo(HaveOccurred())
			Expect(pv2.DeletionTimestamp.IsZero()).To(BeFalse())
		})
	})
})
