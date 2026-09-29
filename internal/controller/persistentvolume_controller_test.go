package controller

import (
	"github.com/cybozu-go/mantle/internal/ceph"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

var _ = Describe("PersistentVolumeReconciler", func() {
	DescribeTable("finalizing a restoring PV being deleted",
		func(
			ctx SpecContext,
			phase corev1.PersistentVolumePhase,
			claimUID types.UID,
			expectFinalized bool,
		) {
			mockCtrl := gomock.NewController(GinkgoT())
			cmdMock := ceph.NewMockCephCmd(mockCtrl)
			// The RBD image does not exist, so deleting it is a no-op.
			cmdMock.EXPECT().RBDLs(gomock.Any()).Return([]string{}, nil).AnyTimes()

			reconciler := NewPersistentVolumeReconciler(k8sClient, k8sClient.Scheme(), resMgr.ClusterID)
			reconciler.ceph = cmdMock

			pv := corev1.PersistentVolume{}
			pv.SetName("pv-" + rand.String(8))
			pv.SetFinalizers([]string{RestoringPVFinalizerName})
			pv.Spec.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}
			pv.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
			pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
			pv.Spec.ClaimRef = &corev1.ObjectReference{
				Kind:      "PersistentVolumeClaim",
				Namespace: "dummy-namespace",
				Name:      "dummy-pvc",
				UID:       claimUID,
			}
			pv.Spec.CSI = &corev1.CSIPersistentVolumeSource{
				Driver:       "rbd.csi.ceph.com",
				VolumeHandle: "dummy-image",
				VolumeAttributes: map[string]string{
					"clusterID": resMgr.ClusterID,
					"pool":      "dummy-pool",
				},
			}
			Expect(k8sClient.Create(ctx, &pv)).To(Succeed())
			pv.Status.Phase = phase
			Expect(k8sClient.Status().Update(ctx, &pv)).To(Succeed())
			Expect(k8sClient.Delete(ctx, &pv)).To(Succeed())

			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(&pv)})
			Expect(err).NotTo(HaveOccurred())

			err = k8sClient.Get(ctx, client.ObjectKeyFromObject(&pv), &pv)
			if expectFinalized {
				Expect(err).To(Satisfy(func(err error) bool {
					return err != nil || !controllerutil.ContainsFinalizer(&pv, RestoringPVFinalizerName)
				}))
			} else {
				Expect(err).NotTo(HaveOccurred())
				Expect(controllerutil.ContainsFinalizer(&pv, RestoringPVFinalizerName)).To(BeTrue())

				// Clean up the PV.
				controllerutil.RemoveFinalizer(&pv, RestoringPVFinalizerName)
				Expect(k8sClient.Update(ctx, &pv)).To(Succeed())
			}
		},
		Entry("released PV", corev1.VolumeReleased, types.UID("dummy-uid"), true),
		// The PVC was deleted before it was bound to the PV. Such a PV never
		// becomes Released, and it can't be bound to any PVCs because it's
		// being deleted.
		Entry("available PV that has never been bound", corev1.VolumeAvailable, types.UID(""), true),
		Entry("bound PV", corev1.VolumeBound, types.UID("dummy-uid"), false),
		// The PV is being bound to the PVC.
		Entry("available PV with the claim's UID", corev1.VolumeAvailable, types.UID("dummy-uid"), false),
	)
})
