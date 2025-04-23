package v1

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("MantleBackup controller", func() {
	var ns string
	var pv *corev1.PersistentVolume
	var pvc *corev1.PersistentVolumeClaim

	BeforeEach(func(ctx SpecContext) {
		ns = resMgr.CreateNamespace()
		var err error
		pv, pvc, err = resMgr.CreateUniquePVAndPVC(ctx, ns)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func(ctx SpecContext) {
		err := k8sClient.Delete(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: ns,
			},
		})
		Expect(err).NotTo(HaveOccurred())
	})

	It("should succeed when pvc does not have remote-uid annotation", func(ctx SpecContext) {
		va := storagev1.VolumeAttachment{
			Spec: storagev1.VolumeAttachmentSpec{
				Source: storagev1.VolumeAttachmentSource{
					PersistentVolumeName: &pv.Name,
				},
			},
		}
		_, err := validator.ValidateCreate(ctx, &va)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should fail when pvc has remote-uid annotation", func(ctx SpecContext) {
		pvc.Annotations = map[string]string{
			annotRemoteUID: "dummy",
		}
		err := k8sClient.Update(ctx, pvc)
		Expect(err).NotTo(HaveOccurred())

		va := storagev1.VolumeAttachment{
			Spec: storagev1.VolumeAttachmentSpec{
				Source: storagev1.VolumeAttachmentSource{
					PersistentVolumeName: &pv.Name,
				},
			},
		}
		_, err = validator.ValidateCreate(ctx, &va)
		Expect(err).To(HaveOccurred())
	})
})
