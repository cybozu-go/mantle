package controller

import (
	"github.com/cybozu-go/mantle/internal/ceph"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("util.createCloneByPV", func() {
	DescribeTable("matrix test",
		func(ctx SpecContext,
			pv *corev1.PersistentVolume, snapshotName, cloneName string, // arguments
			lsResult []string, // if lsResult is nil, ls is not called
			infoResult *ceph.RBDImageInfo, // if infoResult is nil, info is not called
			expectedPool, expectedImage, expectedFeature string, // if expectedFeature is empty, clone is not called
			expectedErr bool,
		) {
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()

			cmdMock := ceph.NewMockCephCmd(ctrl)
			if lsResult != nil {
				cmdMock.EXPECT().RBDLs(expectedPool).Return(lsResult, nil)
			}
			if infoResult != nil {
				cmdMock.EXPECT().RBDInfo(expectedPool, cloneName).Return(infoResult, nil)
			}
			if len(expectedFeature) > 0 {
				cmdMock.EXPECT().RBDClone(expectedPool, expectedImage, snapshotName, cloneName, expectedFeature).Return(nil)
			}
			err := createCloneByPV(ctx, cmdMock, pv, snapshotName, cloneName)
			if expectedErr {
				Expect(err).To(HaveOccurred())
			} else {
				Expect(err).NotTo(HaveOccurred())
			}
		},
		Entry("clone does not exist, create clone",
			&corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							VolumeAttributes: map[string]string{
								"imageName": "bkImage",
								"pool":      "bkPool",
							},
						},
					},
				},
			},
			"bkSnap", "cloneImage",
			[]string{"bkImage"},                 // lsResult
			nil,                                 // RBDInfo is not called
			"bkPool", "bkImage", "deep-flatten", // expected args for RBDClone
			false,
		),
		Entry("clone exists and is clone of the snapshot, do nothing",
			&corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							VolumeAttributes: map[string]string{
								"imageName": "bkImage",
								"pool":      "bkPool",
							},
						},
					},
				},
			},
			"bkSnap", "cloneImage",
			[]string{"bkImage", "cloneImage"}, // cloneImage already exists
			&ceph.RBDImageInfo{ // infoResult
				Parent: &ceph.RBDImageInfoParent{
					Pool:     "bkPool",
					Image:    "bkImage",
					Snapshot: "bkSnap",
				},
			},
			"bkPool", "bkImage", "", // RBDClone will not be called
			false,
		),
		Entry("clone exists but is not clone of the snapshot, error",
			&corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							VolumeAttributes: map[string]string{
								"imageName": "bkImage",
								"pool":      "bkPool",
							},
						},
					},
				},
			},
			"bkSnap", "cloneImage",
			[]string{"bkImage", "cloneImage"}, // cloneImage already exists
			&ceph.RBDImageInfo{
				Parent: &ceph.RBDImageInfoParent{
					Pool:     "bkPool",
					Image:    "bkImage",
					Snapshot: "otherSnap", // different snapshot
				},
			},
			"bkPool", "bkImage", "", // RBDClone will not be called
			true,
		),
		Entry("having imageFeatures",
			&corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							VolumeAttributes: map[string]string{
								"imageName":     "bkImage",
								"pool":          "bkPool",
								"imageFeatures": "layering", // existing features
							},
						},
					},
				},
			},
			"bkSnap", "cloneImage",
			[]string{"bkImage"},                          // lsResult
			nil,                                          // RBDInfo is not called
			"bkPool", "bkImage", "layering,deep-flatten", // expected args for RBDClone
			false,
		),
		Entry("missing imageName in PV manifest",
			&corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							VolumeAttributes: map[string]string{
								"pool": "bkPool",
							},
						},
					},
				},
			},
			"bkSnap", "cloneImage",
			nil,        // RBDLs is not called
			nil,        // RBDInfo is not called
			"", "", "", // RBDClone will not be called
			true,
		),
		Entry("missing pool in PV manifest",
			&corev1.PersistentVolume{
				Spec: corev1.PersistentVolumeSpec{
					PersistentVolumeSource: corev1.PersistentVolumeSource{
						CSI: &corev1.CSIPersistentVolumeSource{
							VolumeAttributes: map[string]string{
								"imageName": "bkImage",
							},
						},
					},
				},
			},
			"bkSnap", "cloneImage",
			nil,        // RBDLs is not called
			nil,        // RBDInfo is not called
			"", "", "", // RBDClone will not be called
			true,
		),
	)
})

var _ = Describe("util.getCephClusterIDFromPVC", func() {
	var namespace string
	storageClassName := util.GetUniqueName("sc-")

	It("should create namespace", func() {
		namespace = resMgr.CreateNamespace()
	})

	DescribeTable("matrix test",
		func(ctx SpecContext, sc *storagev1.StorageClass, pvc *corev1.PersistentVolumeClaim, expectedClusterID string) {
			// create resources
			if sc != nil {
				err := k8sClient.Create(ctx, sc)
				Expect(err).NotTo(HaveOccurred())
			}
			pvc.Namespace = namespace
			err := k8sClient.Create(ctx, pvc)
			Expect(err).NotTo(HaveOccurred())

			// test main
			clusterID, err := getCephClusterIDFromPVC(ctx, k8sClient, pvc)
			Expect(err).NotTo(HaveOccurred())
			Expect(clusterID).To(Equal(expectedClusterID))

			// cleanup
			if sc != nil {
				err := k8sClient.Delete(ctx, sc)
				Expect(err).NotTo(HaveOccurred())
			}
			err = k8sClient.Delete(ctx, pvc)
			Expect(err).NotTo(HaveOccurred())
		},
		Entry("can get clusterID",
			&storagev1.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: storageClassName,
				},
				Provisioner: "test-ceph-cluster.rbd.csi.ceph.com",
				Parameters: map[string]string{
					"clusterID": "test-ceph-cluster",
				},
			},
			&corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: util.GetUniqueName("pvc-"),
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: *resource.NewQuantity(1, resource.BinarySI),
						},
					},
					StorageClassName: &storageClassName,
				},
			},
			"test-ceph-cluster"),
		Entry("no storage class",
			nil,
			&corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pvc",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: *resource.NewQuantity(1, resource.BinarySI),
						},
					},
				},
			},
			""),
		Entry("storage class is not managed by RBD",
			&storagev1.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: storageClassName,
				},
				Provisioner: "topolvm.cybozu.com",
			},
			&corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: util.GetUniqueName("pvc-"),
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: *resource.NewQuantity(1, resource.BinarySI),
						},
					},
					StorageClassName: &storageClassName,
				},
			},
			""),
		Entry("clusterID is not set",
			&storagev1.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: storageClassName,
				},
				Provisioner: "test-ceph-cluster.rbd.csi.ceph.com",
			},
			&corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: util.GetUniqueName("pvc-"),
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: *resource.NewQuantity(1, resource.BinarySI),
						},
					},
					StorageClassName: &storageClassName,
				},
			},
			""),
	)
})
