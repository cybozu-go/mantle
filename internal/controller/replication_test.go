package controller

import (
	"context"
	"encoding/json"
	"net"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/ceph"
	"github.com/cybozu-go/mantle/internal/testutil"
	"github.com/cybozu-go/mantle/pkg/controller/proto"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
)

const (
	replicationTestEndpoint = "localhost:50051"
)

type replicationUnitTest struct {
	mgrUtil testutil.ManagerUtil
	server  *grpc.Server
	conn    *grpc.ClientConn
	client  proto.MantleServiceClient
	ns      string
}

var _ = Describe("Replication unit tests", func() {
	var test replicationUnitTest

	BeforeEach(func(ctx SpecContext) {
		By("prepares Replication server", func() {
			test.mgrUtil = testutil.NewManagerUtil(context.Background(), cfg, scheme.Scheme)
			test.ns = resMgr.CreateNamespace()

			test.server = grpc.NewServer()
			proto.RegisterMantleServiceServer(test.server, NewSecondaryServer(k8sClient))
			l, err := net.Listen("tcp", replicationTestEndpoint)
			Expect(err).NotTo(HaveOccurred())

			go func() {
				err := test.server.Serve(l)
				Expect(err).NotTo(HaveOccurred())
			}()

			test.mgrUtil.Start()
		})

		By("creates gRPC client", func() {
			var err error
			test.conn, err = grpc.NewClient(
				replicationTestEndpoint,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			Expect(err).NotTo(HaveOccurred())

			test.client = proto.NewMantleServiceClient(test.conn)
		})
	})

	AfterEach(func(ctx SpecContext) {
		By("closes gRPC client", func() {
			err := test.conn.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		By("stops server", func() {
			test.server.GracefulStop()

			test.mgrUtil.Stop()
		})
	})

	Context("CreateUpdatePVC", test.testCreateUpdatePVCAfterResizing)
	Context("CreateMantleBackup", test.testCreateMantleBackup)
})

func (test *replicationUnitTest) testCreateUpdatePVCAfterResizing() {
	// CSATEST-1492
	It("calls CreateOrUpdatePVC twice with the different PVC sizes and should not update the PVC size", func() {
		ctx := context.Background()

		srcPVC := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pvc",
				Namespace: test.ns,
				Annotations: map[string]string{
					annotRemoteUID: "test-uid",
					"dummy":        "test-1",
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("1Gi"),
					},
				},
				StorageClassName: ptr.To("test-sc"),
			},
		}

		By("creating a PVC")
		pvcRaw, err := json.Marshal(srcPVC)
		Expect(err).NotTo(HaveOccurred())
		res, err := test.client.CreateOrUpdatePVC(ctx, &proto.CreateOrUpdatePVCRequest{
			Pvc: pvcRaw,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res).NotTo(BeNil())
		uid := res.Uid

		By("updating the PVC with the different storage size")
		// this annotation is to verify that fields besides PVC size are updated
		srcPVC.Annotations["dummy"] = "test-2"
		srcPVC.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("2Gi")
		pvcRaw, err = json.Marshal(srcPVC)
		Expect(err).NotTo(HaveOccurred())
		res, err = test.client.CreateOrUpdatePVC(ctx, &proto.CreateOrUpdatePVCRequest{
			Pvc: pvcRaw,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res).NotTo(BeNil())

		Expect(res.Uid).To(Equal(uid))

		By("getting PVC")
		pvc := &corev1.PersistentVolumeClaim{}
		err = k8sClient.Get(ctx, types.NamespacedName{
			Namespace: srcPVC.Namespace,
			Name:      srcPVC.Name,
		}, pvc)
		Expect(err).NotTo(HaveOccurred())

		Expect(pvc.Annotations["dummy"]).To(Equal("test-2"))
		// the storage size should not be updated
		Expect(pvc.Spec.Resources.Requests.Storage().String()).To(Equal("1Gi"))
	})
}

func (test *replicationUnitTest) newMantleBackup() *mantlev1.MantleBackup {
	GinkgoHelper()
	snapshot := ceph.RBDSnapshot{}
	err := json.Unmarshal(
		[]byte(`{"id":3,"name":"snap1","size":524288000,"protected":"false","timestamp":"Fri May  9 07:58:12 2025"}`),
		&snapshot,
	)
	Expect(err).NotTo(HaveOccurred())

	return &mantlev1.MantleBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.GetUniqueName("test-mb-"),
			Namespace: test.ns,
			Labels: map[string]string{
				labelRemoteBackupTargetPVCUID: util.GetUniqueName("test-remote-pvc-uid-"),
				labelLocalBackupTargetPVCUID:  util.GetUniqueName("test-local-pvc-uid-"),
			},
			Annotations: map[string]string{
				annotRemoteUID: util.GetUniqueName("test-remote-uid-"),
			},
		},
		Spec: mantlev1.MantleBackupSpec{
			PVC:    util.GetUniqueName("test-pvc-"),
			Expire: "1d",
		},
		Status: mantlev1.MantleBackupStatus{
			CreatedAt:        metav1.Time(snapshot.Timestamp),
			SnapSize:         ptr.To(snapshot.Size),
			TransferPartSize: ptr.To(resource.MustParse("1Gi")),
		},
	}
}

func (test *replicationUnitTest) expectPersistedMantleBackup(ctx SpecContext, expected *mantlev1.MantleBackup) {
	GinkgoHelper()

	actual := &mantlev1.MantleBackup{}
	err := k8sClient.Get(ctx, types.NamespacedName{
		Namespace: expected.Namespace,
		Name:      expected.Name,
	}, actual)
	Expect(err).NotTo(HaveOccurred())

	Expect(actual.Labels).To(Equal(expected.Labels))
	Expect(actual.Annotations).To(Equal(expected.Annotations))
	Expect(actual.Spec.PVC).To(Equal(expected.Spec.PVC))
	Expect(actual.Spec.Expire).To(Equal(expected.Spec.Expire))
	Expect(actual.Status.CreatedAt.Equal(&expected.Status.CreatedAt)).To(BeTrue())
	Expect(actual.Status.SnapSize).To(Equal(expected.Status.SnapSize))
	Expect(actual.Status.TransferPartSize).To(Equal(expected.Status.TransferPartSize))
}

func (test *replicationUnitTest) testCreateMantleBackup() {
	It("should create MantleBackup with statuses", func(ctx SpecContext) {
		// Arrange
		mb := test.newMantleBackup()
		mbJson, err := json.Marshal(mb)
		Expect(err).NotTo(HaveOccurred())

		// Act
		_, err = test.client.CreateMantleBackup(ctx, &proto.CreateMantleBackupRequest{
			MantleBackup: mbJson,
		})
		Expect(err).NotTo(HaveOccurred())

		// Assert
		test.expectPersistedMantleBackup(ctx, mb)
	})

	It("should be idempotent", func(ctx SpecContext) {
		// Arrange
		mb := test.newMantleBackup()
		mbJson, err := json.Marshal(mb)
		Expect(err).NotTo(HaveOccurred())

		_, err = test.client.CreateMantleBackup(ctx, &proto.CreateMantleBackupRequest{
			MantleBackup: mbJson,
		})
		Expect(err).NotTo(HaveOccurred())

		// Act
		_, err = test.client.CreateMantleBackup(ctx, &proto.CreateMantleBackupRequest{
			MantleBackup: mbJson,
		})
		Expect(err).NotTo(HaveOccurred())

		// Assert
		test.expectPersistedMantleBackup(ctx, mb)
	})

	It("should not update MantleBackup if it already exists", func(ctx SpecContext) {
		// Arrange
		mb := test.newMantleBackup()
		mbJson, err := json.Marshal(mb)
		Expect(err).NotTo(HaveOccurred())

		_, err = test.client.CreateMantleBackup(ctx, &proto.CreateMantleBackupRequest{
			MantleBackup: mbJson,
		})
		Expect(err).NotTo(HaveOccurred())

		// Modify the MantleBackup object
		origMB := mb.DeepCopy()
		mb.Status.TransferPartSize = ptr.To(resource.MustParse("2Gi"))
		Expect(mb.Status.TransferPartSize).NotTo(Equal(origMB.Status.TransferPartSize))
		mbJson, err = json.Marshal(mb)
		Expect(err).NotTo(HaveOccurred())

		// Act
		_, err = test.client.CreateMantleBackup(ctx, &proto.CreateMantleBackupRequest{
			MantleBackup: mbJson,
		})
		Expect(err).NotTo(HaveOccurred())

		// Assert
		test.expectPersistedMantleBackup(ctx, origMB)
	})

	It("should set status if MantleBackup already exists without status", func(ctx SpecContext) {
		// Arrange
		mb := test.newMantleBackup()
		mb2 := mb.DeepCopy()
		mb2.Status = mantlev1.MantleBackupStatus{} // Clear status
		err := k8sClient.Create(ctx, mb2)
		Expect(err).NotTo(HaveOccurred())

		mbJson, err := json.Marshal(mb)
		Expect(err).NotTo(HaveOccurred())

		// Act
		_, err = test.client.CreateMantleBackup(ctx, &proto.CreateMantleBackupRequest{
			MantleBackup: mbJson,
		})
		Expect(err).NotTo(HaveOccurred())

		// Assert
		test.expectPersistedMantleBackup(ctx, mb)
	})

	It("should reject PVC UID label different from existing one", func(ctx SpecContext) {
		// Arrange
		mb := test.newMantleBackup()
		mbJson, err := json.Marshal(mb)
		Expect(err).NotTo(HaveOccurred())

		_, err = test.client.CreateMantleBackup(ctx, &proto.CreateMantleBackupRequest{
			MantleBackup: mbJson,
		})
		Expect(err).NotTo(HaveOccurred())

		// Modify the PVC UID label
		mb.Labels[labelLocalBackupTargetPVCUID] = "different-uid"
		mbJson, err = json.Marshal(mb)
		Expect(err).NotTo(HaveOccurred())

		// Act
		_, err = test.client.CreateMantleBackup(ctx, &proto.CreateMantleBackupRequest{
			MantleBackup: mbJson,
		})

		// Assert
		Expect(err).To(HaveOccurred())
	})

	It("should reject remote UID annotation different from existing one", func(ctx SpecContext) {
		// Arrange
		mb := test.newMantleBackup()
		mbJson, err := json.Marshal(mb)
		Expect(err).NotTo(HaveOccurred())

		_, err = test.client.CreateMantleBackup(ctx, &proto.CreateMantleBackupRequest{
			MantleBackup: mbJson,
		})
		Expect(err).NotTo(HaveOccurred())

		// Modify the remote UID annotation
		mb.Annotations[annotRemoteUID] = "different-uid"
		mbJson, err = json.Marshal(mb)
		Expect(err).NotTo(HaveOccurred())

		// Act
		_, err = test.client.CreateMantleBackup(ctx, &proto.CreateMantleBackupRequest{
			MantleBackup: mbJson,
		})

		// Assert
		Expect(err).To(HaveOccurred())
	})
}
