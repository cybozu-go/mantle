package controller

import (
	"context"
	"encoding/json"
	"net"

	"github.com/cybozu-go/mantle/internal/testutil"
	"github.com/cybozu-go/mantle/pkg/controller/proto"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
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

	Describe("setup environment", test.setupEnv)
	Describe("test CreateUpdatePVC", test.testCreateUpdatePVCAfterResizing)
	Describe("tearDown environment", test.tearDownEnv)
})

func (test *replicationUnitTest) setupEnv() {
	It("prepares Replication server", func() {
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

	It("creates gRPC client", func() {
		var err error
		test.conn, err = grpc.NewClient(
			replicationTestEndpoint,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		Expect(err).NotTo(HaveOccurred())

		test.client = proto.NewMantleServiceClient(test.conn)
	})
}

func (test *replicationUnitTest) tearDownEnv() {
	It("closes gRPC client", func() {
		err := test.conn.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("stops server", func() {
		test.server.GracefulStop()

		err := test.mgrUtil.Stop()
		Expect(err).NotTo(HaveOccurred())
	})
}

func (test *replicationUnitTest) testCreateUpdatePVCAfterResizing() {
	// CSATEST-1492
	It("calls CreateOrUpdatePVC twice with the different PVC sizes and should not update the PVC size", func() {
		ctx := context.Background()

		scName := "test-sc"
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
				StorageClassName: &scName,
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
