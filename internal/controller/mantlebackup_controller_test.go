package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/ceph"
	"github.com/cybozu-go/mantle/internal/controller/internal/objectstorage"
	"github.com/cybozu-go/mantle/internal/controller/internal/testutil"
	"github.com/cybozu-go/mantle/pkg/controller/proto"
	"github.com/cybozu-go/mantle/test/util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	aerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/kube-openapi/pkg/validation/strfmt"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

const (
	dummyPoolName  = "dummy"
	dummyImageName = "dummy"
	dummyPVCName   = "dummy"
)

// customMatcherHelper is a helper for implementing custom gomock.Matcher instantly.
type customMatcherHelper struct {
	matcher  func(x any) bool
	describe string
}

var _ gomock.Matcher = &customMatcherHelper{}

func (c *customMatcherHelper) Matches(x any) bool {
	return c.matcher(x)
}

func (c *customMatcherHelper) String() string {
	return c.describe
}

func getEnvValue(envVarAry []corev1.EnvVar, name string) (string, error) {
	for _, env := range envVarAry {
		if env.Name == name {
			return env.Value, nil
		}
	}
	return "", errors.New("name not found")
}

var _ = Describe("MantleBackup controller", func() {
	var mgrUtil testutil.ManagerUtil
	var reconciler *MantleBackupReconciler
	var ns string
	var lastExpireQueuedBackups sync.Map

	ensureBackupNotReadyToUse := func(ctx context.Context, backup *mantlev1.MantleBackup) {
		GinkgoHelper()
		Consistently(ctx, func(g Gomega, ctx context.Context) {
			namespacedName := types.NamespacedName{
				Namespace: backup.Namespace,
				Name:      backup.Name,
			}
			err := k8sClient.Get(ctx, namespacedName, backup)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(
				meta.IsStatusConditionTrue(backup.Status.Conditions, mantlev1.BackupConditionReadyToUse),
			).To(BeFalse())
		}, "10s", "1s").Should(Succeed())
	}

	waitForHavingFinalizer := func(ctx context.Context, backup *mantlev1.MantleBackup) {
		EventuallyWithOffset(1, func(g Gomega, ctx context.Context) {
			namespacedName := types.NamespacedName{
				Namespace: backup.Namespace,
				Name:      backup.Name,
			}
			err := k8sClient.Get(ctx, namespacedName, backup)
			g.Expect(err).NotTo(HaveOccurred())

			g.Expect(controllerutil.ContainsFinalizer(backup, MantleBackupFinalizerName)).To(BeTrue(), "finalizer does not set yet")
		}).WithContext(ctx).Should(Succeed())
	}

	simulateExpire := func(ctx context.Context, backup *mantlev1.MantleBackup, offset time.Duration) time.Time {
		err := k8sClient.Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace}, backup)
		Expect(err).NotTo(HaveOccurred())

		// set the creation time to expire the backup after the offset from now
		expire, err := strfmt.ParseDuration(backup.Spec.Expire)
		Expect(err).NotTo(HaveOccurred())
		// trim nsec because kubernetes seems not to save nsec.
		now := time.Unix(time.Now().Unix(), 0).UTC()
		newCreatedAt := now.Add(-expire).Add(offset)
		backup.Status.CreatedAt = metav1.NewTime(newCreatedAt)
		err = k8sClient.Status().Update(ctx, backup)
		Expect(err).NotTo(HaveOccurred())

		return newCreatedAt
	}

	// sniffing the expire queue to check the expiration is deferred.
	setupExpireQueueSniffer := func() {
		origCh := reconciler.expireQueueCh
		newCh := make(chan event.GenericEvent)
		reconciler.expireQueueCh = newCh
		go func() {
			for event := range newCh {
				lastExpireQueuedBackups.Store(
					types.NamespacedName{
						Namespace: event.Object.GetNamespace(),
						Name:      event.Object.GetName(),
					},
					event.Object.DeepCopyObject())
				origCh <- event
			}
		}()
	}

	AfterEach(func() {
		err := mgrUtil.Stop()
		Expect(err).NotTo(HaveOccurred())
	})

	Context("when the role is `standalone`", func() {
		BeforeEach(func() {
			mgrUtil = testutil.NewManagerUtil(context.Background(), cfg, scheme.Scheme)

			reconciler = NewMantleBackupReconciler(
				mgrUtil.GetManager().GetClient(),
				mgrUtil.GetManager().GetScheme(),
				resMgr.ClusterID,
				RoleStandalone,
				nil,
				"dummy image",
				"",
				nil,
				nil,
				resource.MustParse("1Gi"),
			)
			reconciler.ceph = testutil.NewFakeRBD()
			err := reconciler.SetupWithManager(mgrUtil.GetManager())
			Expect(err).NotTo(HaveOccurred())

			setupExpireQueueSniffer()

			mgrUtil.Start()
			time.Sleep(100 * time.Millisecond)

			ns = resMgr.CreateNamespace()
		})

		It("should fail when PVC UID does not match the stored UID", func(ctx SpecContext) {
			_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
			Expect(err).NotTo(HaveOccurred())

			backup, err := resMgr.CreateUniqueBackupFor(ctx, pvc)
			Expect(err).NotTo(HaveOccurred())

			// check the PVC UID is stored in the MantleBackup
			resMgr.WaitForBackupReady(ctx, backup)
			Expect(backup.GetLabels()[labelLocalBackupTargetPVCUID]).To(Equal(string(pvc.GetUID())))

			// simulate the PVC UID mismatch
			backup.SetLabels(map[string]string{labelLocalBackupTargetPVCUID: string(uuid.NewUUID())})
			_, _, err = reconciler.getSnapshotTarget(ctx, backup)
			Expect(err).To(HaveOccurred())
		})

		It("should be ready to use", func(ctx SpecContext) {
			pv, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
			Expect(err).NotTo(HaveOccurred())

			backup, err := resMgr.CreateUniqueBackupFor(ctx, pvc)
			Expect(err).NotTo(HaveOccurred())

			waitForHavingFinalizer(ctx, backup)
			resMgr.WaitForBackupReady(ctx, backup)

			pvcJS := backup.Status.PVCManifest
			Expect(pvcJS).NotTo(BeEmpty())
			pvcStored := corev1.PersistentVolumeClaim{}
			err = json.Unmarshal([]byte(pvcJS), &pvcStored)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvcStored.Name).To(Equal(pvc.Name))
			Expect(pvcStored.Namespace).To(Equal(pvc.Namespace))

			pvJS := backup.Status.PVManifest
			Expect(pvJS).NotTo(BeEmpty())
			pvStored := corev1.PersistentVolume{}
			err = json.Unmarshal([]byte(pvJS), &pvStored)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvStored.Name).To(Equal(pv.Name))

			snaps, err := reconciler.ceph.RBDSnapLs(resMgr.PoolName, pv.Spec.CSI.VolumeAttributes["imageName"])
			Expect(err).NotTo(HaveOccurred())
			Expect(snaps).To(HaveLen(1))
			snapID := backup.Status.SnapID
			Expect(snapID).To(Equal(&snaps[0].Id))

			err = k8sClient.Delete(ctx, backup)
			Expect(err).NotTo(HaveOccurred())

			testutil.CheckDeletedEventually[mantlev1.MantleBackup](ctx, k8sClient, backup.Name, backup.Namespace)
		})

		It("should still be ready to use even if the PVC lost", func(ctx SpecContext) {
			_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
			Expect(err).NotTo(HaveOccurred())

			backup, err := resMgr.CreateUniqueBackupFor(ctx, pvc)
			Expect(err).NotTo(HaveOccurred())

			waitForHavingFinalizer(ctx, backup)
			resMgr.WaitForBackupReady(ctx, backup)

			pvc.Status.Phase = corev1.ClaimLost // simulate lost PVC
			err = k8sClient.Status().Update(ctx, pvc)
			Expect(err).NotTo(HaveOccurred())

			resMgr.WaitForBackupReady(ctx, backup)
		})

		DescribeTable("MantleBackup with correct expiration",
			func(ctx SpecContext, expire string) {
				_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
				Expect(err).NotTo(HaveOccurred())

				_, err = resMgr.CreateUniqueBackupFor(ctx, pvc, func(backup *mantlev1.MantleBackup) {
					backup.Spec.Expire = expire
				})
				Expect(err).NotTo(HaveOccurred())
			},
			Entry("min expire", "1d"),
			Entry("max expire", "15d"),
			Entry("complex expire", "1w2d3h4m5s"),
		)

		DescribeTable("MantleBackup with incorrect expiration",
			func(ctx SpecContext, expire string) {
				_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
				Expect(err).NotTo(HaveOccurred())

				_, err = resMgr.CreateUniqueBackupFor(ctx, pvc, func(backup *mantlev1.MantleBackup) {
					backup.Spec.Expire = expire
				})
				Expect(err).To(Or(
					MatchError(ContainSubstring("expire must be")),
					MatchError(ContainSubstring("body must be of type duration")),
				))
			},
			Entry("invalid short expire", "23h"),
			Entry("invalid long expire", "15d1s"),
			Entry("invalid duration", "foo"),
		)

		It("Should reject updating the expire field", func(ctx SpecContext) {
			_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
			Expect(err).NotTo(HaveOccurred())

			backup, err := resMgr.CreateUniqueBackupFor(ctx, pvc)
			Expect(err).NotTo(HaveOccurred())

			expire, err := strfmt.ParseDuration(backup.Spec.Expire)
			Expect(err).NotTo(HaveOccurred())
			expire += time.Hour
			backup.Spec.Expire = expire.String()
			err = k8sClient.Update(ctx, backup)
			Expect(err).To(MatchError(ContainSubstring("spec.expire is immutable")))
		})

		DescribeTable("MantleBackup expiration",
			func(ctx SpecContext, offset time.Duration) {
				_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
				Expect(err).NotTo(HaveOccurred())

				backup, err := resMgr.CreateUniqueBackupFor(ctx, pvc)
				Expect(err).NotTo(HaveOccurred())

				By("waiting for the backup to be ready")
				resMgr.WaitForBackupReady(ctx, backup)

				expectCreatedAt := backup.Status.CreatedAt.Time

				By("simulate backup expiration")
				newCreatedAt := simulateExpire(ctx, backup, offset)

				By("wait for the backup to be deleted")
				testutil.CheckDeletedEventually[mantlev1.MantleBackup](ctx, k8sClient, backup.Name, backup.Namespace)

				By("check the queued backup has the correct createdAt")
				// If expiration is deferred, the backup with the new createdAt is queued.
				// Otherwise, the backup is not queued after updating the createdAt, so the backup has the original createdAt.
				if offset > 0 {
					expectCreatedAt = newCreatedAt
				}
				v, ok := lastExpireQueuedBackups.Load(types.NamespacedName{Namespace: backup.Namespace, Name: backup.Name})
				Expect(ok).To(BeTrue())
				createdAt := v.(*mantlev1.MantleBackup).Status.CreatedAt.Time
				Expect(createdAt).To(BeTemporally("==", expectCreatedAt))
			},
			Entry("an already expired backup should be deleted immediately", -time.Hour),
			Entry("a near expiring backup should be deleted after expiration", 10*time.Second),
		)

		It("should retain the backup if it has the retain-if-expired annotation", func(ctx SpecContext) {
			_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
			Expect(err).NotTo(HaveOccurred())

			backup, err := resMgr.CreateUniqueBackupFor(ctx, pvc)
			Expect(err).NotTo(HaveOccurred())

			By("waiting for the backup to be ready")
			resMgr.WaitForBackupReady(ctx, backup)

			By("simulate backup expiration")
			simulateExpire(ctx, backup, -time.Hour)

			By("checking the backup is not deleted")
			err = k8sClient.Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace}, backup)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should not be ready to use if the PVC is the lost state from the beginning", func(ctx SpecContext) {
			pv, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
			Expect(err).NotTo(HaveOccurred())
			pv.Status.Phase = corev1.VolumeAvailable
			err = k8sClient.Status().Update(ctx, pv)
			Expect(err).NotTo(HaveOccurred())
			pvc.Status.Phase = corev1.ClaimLost
			err = k8sClient.Status().Update(ctx, pvc)
			Expect(err).NotTo(HaveOccurred())

			backup, err := resMgr.CreateUniqueBackupFor(ctx, pvc)
			Expect(err).NotTo(HaveOccurred())

			ensureBackupNotReadyToUse(ctx, backup)
		})

		It("should not be ready to use if specified non-existent PVC name", func(ctx SpecContext) {
			var err error
			backup, err := resMgr.CreateUniqueBackupFor(ctx, &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "non-existent-pvc",
					Namespace: ns,
				},
			})
			Expect(err).NotTo(HaveOccurred())

			ensureBackupNotReadyToUse(ctx, backup)
		})

		It("should fail the resource creation the second time if the same MantleBackup is created twice", func(ctx SpecContext) {
			_, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
			Expect(err).NotTo(HaveOccurred())

			backup, err := resMgr.CreateUniqueBackupFor(ctx, pvc)
			Expect(err).NotTo(HaveOccurred())

			err = k8sClient.Create(ctx, backup)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("when the role is `primary`", func() {
		var mockCtrl *gomock.Controller
		var grpcClient *proto.MockMantleServiceClient
		proxySettings := &ProxySettings{
			HttpProxy:  "dummy http proxy",
			HttpsProxy: "dummy https proxy",
			NoProxy:    "no proxy",
		}
		BeforeEach(func() {
			mgrUtil = testutil.NewManagerUtil(context.Background(), cfg, scheme.Scheme)

			var t reporter
			mockCtrl = gomock.NewController(t)
			grpcClient = proto.NewMockMantleServiceClient(mockCtrl)
			reconciler = NewMantleBackupReconciler(
				mgrUtil.GetManager().GetClient(),
				mgrUtil.GetManager().GetScheme(),
				resMgr.ClusterID,
				RolePrimary,
				&PrimarySettings{
					Client:                 grpcClient,
					ExportDataStorageClass: resMgr.StorageClassName,
				},
				"dummy image",
				"dummy-secret-env",
				&ObjectStorageSettings{
					BucketName:      "",
					Endpoint:        "",
					CACertConfigMap: nil,
					CACertKey:       nil,
				},
				proxySettings,
				resource.MustParse("1Gi"),
			)
			reconciler.ceph = testutil.NewFakeRBD()

			mockObjectStorage := objectstorage.NewMockBucket(mockCtrl)
			reconciler.objectStorageClient = mockObjectStorage

			err := reconciler.SetupWithManager(mgrUtil.GetManager())
			Expect(err).NotTo(HaveOccurred())

			setupExpireQueueSniffer()

			mgrUtil.Start()
			time.Sleep(100 * time.Millisecond)

			ns = resMgr.CreateNamespace()
		})
		AfterEach(func() {
			if mockCtrl != nil {
				mockCtrl.Finish()
			}
		})

		It("should be synced to remote", func(ctx SpecContext) {
			// CSATEST-1491
			grpcClient.EXPECT().CreateOrUpdatePVC(gomock.Any(), &customMatcherHelper{
				// check if the PVC has the capacity equal to the fake RBD snapshot size
				matcher: func(x any) bool {
					req := x.(*proto.CreateOrUpdatePVCRequest)
					pvc := &corev1.PersistentVolumeClaim{}
					err := json.Unmarshal(req.GetPvc(), pvc)
					if err != nil {
						panic(err)
					}
					capacity, _ := pvc.Spec.Resources.Requests.Storage().AsInt64()
					return capacity == testutil.FakeRBDSnapshotSize
				},
				describe: fmt.Sprintf("CreateOrUpdatePVCRequest contains PVC with spec capacity %d", testutil.FakeRBDSnapshotSize),
			}).
				MinTimes(1).Return(
				&proto.CreateOrUpdatePVCResponse{
					Uid: "a7c9d5e2-4b8f-4e2a-9d3f-1b6a7c8e9f2b",
				}, nil)
			secondaryBackups := []*mantlev1.MantleBackup{}
			grpcClient.EXPECT().CreateOrUpdateMantleBackup(gomock.Any(), gomock.Any()).
				MinTimes(1).
				DoAndReturn(
					func(
						ctx context.Context,
						req *proto.CreateOrUpdateMantleBackupRequest,
						opts ...grpc.CallOption,
					) (*proto.CreateOrUpdateMantleBackupResponse, error) {
						var secondaryBackup mantlev1.MantleBackup
						err := json.Unmarshal(req.GetMantleBackup(), &secondaryBackup)
						if err != nil {
							panic(err)
						}
						secondaryBackups = append(secondaryBackups, &secondaryBackup)
						return &proto.CreateOrUpdateMantleBackupResponse{}, nil
					})
			grpcClient.EXPECT().ListMantleBackup(gomock.Any(), gomock.Any()).
				MinTimes(1).
				DoAndReturn(
					func(
						ctx context.Context,
						req *proto.ListMantleBackupRequest,
						opts ...grpc.CallOption,
					) (*proto.ListMantleBackupResponse, error) {
						data, err := json.Marshal(secondaryBackups)
						if err != nil {
							panic(err)
						}
						return &proto.ListMantleBackupResponse{
							MantleBackupList: data,
						}, nil
					})
			grpcClient.EXPECT().SetSynchronizing(gomock.Any(), gomock.Any()).
				MinTimes(1).
				DoAndReturn(
					func(
						ctx context.Context,
						req *proto.SetSynchronizingRequest,
						opts ...grpc.CallOption,
					) (*proto.SetSynchronizingResponse, error) {
						return &proto.SetSynchronizingResponse{}, nil
					})

			pv, pvc, err := resMgr.CreateUniquePVAndPVC(ctx, ns)
			Expect(err).NotTo(HaveOccurred())

			backup, err := resMgr.CreateUniqueBackupFor(ctx, pvc)
			Expect(err).NotTo(HaveOccurred())

			var jobExport batchv1.Job
			Eventually(func(g Gomega, ctx context.Context) {
				err = k8sClient.Get(ctx, types.NamespacedName{Name: backup.GetName(), Namespace: backup.GetNamespace()}, backup)
				g.Expect(err).NotTo(HaveOccurred())

				pvcJS := backup.Status.PVCManifest
				g.Expect(pvcJS).NotTo(BeEmpty())
				pvcStored := corev1.PersistentVolumeClaim{}
				err = json.Unmarshal([]byte(pvcJS), &pvcStored)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(pvcStored.Name).To(Equal(pvc.Name))
				g.Expect(pvcStored.Namespace).To(Equal(pvc.Namespace))

				pvJS := backup.Status.PVManifest
				g.Expect(pvJS).NotTo(BeEmpty())
				pvStored := corev1.PersistentVolume{}
				err = json.Unmarshal([]byte(pvJS), &pvStored)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(pvStored.Name).To(Equal(pv.Name))

				snaps, err := reconciler.ceph.RBDSnapLs(resMgr.PoolName, pv.Spec.CSI.VolumeAttributes["imageName"])
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(snaps).To(HaveLen(1))
				snapID := backup.Status.SnapID
				g.Expect(snapID).To(Equal(&snaps[0].Id))

				// Make sure export() correctly annotates the MantleBackup resource.
				syncMode, ok := backup.GetAnnotations()[annotSyncMode]
				g.Expect(ok).To(BeTrue())
				g.Expect(syncMode).To(Equal(syncModeFull))

				// Make sure export() creates a PVC for exported data
				var pvcExport corev1.PersistentVolumeClaim
				err = k8sClient.Get(
					ctx,
					types.NamespacedName{
						Name:      MakeExportDataPVCName(backup, 0),
						Namespace: resMgr.ClusterID,
					},
					&pvcExport,
				)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(pvcExport.GetLabels()["app.kubernetes.io/name"]).To(Equal(labelAppNameValue))
				g.Expect(pvcExport.GetLabels()["app.kubernetes.io/component"]).To(Equal(labelComponentExportData))
				g.Expect(pvcExport.Spec.AccessModes[0]).To(Equal(corev1.ReadWriteOnce))
				g.Expect(*pvcExport.Spec.StorageClassName).To(Equal(resMgr.StorageClassName))
				g.Expect(pvcExport.Spec.Resources.Requests.Storage().String()).To(Equal("1288490188")) // floor(1Gi*1.2)

				// Make sure export() creates a Job to export data.
				err = k8sClient.Get(
					ctx,
					types.NamespacedName{
						Name:      MakeExportJobName(backup, 0),
						Namespace: resMgr.ClusterID,
					},
					&jobExport,
				)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(jobExport.GetLabels()["app.kubernetes.io/name"]).To(Equal(labelAppNameValue))
				g.Expect(jobExport.GetLabels()["app.kubernetes.io/component"]).To(Equal(labelComponentExportJob))
				g.Expect(*jobExport.Spec.BackoffLimit).To(Equal(int32(65535)))
				g.Expect(*jobExport.Spec.Template.Spec.SecurityContext.FSGroup).To(Equal(int64(10000)))
				g.Expect(*jobExport.Spec.Template.Spec.SecurityContext.RunAsUser).To(Equal(int64(10000)))
				g.Expect(*jobExport.Spec.Template.Spec.SecurityContext.RunAsGroup).To(Equal(int64(10000)))
				g.Expect(*jobExport.Spec.Template.Spec.SecurityContext.RunAsNonRoot).To(Equal(true))

				// Make sure FROM_SNAP_NAME is empty because we're performing a full backup.
				isFromSnapNameFound := false
				for _, evar := range jobExport.Spec.Template.Spec.Containers[0].Env {
					if evar.Name == "FROM_SNAP_NAME" {
						g.Expect(evar.Value).To(Equal(""))
						isFromSnapNameFound = true
					}
				}
				g.Expect(isFromSnapNameFound).To(BeTrue())
			}).WithContext(ctx).Should(Succeed())

			// Make sure an upload Jobs has not yet been created.
			Consistently(ctx, func(g Gomega) error {
				var jobUpload batchv1.Job
				err = k8sClient.Get(
					ctx,
					types.NamespacedName{
						Name:      MakeUploadJobName(backup, 0),
						Namespace: resMgr.ClusterID,
					},
					&jobUpload,
				)
				g.Expect(aerrors.IsNotFound(err)).To(BeTrue())
				return nil
			}, "1s").Should(Succeed())

			// Make the export Job completed to proceed the reconciliation for backup.
			err = resMgr.ChangeJobCondition(ctx, &jobExport, batchv1.JobComplete, corev1.ConditionTrue)
			Expect(err).NotTo(HaveOccurred())

			// The snapshot size is 5GiB and transferPartSize is 1GiB. So the number of parts is 5.
			for i := 1; i < 5; i++ {
				var job batchv1.Job
				Eventually(func(g Gomega, ctx SpecContext) error {
					return k8sClient.Get(
						ctx,
						types.NamespacedName{
							Name:      MakeExportJobName(backup, i),
							Namespace: resMgr.ClusterID,
						},
						&job,
					)
				}).WithContext(ctx).Should(Succeed())
				err = resMgr.ChangeJobCondition(ctx, &job, batchv1.JobComplete, corev1.ConditionTrue)
				Expect(err).NotTo(HaveOccurred())
			}

			// Make sure the upload Job is created
			Eventually(func(g Gomega, ctx context.Context) {
				var jobUpload batchv1.Job
				err = k8sClient.Get(
					ctx,
					types.NamespacedName{
						Name:      MakeUploadJobName(backup, 0),
						Namespace: resMgr.ClusterID,
					},
					&jobUpload,
				)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(jobUpload.GetLabels()["app.kubernetes.io/name"]).To(Equal("mantle"))
				g.Expect(jobUpload.GetLabels()["app.kubernetes.io/component"]).To(Equal("upload-job"))
				g.Expect(*jobUpload.Spec.BackoffLimit).To(Equal(int32(65535)))
				g.Expect(*jobUpload.Spec.Template.Spec.SecurityContext.FSGroup).To(Equal(int64(10000)))
				g.Expect(*jobUpload.Spec.Template.Spec.SecurityContext.RunAsUser).To(Equal(int64(10000)))
				g.Expect(*jobUpload.Spec.Template.Spec.SecurityContext.RunAsGroup).To(Equal(int64(10000)))
				g.Expect(*jobUpload.Spec.Template.Spec.SecurityContext.RunAsNonRoot).To(Equal(true))

				// Make sure HTTP_PROXY, HTTPS_PROXY, and NO_PROXY environment variables are correctly set.
				httpProxy, err := getEnvValue(jobUpload.Spec.Template.Spec.Containers[0].Env, "HTTP_PROXY")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(httpProxy).To(Equal(proxySettings.HttpProxy))
				httpsProxy, err := getEnvValue(jobUpload.Spec.Template.Spec.Containers[0].Env, "HTTPS_PROXY")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(httpsProxy).To(Equal(proxySettings.HttpsProxy))
				noProxy, err := getEnvValue(jobUpload.Spec.Template.Spec.Containers[0].Env, "NO_PROXY")
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(noProxy).To(Equal(proxySettings.NoProxy))
			}).WithContext(ctx).Should(Succeed())

			// Make the all existing MantleBackups in the primary Mantle
			// SyncedToRemote=True.
			err = updateStatus(ctx, k8sClient, backup, func() error {
				meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
					Type:   mantlev1.BackupConditionSyncedToRemote,
					Status: metav1.ConditionTrue,
					Reason: mantlev1.BackupReasonNone,
				})
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			// Make the all existing MantleBackups in the (mocked) secondary Mantle
			// ReadyToUse=True.
			for _, backup := range secondaryBackups {
				meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
					Type:   mantlev1.BackupConditionReadyToUse,
					Status: metav1.ConditionTrue,
					Reason: mantlev1.BackupReasonNone,
				})
			}

			// Create another MantleBackup (backup2) to make sure it should become a incremental backup.
			backup2, err := resMgr.CreateUniqueBackupFor(ctx, pvc)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func(g Gomega, ctx context.Context) {
				err = k8sClient.Get(ctx, types.NamespacedName{Name: backup2.GetName(), Namespace: backup2.GetNamespace()}, backup2)
				g.Expect(err).NotTo(HaveOccurred())

				// Make sure backup2 is an incremental backup.
				syncMode2, ok := backup2.GetAnnotations()[annotSyncMode]
				g.Expect(ok).To(BeTrue())
				g.Expect(syncMode2).To(Equal(syncModeIncremental))
				err = k8sClient.Get(ctx, types.NamespacedName{Name: backup.GetName(), Namespace: backup.GetNamespace()}, backup)
				g.Expect(err).NotTo(HaveOccurred())
				diffTo, ok := backup.GetAnnotations()[annotDiffTo]
				g.Expect(ok).To(BeTrue())
				g.Expect(diffTo).To(Equal(backup2.GetName()))

				// Make sure export() creates a Job to export data for backup2.
				var jobExport2 batchv1.Job
				err = k8sClient.Get(
					ctx,
					types.NamespacedName{
						Name:      MakeExportJobName(backup2, 0),
						Namespace: resMgr.ClusterID,
					},
					&jobExport2,
				)
				g.Expect(err).NotTo(HaveOccurred())

				// Make sure FROM_SNAP_NAME is filled correctly because we're performing an incremental backup.
				isFromSnapNameFound := false
				for _, evar := range jobExport2.Spec.Template.Spec.Containers[0].Env {
					if evar.Name == "FROM_SNAP_NAME" {
						g.Expect(evar.Value).To(Equal(backup.GetName()))
						isFromSnapNameFound = true
					}
				}
				g.Expect(isFromSnapNameFound).To(BeTrue())
			}).WithContext(ctx).Should(Succeed())

			err = k8sClient.Delete(ctx, backup)
			Expect(err).NotTo(HaveOccurred())
			err = k8sClient.Delete(ctx, backup2)
			Expect(err).NotTo(HaveOccurred())

			testutil.CheckDeletedEventually[mantlev1.MantleBackup](ctx, k8sClient, backup.Name, backup.Namespace)
		})
	})
})

var _ = Describe("searchDiffOriginMantleBackup", func() {
	testMantleBackup := newMantleBackup("test0", "test-ns", nil, nil, false,
		5, metav1.ConditionTrue, metav1.ConditionFalse)

	basePrimaryBackups := []mantlev1.MantleBackup{
		*newMantleBackup("test1", "test-ns", nil, nil, false,
			1, metav1.ConditionTrue, metav1.ConditionTrue),
		*newMantleBackup("test2", "test-ns", nil, nil, false,
			6, metav1.ConditionTrue, metav1.ConditionFalse),
		*newMantleBackup("test3", "test-ns", nil, nil, false,
			3, metav1.ConditionTrue, metav1.ConditionTrue),
		*newMantleBackup("test4", "test-ns", nil, nil, false,
			4, metav1.ConditionTrue, metav1.ConditionTrue),
	}
	// Note that slices.Clone() does the shallow copy.
	// ref. https://pkg.go.dev/slices#Clone
	primaryBackupsWithBlankCondition := slices.Clone(basePrimaryBackups)
	primaryBackupsWithBlankCondition[2] = *basePrimaryBackups[2].DeepCopy()
	meta.RemoveStatusCondition(&primaryBackupsWithBlankCondition[2].Status.Conditions, mantlev1.BackupConditionReadyToUse)
	primaryBackupsWithDeletionTimestamp := slices.Clone(basePrimaryBackups)
	primaryBackupsWithDeletionTimestamp[2] = *basePrimaryBackups[2].DeepCopy()
	now := metav1.Now()
	primaryBackupsWithDeletionTimestamp[2].SetDeletionTimestamp(&now)

	testSecondaryMantleBackups := map[string]*mantlev1.MantleBackup{
		"test1": basePrimaryBackups[0].DeepCopy(),
		// "test2" cannot exist on the secondary cluster
		// because it has a higher snapID than "test0".
		"test3": basePrimaryBackups[2].DeepCopy(),
		// "test4" is intentionally omitted.
	}

	DescribeTable("Search for the MantleBackup which is used for the diff origin",
		func(backup *mantlev1.MantleBackup,
			primaryBackups []mantlev1.MantleBackup,
			secondaryBackupMap map[string]*mantlev1.MantleBackup,
			shouldFindBackup bool,
			expectedBackupName string) {
			foundBackup := searchForDiffOriginMantleBackup(backup, primaryBackups, secondaryBackupMap)
			if shouldFindBackup {
				Expect(foundBackup).NotTo(BeNil())
				Expect(foundBackup.GetName()).To(Equal(expectedBackupName))
			} else {
				Expect(foundBackup).To(BeNil())
			}
		},
		Entry("should return nil when no MantleBackup found on the secondary cluster",
			testMantleBackup, basePrimaryBackups, make(map[string]*mantlev1.MantleBackup),
			false, ""),
		Entry("should find the correct MantleBackup",
			testMantleBackup, basePrimaryBackups, testSecondaryMantleBackups,
			true, "test3"),
		Entry("should skip the not-ready MantleBackup",
			testMantleBackup, primaryBackupsWithBlankCondition, testSecondaryMantleBackups,
			true, "test1"),
		Entry("should skip the MantleBackup with the deletion timestamp",
			testMantleBackup, primaryBackupsWithDeletionTimestamp, testSecondaryMantleBackups,
			true, "test1"),
	)
})

type reporter struct{}

func (g reporter) Errorf(format string, args ...any) {
	Fail(fmt.Sprintf(format, args...))
}

func (g reporter) Fatalf(format string, args ...any) {
	Fail(fmt.Sprintf(format, args...))
}

func newMantleBackup(
	name string,
	namespace string,
	annotations map[string]string,
	labels map[string]string,
	withDelTimestamp bool,
	snapID int,
	readyToUse metav1.ConditionStatus,
	syncedToRemote metav1.ConditionStatus,
) *mantlev1.MantleBackup {
	newMB := &mantlev1.MantleBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: annotations,
			Labels:      labels,
		},
		Status: mantlev1.MantleBackupStatus{
			SnapID: &snapID,
		},
	}
	if withDelTimestamp {
		now := metav1.Now()
		newMB.SetDeletionTimestamp(&now)
	}
	meta.SetStatusCondition(&newMB.Status.Conditions, metav1.Condition{
		Type:   mantlev1.BackupConditionReadyToUse,
		Status: readyToUse,
	})
	meta.SetStatusCondition(&newMB.Status.Conditions, metav1.Condition{
		Type:   mantlev1.BackupConditionSyncedToRemote,
		Status: syncedToRemote,
	})
	return newMB
}

var _ = Describe("prepareForDataSynchronization", func() {
	testPVCUID := "d3b07384-d9a7-4e6b-8a3b-1f4b7b7b7b7b"
	testName := "testSnap5"
	testNamespace := "test-ns"
	primaryLabels := map[string]string{
		labelLocalBackupTargetPVCUID: testPVCUID,
	}
	secondaryLabels := map[string]string{
		labelRemoteBackupTargetPVCUID: testPVCUID,
	}
	testMantleBackup := newMantleBackup(testName, testNamespace, nil, primaryLabels,
		false, 5, metav1.ConditionTrue, metav1.ConditionFalse)

	doTest := func(
		backup *mantlev1.MantleBackup,
		primaryBackupsWithoutTarget []*mantlev1.MantleBackup,
		secondaryMantleBackups []*mantlev1.MantleBackup,
		isIncremental bool,
		isSecondaryMantleBackupReadyToUse bool,
		diffFrom *mantlev1.MantleBackup,
	) {
		var t reporter
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()
		grpcClient := proto.NewMockMantleServiceClient(mockCtrl)

		data, err := json.Marshal(secondaryMantleBackups)
		Expect(err).NotTo(HaveOccurred())
		grpcClient.EXPECT().ListMantleBackup(gomock.Any(),
			&proto.ListMantleBackupRequest{
				PvcUID:    testPVCUID,
				Namespace: testNamespace,
			}).Times(1).Return(
			&proto.ListMantleBackupResponse{
				MantleBackupList: data,
			}, nil)

		ctrlClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
		Expect(ctrlClient).NotTo(BeNil())
		primaryBackups := append(primaryBackupsWithoutTarget, backup.DeepCopy())
		for _, backup := range primaryBackups {
			shouldBeDeleted := false
			if !backup.DeletionTimestamp.IsZero() {
				shouldBeDeleted = true
			}

			err := ctrlClient.Create(context.Background(), backup)
			Expect(err).NotTo(HaveOccurred())
			if shouldBeDeleted {
				// Add a finalizer to prevent the immediate deletion.
				_ = controllerutil.AddFinalizer(backup, MantleBackupFinalizerName)
				err = ctrlClient.Update(context.Background(), backup)
				Expect(err).NotTo(HaveOccurred())
				err := ctrlClient.Delete(context.Background(), backup)
				Expect(err).NotTo(HaveOccurred())
				// Check if the deletion is blocked by the finalizer.
				err = ctrlClient.Get(context.Background(), types.NamespacedName{
					Namespace: backup.Namespace,
					Name:      backup.Name,
				}, backup)
				Expect(err).NotTo(HaveOccurred())
				Expect(controllerutil.ContainsFinalizer(backup, MantleBackupFinalizerName)).To(BeTrue())
			}
		}

		mbr := NewMantleBackupReconciler(ctrlClient,
			ctrlClient.Scheme(), "test", RolePrimary, nil, "dummy image", "", nil, nil, resource.MustParse("1Gi"))

		ret, err := mbr.prepareForDataSynchronization(context.Background(),
			backup, grpcClient)
		Expect(err).NotTo(HaveOccurred())
		Expect(ret.isIncremental).To(Equal(isIncremental))
		Expect(ret.isSecondaryMantleBackupReadyToUse).To(Equal(isSecondaryMantleBackupReadyToUse))
		if isIncremental {
			Expect(ret.diffFrom).NotTo(BeNil())
			Expect(ret.diffFrom.GetName()).To(Equal(diffFrom.GetName()))
		} else {
			Expect(ret.diffFrom).To(BeNil())
		}
	}

	DescribeTable("MantleBackup without annotations", doTest,
		Entry("No synced MantleBackup exists",
			testMantleBackup,
			[]*mantlev1.MantleBackup{},
			[]*mantlev1.MantleBackup{
				newMantleBackup(testName, testNamespace, nil, secondaryLabels, false,
					5, metav1.ConditionFalse, metav1.ConditionUnknown),
			}, false, false, nil),
		Entry("Synced but not reflected to the condition",
			testMantleBackup,
			[]*mantlev1.MantleBackup{},
			[]*mantlev1.MantleBackup{
				newMantleBackup(testName, testNamespace, nil, secondaryLabels, false,
					5, metav1.ConditionTrue, metav1.ConditionUnknown),
			}, false, true, nil),
		Entry("Synced MantleBackup exists but deletionTimestamp is set on both clusters",
			testMantleBackup,
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap3", testNamespace, nil, primaryLabels, true,
					3, metav1.ConditionTrue, metav1.ConditionTrue),
			},
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap3", testNamespace, nil, secondaryLabels, true,
					3, metav1.ConditionTrue, metav1.ConditionUnknown),
				newMantleBackup(testName, testNamespace, nil, secondaryLabels, false,
					5, metav1.ConditionFalse, metav1.ConditionUnknown),
			}, false, false, nil),
		Entry("Synced MantleBackup exists but deletionTimestamp is set on the primary cluster",
			testMantleBackup,
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap3", testNamespace, nil, primaryLabels, true,
					3, metav1.ConditionTrue, metav1.ConditionTrue),
			},
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap3", testNamespace, nil, secondaryLabels, false,
					3, metav1.ConditionTrue, metav1.ConditionUnknown),
				newMantleBackup(testName, testNamespace, nil, secondaryLabels, false,
					5, metav1.ConditionFalse, metav1.ConditionUnknown),
			}, false, false, nil),
		Entry("Synced MantleBackup exists but deletionTimestamp is set on the secondary cluster",
			testMantleBackup,
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap3", testNamespace, nil, primaryLabels, false,
					3, metav1.ConditionTrue, metav1.ConditionTrue),
			},
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap3", testNamespace, nil, secondaryLabels, true,
					3, metav1.ConditionTrue, metav1.ConditionUnknown),
				newMantleBackup(testName, testNamespace, nil, secondaryLabels, false,
					5, metav1.ConditionFalse, metav1.ConditionUnknown),
			}, false, false, nil),
		Entry("The candidate for the diff origin MantleBackup does not exist on the primary cluster",
			testMantleBackup,
			[]*mantlev1.MantleBackup{},
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap3", testNamespace, nil, secondaryLabels, false,
					3, metav1.ConditionTrue, metav1.ConditionUnknown),
				newMantleBackup(testName, testNamespace, nil, secondaryLabels, false,
					5, metav1.ConditionFalse, metav1.ConditionUnknown),
			}, false, false, nil),
		Entry("The candidate for the diff origin MantleBackup does not exist on the secondary cluster",
			testMantleBackup,
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap3", testNamespace, nil, primaryLabels, false,
					3, metav1.ConditionTrue, metav1.ConditionTrue),
			},
			[]*mantlev1.MantleBackup{
				newMantleBackup(testName, testNamespace, nil, secondaryLabels, false,
					5, metav1.ConditionFalse, metav1.ConditionUnknown),
			}, false, false, nil),
		Entry("Incremental backup",
			testMantleBackup,
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap1", testNamespace, nil, primaryLabels, false,
					1, metav1.ConditionTrue, metav1.ConditionTrue),
				newMantleBackup("testSnap3", testNamespace, nil, primaryLabels, false,
					3, metav1.ConditionTrue, metav1.ConditionTrue),
			},
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap1", testNamespace, nil, secondaryLabels, false,
					1, metav1.ConditionTrue, metav1.ConditionUnknown),
				newMantleBackup("testSnap3", testNamespace, nil, secondaryLabels, false,
					3, metav1.ConditionTrue, metav1.ConditionUnknown),
				newMantleBackup(testName, testNamespace, nil, secondaryLabels, false,
					5, metav1.ConditionFalse, metav1.ConditionUnknown),
			}, true, false, newMantleBackup("testSnap3", testNamespace, nil, primaryLabels,
				false, 3, metav1.ConditionTrue, metav1.ConditionTrue),
		),
		Entry("Incremental backup but skips the MantleBackup which only exists on the primary cluster",
			testMantleBackup,
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap1", testNamespace, nil, primaryLabels, false,
					1, metav1.ConditionTrue, metav1.ConditionTrue),
				newMantleBackup("testSnap3", testNamespace, nil, primaryLabels, false,
					3, metav1.ConditionTrue, metav1.ConditionTrue),
			},
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap1", testNamespace, nil, secondaryLabels, false,
					1, metav1.ConditionTrue, metav1.ConditionUnknown),
				newMantleBackup(testName, testNamespace, nil, secondaryLabels, false,
					5, metav1.ConditionFalse, metav1.ConditionUnknown),
			}, true, false, newMantleBackup("testSnap1", testNamespace, nil, primaryLabels,
				false, 1, metav1.ConditionTrue, metav1.ConditionTrue),
		),
	)

	syncModeFullAnnotation := map[string]string{
		annotSyncMode: syncModeFull,
	}
	syncModeIncAnnotation := map[string]string{
		annotSyncMode: syncModeIncremental,
		annotDiffFrom: "testSnap3",
	}
	DescribeTable("MantleBackup with annotations", doTest,
		Entry(`Skip search because the MantleBackup already has the "sync-mode: full" annotation.`,
			newMantleBackup(testName, testNamespace, syncModeFullAnnotation, primaryLabels,
				false, 5, metav1.ConditionTrue, metav1.ConditionFalse),
			[]*mantlev1.MantleBackup{},
			[]*mantlev1.MantleBackup{
				newMantleBackup(testName, testNamespace, syncModeFullAnnotation, secondaryLabels,
					false, 5, metav1.ConditionFalse, metav1.ConditionUnknown),
			}, false, false, nil),
		Entry(`Skip search because the MantleBackup already has the "sync-mode: incremental" annotation.`,
			newMantleBackup(testName, testNamespace, syncModeIncAnnotation, primaryLabels,
				false, 5, metav1.ConditionTrue, metav1.ConditionFalse),
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap1", testNamespace, nil, primaryLabels, false,
					1, metav1.ConditionTrue, metav1.ConditionTrue),
				newMantleBackup("testSnap3", testNamespace, nil, primaryLabels, false,
					3, metav1.ConditionTrue, metav1.ConditionTrue),
			},
			[]*mantlev1.MantleBackup{
				newMantleBackup("testSnap1", testNamespace, nil, secondaryLabels, false,
					1, metav1.ConditionTrue, metav1.ConditionUnknown),
				newMantleBackup("testSnap3", testNamespace, nil, secondaryLabels, false,
					3, metav1.ConditionTrue, metav1.ConditionUnknown),
				newMantleBackup(testName, testNamespace, syncModeIncAnnotation, secondaryLabels,
					false, 5, metav1.ConditionFalse, metav1.ConditionUnknown),
			}, true, false, newMantleBackup("testSnap3", testNamespace, nil, primaryLabels,
				false, 3, metav1.ConditionTrue, metav1.ConditionTrue),
		),
	)
})

var _ = Describe("SetSynchronizing", func() {
	doTestCallOnce := func(
		target, source *mantlev1.MantleBackup,
		shouldBeError bool,
		check func(target, source *mantlev1.MantleBackup) error,
	) {
		var err error
		targetName := "target-name"
		target.SetName(targetName)
		backupNamespace := "target-ns"
		target.SetNamespace(backupNamespace)
		sourceName := "source"

		ctrlClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()

		err = ctrlClient.Create(context.Background(), target)
		Expect(err).NotTo(HaveOccurred())

		var diffFrom *string
		if source != nil {
			source.SetName(sourceName)
			source.SetNamespace(backupNamespace)
			err = ctrlClient.Create(context.Background(), source)
			Expect(err).NotTo(HaveOccurred())
			diffFrom = &sourceName
		}

		secondaryServer := NewSecondaryServer(ctrlClient)
		_, err = secondaryServer.SetSynchronizing(context.Background(), &proto.SetSynchronizingRequest{
			Name:      targetName,
			Namespace: backupNamespace,
			DiffFrom:  diffFrom,
		})
		if shouldBeError {
			Expect(err).To(HaveOccurred())
		} else {
			Expect(err).NotTo(HaveOccurred())
		}

		err = ctrlClient.Get(
			context.Background(),
			types.NamespacedName{Name: targetName, Namespace: backupNamespace},
			target,
		)
		Expect(err).NotTo(HaveOccurred())
		if source != nil {
			err = ctrlClient.Get(
				context.Background(),
				types.NamespacedName{Name: sourceName, Namespace: backupNamespace},
				source,
			)
			Expect(err).NotTo(HaveOccurred())
		}
		err = check(target, source)
		Expect(err).NotTo(HaveOccurred())
	}
	DescribeTable("call SetSynchronizing once", doTestCallOnce,
		Entry(
			"a full backup should succeed",
			&mantlev1.MantleBackup{},
			nil,
			false,
			func(target, source *mantlev1.MantleBackup) error {
				syncMode, ok := target.GetAnnotations()[annotSyncMode]
				if !ok || syncMode != syncModeFull {
					return errors.New("syncMode is invalid")
				}
				if _, ok := target.GetAnnotations()[annotDiffFrom]; ok {
					return errors.New("diffFrom should not exist")
				}
				return nil
			},
		),
		Entry(
			"an incremental backup should succeed",
			&mantlev1.MantleBackup{},
			&mantlev1.MantleBackup{},
			false,
			func(target, source *mantlev1.MantleBackup) error {
				syncMode, ok := target.GetAnnotations()[annotSyncMode]
				if !ok || syncMode != syncModeIncremental {
					return errors.New("syncMode is invalid")
				}
				diffFrom, ok := target.GetAnnotations()[annotDiffFrom]
				if !ok || diffFrom != source.GetName() {
					return errors.New("diffFrom is invalid")
				}
				diffTo, ok := source.GetAnnotations()[annotDiffTo]
				if !ok || diffTo != target.GetName() {
					return errors.New("diffTo is invalid")
				}
				return nil
			},
		),
		Entry(
			"a backup should fail if target's ReadyToUse is True",
			&mantlev1.MantleBackup{
				Status: mantlev1.MantleBackupStatus{
					Conditions: []metav1.Condition{
						{
							Type:   mantlev1.BackupConditionReadyToUse,
							Status: metav1.ConditionTrue,
						},
					},
				},
			},
			nil,
			true,
			func(_, _ *mantlev1.MantleBackup) error { return nil },
		),
	)

	doTestCallTwice := func(
		name1 string,
		diffFrom1 *string,
		name2 string,
		diffFrom2 *string,
		shouldBeError bool,
	) {
		var err error

		ctrlClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()

		for _, name := range []string{"M0", "M1", "M2"} {
			err = ctrlClient.Create(context.Background(), &mantlev1.MantleBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name: name,
				},
			})
			Expect(err).NotTo(HaveOccurred())
		}

		secondaryServer := NewSecondaryServer(ctrlClient)
		_, err = secondaryServer.SetSynchronizing(context.Background(), &proto.SetSynchronizingRequest{
			Name:      name1,
			Namespace: "",
			DiffFrom:  diffFrom1,
		})
		Expect(err).NotTo(HaveOccurred())
		_, err = secondaryServer.SetSynchronizing(context.Background(), &proto.SetSynchronizingRequest{
			Name:      name2,
			Namespace: "",
			DiffFrom:  diffFrom2,
		})
		if shouldBeError {
			Expect(err).To(HaveOccurred())
		} else {
			Expect(err).NotTo(HaveOccurred())
		}
	}
	m0 := "M0"
	m1 := "M1"
	m2 := "M2"
	DescribeTable("call SetSynchronizing twice", doTestCallTwice,
		Entry("case 1", m0, nil, m0, nil, false),
		Entry("case 2", m1, &m0, m1, &m0, false),
		Entry("case 3", m1, nil, m1, &m0, true),
		Entry("case 4", m1, &m0, m1, nil, true),
		Entry("case 5", m2, &m0, m2, &m1, true),
		Entry("case 6", m1, &m0, m2, &m0, true),
	)
})

func createMantleBackupUsingDummyPVC(ctx context.Context, name, ns string) (*mantlev1.MantleBackup, error) {
	pvc := corev1.PersistentVolumeClaim{
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}
	dummyPVCManifest, err := json.Marshal(pvc)
	if err != nil {
		return nil, err
	}

	pv := corev1.PersistentVolume{
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					VolumeAttributes: map[string]string{
						"pool":      dummyPoolName,
						"imageName": dummyImageName,
					},
				},
			},
		},
	}
	dummyPVManifest, err := json.Marshal(pv)
	if err != nil {
		return nil, err
	}

	target := &mantlev1.MantleBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		Spec: mantlev1.MantleBackupSpec{
			PVC:    dummyPVCName,
			Expire: "1d",
		},
	}
	controllerutil.AddFinalizer(target, MantleBackupFinalizerName)
	if err := k8sClient.Create(ctx, target); err != nil {
		return nil, err
	}

	if err := updateStatus(ctx, k8sClient, target, func() error {
		target.Status.PVManifest = string(dummyPVManifest)
		target.Status.PVCManifest = string(dummyPVCManifest)
		return nil
	}); err != nil {
		return nil, err
	}

	return target, nil
}

func createSnapshotForMantleBackupUsingDummyPVC(ctx context.Context, cephCmd ceph.CephCmd, backup *mantlev1.MantleBackup) error {
	snapName := util.GetUniqueName("snap")
	if err := cephCmd.RBDSnapCreate(dummyPoolName, dummyImageName, snapName); err != nil {
		return err
	}
	snaps, err := cephCmd.RBDSnapLs(dummyPoolName, dummyImageName)
	if err != nil {
		return err
	}
	index := slices.IndexFunc(snaps, func(snap ceph.RBDSnapshot) bool { return snap.Name == snapName })
	if index == -1 {
		return errors.New("unreachable: not found")
	}
	if err := updateStatus(ctx, k8sClient, backup, func() error {
		backup.Status.SnapID = &snaps[index].Id
		backup.Status.SnapSize = &snaps[index].Size
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func setStatusTransferPartSize(ctx context.Context, backup *mantlev1.MantleBackup) error {
	transferPartSize := resource.MustParse("1Gi")
	backup.Status.TransferPartSize = &transferPartSize
	if err := k8sClient.Status().Update(ctx, backup); err != nil {
		return err
	}
	return nil
}

var _ = Describe("export and upload", func() {
	var mockCtrl *gomock.Controller
	var grpcClient *proto.MockMantleServiceClient
	var mbr *MantleBackupReconciler
	var nsController, ns string

	createAndExportMantleBackup := func(
		ctx SpecContext,
		mbr *MantleBackupReconciler,
		name, ns string,
		isIncremental, isSecondaryMantleBackupReadyToUse bool,
		diffFrom *mantlev1.MantleBackup,
	) *mantlev1.MantleBackup {
		GinkgoHelper()

		target, err := createMantleBackupUsingDummyPVC(ctx, name, ns)
		Expect(err).NotTo(HaveOccurred())

		err = createSnapshotForMantleBackupUsingDummyPVC(ctx, mbr.ceph, target)
		Expect(err).NotTo(HaveOccurred())

		grpcClient.EXPECT().SetSynchronizing(gomock.Any(), gomock.Any()).
			Times(1).Return(&proto.SetSynchronizingResponse{}, nil)

		ret, err := mbr.startExportAndUpload(ctx, target, &dataSyncPrepareResult{
			isIncremental:                     isIncremental,
			isSecondaryMantleBackupReadyToUse: isSecondaryMantleBackupReadyToUse,
			diffFrom:                          diffFrom,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(ret.Requeue).To(BeTrue())

		return target
	}

	runStartExportAndUpload := func(ctx SpecContext, target *mantlev1.MantleBackup) {
		grpcClient.EXPECT().SetSynchronizing(gomock.Any(), gomock.Any()).
			Times(1).Return(&proto.SetSynchronizingResponse{}, nil)
		ret, err := mbr.startExportAndUpload(ctx, target, &dataSyncPrepareResult{
			isIncremental:                     false,
			isSecondaryMantleBackupReadyToUse: false,
			diffFrom:                          nil,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(ret.Requeue).To(BeTrue())
	}

	completeJob := func(ctx SpecContext, jobName string) {
		GinkgoHelper()

		// Get export Job for target1
		var job batchv1.Job
		Eventually(ctx, func(g Gomega, ctx SpecContext) {
			err := k8sClient.Get(ctx,
				types.NamespacedName{Name: jobName, Namespace: nsController}, &job)
			g.Expect(err).NotTo(HaveOccurred())
		}).Should(Succeed())

		// Make export Job for target 1 Completed.
		err := resMgr.ChangeJobCondition(ctx, &job, batchv1.JobComplete, corev1.ConditionTrue)
		Expect(err).NotTo(HaveOccurred())
	}

	waitJobDeleted := func(ctx SpecContext, jobName string) {
		Eventually(ctx, func(g Gomega, ctx SpecContext) {
			var job batchv1.Job
			err := k8sClient.Get(ctx,
				types.NamespacedName{Name: jobName, Namespace: nsController}, &job)
			g.Expect(err).To(HaveOccurred())
			g.Expect(aerrors.IsNotFound(err)).To(BeTrue())
		}).Should(Succeed())
	}

	ensureJobNotDeleted := func(ctx SpecContext, jobName string) {
		Consistently(ctx, func(g Gomega, ctx SpecContext) {
			var job batchv1.Job
			err := k8sClient.Get(ctx,
				types.NamespacedName{Name: jobName, Namespace: nsController}, &job)
			g.Expect(err).NotTo(HaveOccurred())
		}, "3s", "1s").Should(Succeed())
	}

	BeforeEach(func() {
		var t reporter
		mockCtrl = gomock.NewController(t)
		grpcClient = proto.NewMockMantleServiceClient(mockCtrl)

		nsController = resMgr.CreateNamespace()

		mbr = NewMantleBackupReconciler(
			k8sClient,
			scheme.Scheme,
			nsController,
			RolePrimary,
			&PrimarySettings{
				Client:                 grpcClient,
				ExportDataStorageClass: resMgr.StorageClassName,
				MaxExportJobs:          1,
			},
			"dummy image",
			"dummy-secret",
			&ObjectStorageSettings{},
			&ProxySettings{
				HttpProxy:  "",
				HttpsProxy: "",
				NoProxy:    "",
			},
			resource.MustParse("1Gi"),
		)
		mbr.ceph = testutil.NewFakeRBD()

		ns = resMgr.CreateNamespace()
	})

	AfterEach(func() {
		if mockCtrl != nil {
			mockCtrl.Finish()
		}
	})

	Context("export", func() {
		It("should set correct annotations after export() is called", func(ctx SpecContext) {
			// test a full backup
			target := createAndExportMantleBackup(ctx, mbr, "target", ns, false, false, nil)

			err := k8sClient.Get(ctx,
				types.NamespacedName{Name: target.GetName(), Namespace: target.GetNamespace()}, target)
			Expect(err).NotTo(HaveOccurred())
			_, ok := target.GetAnnotations()[annotDiffFrom]
			Expect(ok).To(BeFalse())

			// test an incremental backup
			target2 := createAndExportMantleBackup(ctx, mbr, "target2", ns, true, false, target)

			err = k8sClient.Get(ctx,
				types.NamespacedName{Name: target.GetName(), Namespace: target.GetNamespace()}, target)
			Expect(err).NotTo(HaveOccurred())
			diffTo, ok := target.GetAnnotations()[annotDiffTo]
			Expect(ok).To(BeTrue())
			Expect(diffTo).To(Equal(target2.GetName()))

			err = k8sClient.Get(ctx,
				types.NamespacedName{Name: target2.GetName(), Namespace: target2.GetNamespace()}, target2)
			Expect(err).NotTo(HaveOccurred())
			diffFrom, ok := target2.GetAnnotations()[annotDiffFrom]
			Expect(ok).To(BeTrue())
			Expect(diffFrom).To(Equal(target.GetName()))
		})

		It("should throttle export jobs correctly", func(ctx SpecContext) {
			getNumOfExportJobs := func(ns string) (int, error) {
				var jobs batchv1.JobList
				err := k8sClient.List(ctx, &jobs, &client.ListOptions{
					Namespace: ns,
					LabelSelector: labels.SelectorFromSet(map[string]string{
						"app.kubernetes.io/name":      labelAppNameValue,
						"app.kubernetes.io/component": labelComponentExportJob,
					}),
				})
				return len(jobs.Items), err
			}

			// create 5 different MantleBackup resources and call export() for each of them
			for i := 0; i < 5; i++ {
				createAndExportMantleBackup(ctx, mbr, fmt.Sprintf("target1-%d", i), ns, false, false, nil)
			}

			// make sure that only 1 Job is created
			Consistently(ctx, func(g Gomega) error {
				numJobs, err := getNumOfExportJobs(nsController)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(numJobs).To(Equal(1))
				return nil
			}, "1s").Should(Succeed())

			// make sure that another mantle-controller existing in a different namespace can create an export Job.
			nsController2 := resMgr.CreateNamespace()
			mbr2 := NewMantleBackupReconciler(
				k8sClient,
				scheme.Scheme,
				nsController2,
				RolePrimary,
				&PrimarySettings{
					Client:                 grpcClient,
					ExportDataStorageClass: resMgr.StorageClassName,
					MaxExportJobs:          1,
				},
				"dummy image",
				"",
				nil,
				nil,
				resource.MustParse("1Gi"),
			)
			mbr2.ceph = testutil.NewFakeRBD()
			ns2 := resMgr.CreateNamespace()
			createAndExportMantleBackup(ctx, mbr2, "target2", ns2, false, false, nil)
			Eventually(ctx, func(g Gomega) error {
				numJobs, err := getNumOfExportJobs(nsController2)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(numJobs).To(Equal(1))
				return nil
			}).Should(Succeed())
		})

		DescribeTable(
			"Deletion of completed export Jobs",
			func(ctx SpecContext, backupTransferPartSize int64, numOfParts int) {
				mbr.primarySettings.MaxExportJobs = 2
				mbr.backupTransferPartSize = *resource.NewQuantity(backupTransferPartSize, resource.BinarySI)

				target1 := createAndExportMantleBackup(ctx, mbr, "target1", ns, false, false, nil)
				target2 := createAndExportMantleBackup(ctx, mbr, "target2", ns, false, false, nil)

				for partNum := 0; partNum < numOfParts; partNum++ {
					completeJob(ctx, MakeExportJobName(target1, partNum))
					runStartExportAndUpload(ctx, target1)
					runStartExportAndUpload(ctx, target2)

					// Make sure the export Job for target 1 is deleted.
					if partNum > 0 {
						waitJobDeleted(ctx, MakeExportJobName(target1, partNum-1))
					}
				}

				// Make sure the export Job of part 0 for target 2 is NOT deleted.
				ensureJobNotDeleted(ctx, MakeExportJobName(target2, 0))
			},
			Entry("snap size < transfer part size", int64(testutil.FakeRBDSnapshotSize)+1, 1),
			Entry("snap size = transfer part size", int64(testutil.FakeRBDSnapshotSize), 1),
			Entry("snap size > transfer part size", int64(testutil.FakeRBDSnapshotSize-1), 2),
		)

		It("should use the original transfer part size if it's changed", func(ctx SpecContext) {
			origSize := *resource.NewQuantity(testutil.FakeRBDSnapshotSize-1, resource.BinarySI)
			newSize := *resource.NewQuantity(testutil.FakeRBDSnapshotSize+1, resource.BinarySI)
			mbr.backupTransferPartSize = origSize
			backup := createAndExportMantleBackup(ctx, mbr, "backup", ns, false, false, nil)
			mbr.backupTransferPartSize = newSize

			// mimic as if the reconciler is called at least once after setting the new transfer part size.
			runStartExportAndUpload(ctx, backup)

			Eventually(ctx, func(g Gomega, ctx SpecContext) {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: backup.GetName(), Namespace: backup.GetNamespace()}, backup)
				Expect(err).NotTo(HaveOccurred())
			}).Should(Succeed())
			Expect(backup.Status.TransferPartSize.Equal(origSize)).To(BeTrue())

			completeJob(ctx, MakeExportJobName(backup, 0))
			runStartExportAndUpload(ctx, backup)
			completeJob(ctx, MakeExportJobName(backup, 1))
			runStartExportAndUpload(ctx, backup)
		})
	})

	Context("upload", func() {
		DescribeTable(
			"Deletion of completed upload Jobs",
			func(ctx SpecContext, backupTransferPartSize int64, numOfParts int) {
				mbr.primarySettings.MaxExportJobs = 2
				mbr.backupTransferPartSize = *resource.NewQuantity(backupTransferPartSize, resource.BinarySI)

				target1 := createAndExportMantleBackup(ctx, mbr, "target1", ns, false, false, nil)
				target2 := createAndExportMantleBackup(ctx, mbr, "target2", ns, false, false, nil)

				// Complete all export Jobs
				for partNum := 0; partNum < numOfParts; partNum++ {
					completeJob(ctx, MakeExportJobName(target1, partNum))
					runStartExportAndUpload(ctx, target1)
					completeJob(ctx, MakeExportJobName(target2, partNum))
					runStartExportAndUpload(ctx, target2)
				}

				// Complete upload Jobs and check that they are deleted.
				for partNum := 0; partNum < numOfParts; partNum++ {
					completeJob(ctx, MakeUploadJobName(target1, partNum))
					runStartExportAndUpload(ctx, target1)
					runStartExportAndUpload(ctx, target2)

					// Make sure the upload Job for target 1 is deleted.
					if partNum > 0 {
						waitJobDeleted(ctx, MakeUploadJobName(target1, partNum-1))
					}
				}

				// Make sure the upload Job of part 0 for target 2 is NOT deleted.
				ensureJobNotDeleted(ctx, MakeUploadJobName(target2, 0))
			},
			Entry("snap size < transfer part size", int64(testutil.FakeRBDSnapshotSize)+1, 1),
			Entry("snap size = transfer part size", int64(testutil.FakeRBDSnapshotSize), 1),
			Entry("snap size > transfer part size", int64(testutil.FakeRBDSnapshotSize-1), 2),
		)
	})
})

var _ = Describe("import", func() {
	var mockCtrl *gomock.Controller
	var mbr *MantleBackupReconciler
	var nsController, ns string
	var mockObjectStorage *objectstorage.MockBucket

	BeforeEach(func() {
		var t reporter
		mockCtrl = gomock.NewController(t)
		mockObjectStorage = objectstorage.NewMockBucket(mockCtrl)

		nsController = resMgr.CreateNamespace()

		mbr = NewMantleBackupReconciler(
			k8sClient,
			scheme.Scheme,
			nsController,
			RoleSecondary,
			nil,
			"dummy-image",
			"dummy-env-secret",
			&ObjectStorageSettings{},
			nil,
			resource.MustParse("1Gi"),
		)
		mbr.objectStorageClient = mockObjectStorage
		mbr.ceph = testutil.NewFakeRBD()

		ns = resMgr.CreateNamespace()
	})

	AfterEach(func() {
		if mockCtrl != nil {
			mockCtrl.Finish()
		}
	})

	DescribeTable("isExportDataAlreadyUploaded: inputs and outputs",
		func(ctx SpecContext, gotExist, gotError, expectUploaded, expectError bool) {
			mockObjectStorage.EXPECT().Exists(gomock.Any(), gomock.Eq("name-uid-0.bin")).DoAndReturn(
				func(_ context.Context, _ string) (bool, error) {
					if gotError {
						return gotExist, errors.New("error")
					}
					return gotExist, nil
				})
			uploaded, err := mbr.isExportDataAlreadyUploaded(ctx, &mantlev1.MantleBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name: "name",
					Annotations: map[string]string{
						annotRemoteUID: "uid",
					},
				},
			}, 0)
			if expectError {
				Expect(err).To(HaveOccurred())
			} else {
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(uploaded).To(Equal(expectUploaded))
		},
		Entry("exist", true, false, true, false),
		Entry("not exist", false, false, false, false),
		Entry("error", false, true, false, true),
	)

	Context("reconcileImportJob", func() {
		It("should work correctly", func(ctx SpecContext) {
			backup, err := createMantleBackupUsingDummyPVC(ctx, "target", ns)
			Expect(err).NotTo(HaveOccurred())
			err = setStatusTransferPartSize(ctx, backup)
			Expect(err).NotTo(HaveOccurred())

			// set .status.snapSize
			err = updateStatus(ctx, k8sClient, backup, func() error {
				var i int64 = testutil.FakeRBDSnapshotSize
				backup.Status.SnapSize = &i
				return nil
			})
			Expect(err).NotTo(HaveOccurred())

			snapshotTarget := &snapshotTarget{
				pvc: &corev1.PersistentVolumeClaim{},
				pv: &corev1.PersistentVolume{
					Spec: corev1.PersistentVolumeSpec{
						PersistentVolumeSource: corev1.PersistentVolumeSource{
							CSI: &corev1.CSIPersistentVolumeSource{
								VolumeAttributes: map[string]string{
									"pool": "",
								},
							},
						},
					},
				},
				imageName: "",
				poolName:  "",
			}

			mockObjectStorage.EXPECT().Exists(gomock.Any(), gomock.Eq("target--0.bin")).DoAndReturn(
				func(_ context.Context, _ string) (bool, error) {
					return true, nil
				}).Times(2)

			// The first call to reconcileImportJob should create an import Job
			res, err := mbr.reconcileImportJob(ctx, backup, snapshotTarget, -1)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Requeue).To(BeTrue())

			var importJob batchv1.Job
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      MakeImportJobName(backup, 0),
				Namespace: nsController,
			}, &importJob)
			Expect(err).NotTo(HaveOccurred())

			// The successive calls should return ctrl.Result{Requeue: true} until the import Job is completed.
			res, err = mbr.reconcileImportJob(ctx, backup, snapshotTarget, -1)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Requeue).To(BeTrue())

			// Make the import Job completed.
			err = resMgr.ChangeJobCondition(ctx, &importJob, batchv1.JobComplete, corev1.ConditionTrue)
			Expect(err).NotTo(HaveOccurred())

			// Make dummy snapshot.
			err = mbr.ceph.RBDSnapCreate("", "", backup.GetName())
			Expect(err).NotTo(HaveOccurred())
			dummySnapshot, err := ceph.FindRBDSnapshot(mbr.ceph, "", "", backup.GetName())
			Expect(err).NotTo(HaveOccurred())

			// The call should update the status of the MantleBackup resource.
			largestCompletedPartNum := int(testutil.FakeRBDSnapshotSize/backup.Status.TransferPartSize.Value() - 1)
			res, err = mbr.reconcileImportJob(ctx, backup, snapshotTarget, largestCompletedPartNum)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.IsZero()).To(BeTrue())

			err = k8sClient.Get(ctx, types.NamespacedName{Name: backup.GetName(), Namespace: backup.GetNamespace()}, backup)
			Expect(err).NotTo(HaveOccurred())
			Expect(meta.IsStatusConditionTrue(backup.Status.Conditions, mantlev1.BackupConditionReadyToUse)).To(BeTrue())
			Expect(*backup.Status.SnapID).To(Equal(dummySnapshot.Id))
		})
	})

	// Utility functions used in the tests for primaryCleanup and secondaryCleanup.
	createTargetBackup := func(ctx SpecContext, name string, diffFrom, remoteUID *string) *mantlev1.MantleBackup {
		GinkgoHelper()
		backup, err := createMantleBackupUsingDummyPVC(ctx, name, ns)
		Expect(err).NotTo(HaveOccurred())
		m := map[string]string{}
		if diffFrom == nil {
			m[annotSyncMode] = syncModeFull
		} else {
			m[annotSyncMode] = syncModeIncremental
			m[annotDiffFrom] = *diffFrom
		}
		if remoteUID != nil {
			m[annotRemoteUID] = *remoteUID
		}
		backup.SetAnnotations(m)
		err = k8sClient.Update(ctx, backup)
		Expect(err).NotTo(HaveOccurred())
		err = setStatusTransferPartSize(ctx, backup)
		Expect(err).NotTo(HaveOccurred())
		err = createSnapshotForMantleBackupUsingDummyPVC(ctx, mbr.ceph, backup)
		Expect(err).NotTo(HaveOccurred())
		return backup
	}
	createJob := func(ctx context.Context, name string) {
		GinkgoHelper()
		var job batchv1.Job
		job.SetName(name)
		job.SetNamespace(nsController)
		job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyOnFailure
		job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "dummy", Image: "dummy"}}
		err := k8sClient.Create(ctx, &job)
		Expect(err).NotTo(HaveOccurred())
	}
	createPVC := func(ctx context.Context, name string) {
		GinkgoHelper()
		var pvc corev1.PersistentVolumeClaim
		pvc.SetName(name)
		pvc.SetNamespace(nsController)
		pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
		pvc.Spec.Resources.Requests = corev1.ResourceList(map[corev1.ResourceName]resource.Quantity{
			"storage": resource.MustParse("1Gi"),
		})
		err := k8sClient.Create(ctx, &pvc)
		Expect(err).NotTo(HaveOccurred())
	}
	checkPVCDeleted := func(ctx context.Context, name string) {
		GinkgoHelper()
		// Check that PVC has deletionTimestamp
		var pvc corev1.PersistentVolumeClaim
		err := k8sClient.Get(
			ctx,
			types.NamespacedName{Name: name, Namespace: nsController},
			&pvc,
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(pvc.GetDeletionTimestamp().IsZero()).To(BeFalse())
	}
	checkPVCExists := func(ctx context.Context, name string) {
		GinkgoHelper()
		var pvc corev1.PersistentVolumeClaim
		err := k8sClient.Get(
			ctx,
			types.NamespacedName{Name: name, Namespace: nsController},
			&pvc,
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(pvc.GetDeletionTimestamp().IsZero()).To(BeTrue())
	}
	checkJobDeleted := func(ctx context.Context, name string) {
		GinkgoHelper()
		var job batchv1.Job
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: nsController}, &job)
		Expect(err).To(HaveOccurred())
		Expect(aerrors.IsNotFound(err)).To(BeTrue())
	}
	checkJobExists := func(ctx context.Context, name string) {
		GinkgoHelper()
		var job batchv1.Job
		err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: nsController}, &job)
		Expect(err).NotTo(HaveOccurred())
	}

	Context("primaryCleanup", func() {
		// Utility functions used in the tests for primaryCleanup.
		createExportAndUploadJobs := func(ctx context.Context, backup *mantlev1.MantleBackup) {
			GinkgoHelper()
			createJob(ctx, MakeExportJobName(backup, 0))
			createJob(ctx, MakeUploadJobName(backup, 0))
		}
		createExportDataPVC := func(ctx context.Context, backup *mantlev1.MantleBackup) {
			GinkgoHelper()
			createPVC(ctx, MakeExportDataPVCName(backup, 0))
		}
		checkExportAndUploadJobsDeleted := func(ctx context.Context, backup *mantlev1.MantleBackup) {
			GinkgoHelper()
			checkJobDeleted(ctx, MakeExportJobName(backup, 0))
			checkJobDeleted(ctx, MakeUploadJobName(backup, 0))
		}
		checkExportDataPVCDeleted := func(ctx context.Context, backup *mantlev1.MantleBackup) {
			GinkgoHelper()
			checkPVCDeleted(ctx, MakeExportDataPVCName(backup, 0))
		}

		It("should delete annotations, Jobs, and PVCs, and update SyncedToRemote", func(ctx SpecContext) {
			// Create source MantleBackup
			source, err := createMantleBackupUsingDummyPVC(ctx, "source", ns)
			Expect(err).NotTo(HaveOccurred())
			source.SetAnnotations(map[string]string{
				annotDiffTo: "target",
			})
			err = k8sClient.Update(ctx, source)
			Expect(err).NotTo(HaveOccurred())
			err = createSnapshotForMantleBackupUsingDummyPVC(ctx, mbr.ceph, source)
			Expect(err).NotTo(HaveOccurred())

			// Create target MantleBackup
			backup := createTargetBackup(ctx, "target", ptr.To("source"), nil)

			// Create export and upload Jobs
			createExportAndUploadJobs(ctx, backup)

			// Create export data PVC
			createExportDataPVC(ctx, backup)

			// Perform primaryCleanup
			res, err := mbr.primaryCleanup(ctx, backup)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.IsZero()).To(BeTrue())

			// Check that sync-mode and diff-from annotations of the target MantleBackup are deleted
			err = k8sClient.Get(ctx, types.NamespacedName{Name: backup.GetName(), Namespace: backup.GetNamespace()}, backup)
			Expect(err).NotTo(HaveOccurred())
			_, ok := backup.GetAnnotations()[annotDiffFrom]
			Expect(ok).To(BeFalse())
			_, ok = backup.GetAnnotations()[annotSyncMode]
			Expect(ok).To(BeFalse())

			// Check that diff-to annotation of the source MantleBackup are deleted
			err = k8sClient.Get(ctx, types.NamespacedName{Name: source.GetName(), Namespace: source.GetNamespace()}, source)
			Expect(err).NotTo(HaveOccurred())
			_, ok = source.GetAnnotations()[annotDiffTo]
			Expect(ok).To(BeFalse())

			// Check that SyncedToRemote is set True
			Expect(meta.IsStatusConditionTrue(backup.Status.Conditions, mantlev1.BackupConditionSyncedToRemote)).To(BeTrue())

			// Check that the Jobs are deleted
			checkExportAndUploadJobsDeleted(ctx, backup)

			// Check that PVC has deletionTimestamp
			checkExportDataPVCDeleted(ctx, backup)
		})

		It("should work correctly if deletionTimestamp is set", func(ctx SpecContext) {
			backup, err := createMantleBackupUsingDummyPVC(ctx, "target", ns)
			Expect(err).NotTo(HaveOccurred())
			err = setStatusTransferPartSize(ctx, backup)
			Expect(err).NotTo(HaveOccurred())
			err = createSnapshotForMantleBackupUsingDummyPVC(ctx, mbr.ceph, backup)
			Expect(err).NotTo(HaveOccurred())

			err = k8sClient.Delete(ctx, backup)
			Expect(err).NotTo(HaveOccurred())

			// fetch the latest resourceVersion
			err = k8sClient.Get(ctx, types.NamespacedName{Name: backup.GetName(), Namespace: backup.GetNamespace()}, backup)
			Expect(err).NotTo(HaveOccurred())

			res, err := mbr.primaryCleanup(ctx, backup)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.IsZero()).To(BeTrue())

			// SyncedToRemote should NOT be true.
			Expect(meta.IsStatusConditionTrue(backup.Status.Conditions, mantlev1.BackupConditionSyncedToRemote)).To(BeFalse())
		})

		It("should not delete unrelated resources", func(ctx SpecContext) {
			// Arrange
			backup1 := createTargetBackup(ctx, "backup1", nil, nil)
			backup2 := createTargetBackup(ctx, "backup2", nil, nil)
			createExportAndUploadJobs(ctx, backup1)
			createExportAndUploadJobs(ctx, backup2)
			createExportDataPVC(ctx, backup1)
			createExportDataPVC(ctx, backup2)

			// Act
			res, err := mbr.primaryCleanup(ctx, backup1)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.IsZero()).To(BeTrue())

			// Assert
			// The resources for backup1 should be removed.
			checkExportAndUploadJobsDeleted(ctx, backup1)
			checkExportDataPVCDeleted(ctx, backup1)

			// The resources for backup2 should NOT be removed.
			checkJobExists(ctx, MakeExportJobName(backup2, 0))
			checkJobExists(ctx, MakeUploadJobName(backup2, 0))
			checkPVCExists(ctx, MakeExportDataPVCName(backup2, 0))
		})
	})

	Context("secondaryCleanup", func() {
		// Utility functions used in the tests for secondaryCleanup.
		createDiscardAndImportJobs := func(ctx context.Context, backup *mantlev1.MantleBackup) {
			GinkgoHelper()
			for _, name := range []string{MakeDiscardJobName(backup), MakeImportJobName(backup, 0)} {
				createJob(ctx, name)
			}
		}
		createDiscardDataPVC := func(ctx context.Context, backup *mantlev1.MantleBackup) {
			GinkgoHelper()
			createPVC(ctx, MakeDiscardPVCName(backup))
		}
		createDiscardDataPV := func(ctx context.Context, backup *mantlev1.MantleBackup) {
			GinkgoHelper()
			var discardDataPV corev1.PersistentVolume
			discardDataPV.SetName(MakeDiscardPVName(backup))
			discardDataPV.Spec.HostPath = &corev1.HostPathVolumeSource{Path: "/dummy"}
			discardDataPV.Spec.StorageClassName = "manual"
			discardDataPV.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
			discardDataPV.Spec.Capacity = corev1.ResourceList(map[corev1.ResourceName]resource.Quantity{
				"storage": resource.MustParse("1Gi"),
			})
			err := k8sClient.Create(ctx, &discardDataPV)
			Expect(err).NotTo(HaveOccurred())
		}
		expectAccessToObjectStorage := func(backup *mantlev1.MantleBackup) {
			prefix := fmt.Sprintf("%s-%s-", backup.GetName(), backup.GetAnnotations()[annotRemoteUID])
			mockObjectStorage.EXPECT().Delete(gomock.Any(), gomock.Eq(prefix+"0.bin")).Return(nil)
			mockObjectStorage.EXPECT().Delete(gomock.Any(), gomock.Eq(prefix+"1.bin")).Return(nil)
			mockObjectStorage.EXPECT().Delete(gomock.Any(), gomock.Eq(prefix+"2.bin")).Return(nil)
			mockObjectStorage.EXPECT().Delete(gomock.Any(), gomock.Eq(prefix+"3.bin")).Return(nil)
			mockObjectStorage.EXPECT().Delete(gomock.Any(), gomock.Eq(prefix+"4.bin")).Return(nil)
		}
		checkDiscardDataPVCDeleted := func(ctx context.Context, backup *mantlev1.MantleBackup) {
			checkPVCDeleted(ctx, MakeDiscardPVCName(backup))
		}
		checkDiscardDataPVDeleted := func(ctx context.Context, backup *mantlev1.MantleBackup) {
			var pv corev1.PersistentVolume
			err := k8sClient.Get(
				ctx,
				types.NamespacedName{Name: MakeDiscardPVName(backup)},
				&pv,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pv.GetDeletionTimestamp().IsZero()).To(BeFalse())
		}
		checkDiscardAndImportJobsDeleted := func(ctx context.Context, backup *mantlev1.MantleBackup) {
			checkJobDeleted(ctx, MakeDiscardJobName(backup))
			checkJobDeleted(ctx, MakeImportJobName(backup, 0))
		}

		It("should delete annotations, Jobs, PVs, PVCs, and exported data on the object storage", func(ctx SpecContext) {
			// Arrange
			// Create source MantleBackup
			source, err := createMantleBackupUsingDummyPVC(ctx, "source", ns)
			Expect(err).NotTo(HaveOccurred())
			source.SetAnnotations(map[string]string{
				annotDiffTo: "target",
			})
			err = k8sClient.Update(ctx, source)
			Expect(err).NotTo(HaveOccurred())
			err = setStatusTransferPartSize(ctx, source)
			Expect(err).NotTo(HaveOccurred())
			err = createSnapshotForMantleBackupUsingDummyPVC(ctx, mbr.ceph, source)
			Expect(err).NotTo(HaveOccurred())

			// Create target MantleBackup
			backup := createTargetBackup(ctx, "target", ptr.To("source"), ptr.To("uid"))

			// Create necessary resources
			createDiscardAndImportJobs(ctx, backup)

			// Create discard data PVC
			createDiscardDataPVC(ctx, backup)

			// Create discard data PV
			createDiscardDataPV(ctx, backup)

			// Expect access to the mocked object storage
			expectAccessToObjectStorage(backup)

			// Perform secondaryCleanup
			res, err := mbr.secondaryCleanup(ctx, backup, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.IsZero()).To(BeTrue())

			// Check that sync-mode and diff-from annotations of the target MantleBackup are deleted
			err = k8sClient.Get(ctx, types.NamespacedName{Name: backup.GetName(), Namespace: backup.GetNamespace()}, backup)
			Expect(err).NotTo(HaveOccurred())
			_, ok := backup.GetAnnotations()[annotDiffFrom]
			Expect(ok).To(BeFalse())
			_, ok = backup.GetAnnotations()[annotSyncMode]
			Expect(ok).To(BeFalse())

			// Check that diff-to annotation of the source MantleBackup are deleted
			err = k8sClient.Get(ctx, types.NamespacedName{Name: source.GetName(), Namespace: source.GetNamespace()}, source)
			Expect(err).NotTo(HaveOccurred())
			_, ok = source.GetAnnotations()[annotDiffTo]
			Expect(ok).To(BeFalse())

			// Check that the Jobs are deleted
			checkDiscardAndImportJobsDeleted(ctx, backup)

			// Check that PVC has deletionTimestamp
			checkDiscardDataPVCDeleted(ctx, backup)

			// Check that PV has deletionTimestamp
			checkDiscardDataPVDeleted(ctx, backup)
		})

		It("should work correctly if deletionTimestamp is set", func(ctx SpecContext) {
			backup, err := createMantleBackupUsingDummyPVC(ctx, "target", ns)
			Expect(err).NotTo(HaveOccurred())
			backup.SetAnnotations(map[string]string{
				annotRemoteUID: "uid",
			})
			err = k8sClient.Update(ctx, backup)
			Expect(err).NotTo(HaveOccurred())
			err = setStatusTransferPartSize(ctx, backup)
			Expect(err).NotTo(HaveOccurred())
			err = createSnapshotForMantleBackupUsingDummyPVC(ctx, mbr.ceph, backup)
			Expect(err).NotTo(HaveOccurred())

			err = k8sClient.Delete(ctx, backup)
			Expect(err).NotTo(HaveOccurred())

			// fetch the latest resourceVersion
			err = k8sClient.Get(ctx, types.NamespacedName{Name: backup.GetName(), Namespace: backup.GetNamespace()}, backup)
			Expect(err).NotTo(HaveOccurred())

			res, err := mbr.secondaryCleanup(ctx, backup, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.IsZero()).To(BeTrue())
		})

		It("should not delete unrelated resources", func(ctx SpecContext) {
			// Arrange
			backup1 := createTargetBackup(ctx, "backup1", nil, ptr.To("uid"))
			createDiscardAndImportJobs(ctx, backup1)
			createDiscardDataPVC(ctx, backup1)
			createDiscardDataPV(ctx, backup1)
			expectAccessToObjectStorage(backup1)
			backup2 := createTargetBackup(ctx, "backup2", nil, ptr.To("uid"))
			createDiscardAndImportJobs(ctx, backup2)
			createDiscardDataPVC(ctx, backup2)
			createDiscardDataPV(ctx, backup2)
			// We don't expect access to the (mocked) object storage for backup2, so
			// let's NOT call mockObjectStorage.EXPECT() here.

			// Act
			res, err := mbr.secondaryCleanup(ctx, backup1, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.IsZero()).To(BeTrue())

			// Assert
			// The resources for backup1 should be deleted.
			checkDiscardAndImportJobsDeleted(ctx, backup1)
			checkDiscardDataPVCDeleted(ctx, backup1)
			checkDiscardDataPVDeleted(ctx, backup1)

			// The resources for backup2 should NOT be deleted.
			checkJobExists(ctx, MakeDiscardJobName(backup2))
			checkJobExists(ctx, MakeImportJobName(backup2, 0))
			checkPVCExists(ctx, MakeDiscardPVCName(backup2))

			// The PV for backup2 should not be deleted.
			var pv corev1.PersistentVolume
			err = k8sClient.Get(
				ctx,
				types.NamespacedName{Name: MakeDiscardPVName(backup2)},
				&pv,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pv.GetDeletionTimestamp().IsZero()).To(BeTrue())

			// the snapshot for backup2 should not be deleted.
			snaps, err := mbr.ceph.RBDSnapLs(dummyPoolName, dummyImageName)
			Expect(err).NotTo(HaveOccurred())
			index := slices.IndexFunc(snaps, func(snap ceph.RBDSnapshot) bool {
				return snap.Id == *backup2.Status.SnapID
			})
			Expect(index).NotTo(Equal(-1))
		})
	})

	Context("reconcileDiscardJob", func() {
		It("should NOT create anything in an incremental backup", func(ctx SpecContext) {
			backup, err := createMantleBackupUsingDummyPVC(ctx, "target", ns)
			Expect(err).NotTo(HaveOccurred())
			backup.SetAnnotations(map[string]string{
				annotDiffFrom:  "source",
				annotSyncMode:  syncModeIncremental,
				annotRemoteUID: "uid",
			})
			err = k8sClient.Update(ctx, backup)
			Expect(err).NotTo(HaveOccurred())

			result, err := mbr.reconcileDiscardJob(ctx, backup, &snapshotTarget{})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			var pv corev1.PersistentVolume
			err = k8sClient.Get(ctx, types.NamespacedName{Name: MakeDiscardPVName(backup)}, &pv)
			Expect(err).To(HaveOccurred())
			Expect(aerrors.IsNotFound(err)).To(BeTrue())

			var pvc corev1.PersistentVolume
			err = k8sClient.Get(ctx, types.NamespacedName{Name: MakeDiscardPVCName(backup), Namespace: nsController}, &pvc)
			Expect(err).To(HaveOccurred())
			Expect(aerrors.IsNotFound(err)).To(BeTrue())

			var job batchv1.Job
			err = k8sClient.Get(ctx, types.NamespacedName{Name: MakeDiscardJobName(backup), Namespace: nsController}, &job)
			Expect(err).To(HaveOccurred())
			Expect(aerrors.IsNotFound(err)).To(BeTrue())
		})

		It("should create a PV, PVC, and Job, requeue, and complete in a full backup", func(ctx SpecContext) {
			backup, err := createMantleBackupUsingDummyPVC(ctx, "target", ns)
			Expect(err).NotTo(HaveOccurred())
			backup.SetAnnotations(map[string]string{
				annotSyncMode: syncModeFull,
			})
			err = k8sClient.Update(ctx, backup)
			Expect(err).NotTo(HaveOccurred())

			pvCapacity := corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("1Gi"),
			}
			pvDriver := "test-pv-driver"
			pvControllerExpandSecretRef := corev1.SecretReference{
				Name:      "test-pv-cesr-name",
				Namespace: "test-pv-cesr-ns",
			}
			pvNodeStageSecretRef := corev1.SecretReference{
				Name:      "test-pv-nssr-name",
				Namespace: "test-pv-nssr-ns",
			}
			pvClusterID := "test-pv-cluster-id"
			pvImageFeatures := "test-pv-image-features"
			pvImageFormat := "test-pv-image-format"
			pvPool := "test-pv-pool"
			pvImageName := "test-pv-image-name"
			pvcResources := corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			}
			snapshotTarget := &snapshotTarget{
				pvc: &corev1.PersistentVolumeClaim{
					Spec: corev1.PersistentVolumeClaimSpec{
						Resources: pvcResources,
					},
				},
				pv: &corev1.PersistentVolume{
					Spec: corev1.PersistentVolumeSpec{
						Capacity: pvCapacity,
						PersistentVolumeSource: corev1.PersistentVolumeSource{
							CSI: &corev1.CSIPersistentVolumeSource{
								Driver:                    pvDriver,
								ControllerExpandSecretRef: &pvControllerExpandSecretRef,
								NodeStageSecretRef:        &pvNodeStageSecretRef,
								VolumeAttributes: map[string]string{
									"clusterID":     pvClusterID,
									"imageFeatures": pvImageFeatures,
									"imageFormat":   pvImageFormat,
									"pool":          pvPool,
									"imageName":     pvImageName,
								},
							},
						},
					},
				},
				imageName: pvImageName,
				poolName:  "poolName",
			}

			// The first call to reconcileDiscardJob should create a PV, PVC, and Job, and requeue.
			result, err := mbr.reconcileDiscardJob(ctx, backup, snapshotTarget)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeTrue())

			var pv corev1.PersistentVolume
			err = k8sClient.Get(ctx, types.NamespacedName{Name: MakeDiscardPVName(backup), Namespace: nsController}, &pv)
			Expect(err).NotTo(HaveOccurred())
			Expect(pv.GetLabels()["app.kubernetes.io/name"]).To(Equal(labelAppNameValue))
			Expect(pv.GetLabels()["app.kubernetes.io/component"]).To(Equal(labelComponentDiscardVolume))
			Expect(len(pv.Spec.AccessModes)).To(Equal(1))
			Expect(pv.Spec.AccessModes[0]).To(Equal(corev1.ReadWriteOnce))
			Expect(pv.Spec.Capacity).To(Equal(pvCapacity))
			Expect(pv.Spec.CSI.Driver).To(Equal(pvDriver))
			Expect(*pv.Spec.CSI.ControllerExpandSecretRef).To(Equal(pvControllerExpandSecretRef))
			Expect(*pv.Spec.CSI.NodeStageSecretRef).To(Equal(pvNodeStageSecretRef))
			Expect(pv.Spec.CSI.VolumeAttributes["clusterID"]).To(Equal(pvClusterID))
			Expect(pv.Spec.CSI.VolumeAttributes["imageFeatures"]).To(Equal(pvImageFeatures))
			Expect(pv.Spec.CSI.VolumeAttributes["imageFormat"]).To(Equal(pvImageFormat))
			Expect(pv.Spec.CSI.VolumeAttributes["pool"]).To(Equal(pvPool))
			Expect(pv.Spec.CSI.VolumeAttributes["staticVolume"]).To(Equal("true"))
			Expect(pv.Spec.CSI.VolumeHandle).To(Equal(pvImageName))
			Expect(pv.Spec.PersistentVolumeReclaimPolicy).To(Equal(corev1.PersistentVolumeReclaimRetain))
			Expect(*pv.Spec.VolumeMode).To(Equal(corev1.PersistentVolumeBlock))
			Expect(pv.Spec.StorageClassName).To(Equal(""))

			var pvc corev1.PersistentVolumeClaim
			err = k8sClient.Get(ctx, types.NamespacedName{Name: MakeDiscardPVName(backup), Namespace: nsController}, &pvc)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc.GetLabels()["app.kubernetes.io/name"]).To(Equal(labelAppNameValue))
			Expect(pvc.GetLabels()["app.kubernetes.io/component"]).To(Equal(labelComponentDiscardVolume))
			Expect(*pvc.Spec.StorageClassName).To(Equal(""))
			Expect(len(pvc.Spec.AccessModes)).To(Equal(1))
			Expect(pvc.Spec.AccessModes[0]).To(Equal(corev1.ReadWriteOnce))
			Expect(pvc.Spec.Resources).To(Equal(pvcResources))
			Expect(*pvc.Spec.VolumeMode).To(Equal(corev1.PersistentVolumeBlock))
			Expect(pvc.Spec.VolumeName).To(Equal(MakeDiscardPVName(backup)))

			var job batchv1.Job
			err = k8sClient.Get(ctx, types.NamespacedName{Name: MakeDiscardJobName(backup), Namespace: nsController}, &job)
			Expect(err).NotTo(HaveOccurred())
			Expect(job.GetLabels()["app.kubernetes.io/name"]).To(Equal(labelAppNameValue))
			Expect(job.GetLabels()["app.kubernetes.io/component"]).To(Equal(labelComponentDiscardJob))
			Expect(*job.Spec.BackoffLimit).To(Equal(int32(65535)))
			Expect(len(job.Spec.Template.Spec.Containers)).To(Equal(1))
			Expect(job.Spec.Template.Spec.Containers[0].Name).To(Equal("discard"))
			Expect(*job.Spec.Template.Spec.Containers[0].SecurityContext.Privileged).To(BeTrue())
			Expect(*job.Spec.Template.Spec.Containers[0].SecurityContext.RunAsGroup).To(Equal(int64(0)))
			Expect(*job.Spec.Template.Spec.Containers[0].SecurityContext.RunAsUser).To(Equal(int64(0)))
			Expect(job.Spec.Template.Spec.Containers[0].Image).To(Equal(mbr.podImage))
			Expect(len(job.Spec.Template.Spec.Containers[0].VolumeDevices)).To(Equal(1))
			Expect(job.Spec.Template.Spec.Containers[0].VolumeDevices[0].Name).To(Equal("discard-rbd"))
			Expect(job.Spec.Template.Spec.Containers[0].VolumeDevices[0].DevicePath).To(Equal("/dev/discard-rbd"))
			Expect(job.Spec.Template.Spec.RestartPolicy).To(Equal(corev1.RestartPolicyOnFailure))
			Expect(len(job.Spec.Template.Spec.Volumes)).To(Equal(1))
			Expect(job.Spec.Template.Spec.Volumes[0]).To(Equal(corev1.Volume{
				Name: "discard-rbd",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: MakeDiscardPVCName(backup),
					},
				},
			}))

			// Make the Job completed
			err = resMgr.ChangeJobCondition(ctx, &job, batchv1.JobComplete, corev1.ConditionTrue)
			Expect(err).NotTo(HaveOccurred())

			// A call to reconcileDiscardJob should NOT requeue after the Job completed
			result, err = mbr.reconcileDiscardJob(ctx, backup, snapshotTarget)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())
		})
	})

	Context("handleCompletedImportJobs", func() {
		DescribeTable(
			"Deletion of completed import Jobs",
			func(ctx SpecContext, backupTransferPartSize int64, numOfParts int) {
				createImportJob := func(backup *mantlev1.MantleBackup, partNum int) *batchv1.Job {
					job := batchv1.Job{
						ObjectMeta: metav1.ObjectMeta{
							Name:      MakeImportJobName(backup, partNum),
							Namespace: nsController,
							Labels: map[string]string{
								"app.kubernetes.io/name":      labelAppNameValue,
								"app.kubernetes.io/component": labelComponentImportJob,
							},
						},
						Spec: batchv1.JobSpec{
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									RestartPolicy: corev1.RestartPolicyOnFailure,
									Containers: []corev1.Container{
										{
											Name:  "dummy",
											Image: "dummy",
										},
									},
								},
							},
						},
					}
					err := k8sClient.Create(ctx, &job)
					Expect(err).NotTo(HaveOccurred())
					return &job
				}

				// Create two MantleBackups
				backup1, err := createMantleBackupUsingDummyPVC(ctx, "target1", ns)
				Expect(err).NotTo(HaveOccurred())
				backup2, err := createMantleBackupUsingDummyPVC(ctx, "target2", ns)
				Expect(err).NotTo(HaveOccurred())

				// Emulate the reconciler; create an import Job for each MantleBackup.
				job1 := createImportJob(backup1, 0)
				_ = createImportJob(backup2, 0)

				// Make the import Job for MantleBackup1 Completed.
				err = resMgr.ChangeJobCondition(ctx, job1, batchv1.JobComplete, corev1.ConditionTrue)
				Expect(err).NotTo(HaveOccurred())

				// Call handleCompletedImportJobs for MantleBackup1.
				largestCompletedPartNum, err := mbr.handleCompletedImportJobs(ctx, backup1)
				Expect(err).NotTo(HaveOccurred())
				Expect(largestCompletedPartNum).To(Equal(0))

				// The import Job for MantleBackup1 should NOT be deleted because it's the latest.
				Consistently(ctx, func(g Gomega, ctx SpecContext) {
					var job batchv1.Job
					err = k8sClient.Get(ctx, types.NamespacedName{Name: MakeImportJobName(backup1, 0), Namespace: nsController}, &job)
					Expect(err).NotTo(HaveOccurred())
				}, "3s", "1s").Should(Succeed())

				// Create a new import Job for part number 2 of MantleBackup1, and make it Completed.
				job2 := createImportJob(backup1, 1)
				err = resMgr.ChangeJobCondition(ctx, job2, batchv1.JobComplete, corev1.ConditionTrue)
				Expect(err).NotTo(HaveOccurred())

				// Call handleCompletedImportJobs for MantleBackup1.
				largestCompletedPartNum, err = mbr.handleCompletedImportJobs(ctx, backup1)
				Expect(err).NotTo(HaveOccurred())
				Expect(largestCompletedPartNum).To(Equal(1))

				// Now the import Job for part number 1 of MantleBackup1 should be deleted.
				Eventually(ctx, func(g Gomega, ctx SpecContext) {
					var job batchv1.Job
					err = k8sClient.Get(ctx, types.NamespacedName{Name: MakeImportJobName(backup1, 0), Namespace: nsController}, &job)
					Expect(err).To(HaveOccurred())
					Expect(aerrors.IsNotFound(err)).To(BeTrue())
				}).Should(Succeed())

				// The import Job for MantleBackup2 should NOT be deleted.
				Consistently(ctx, func(g Gomega, ctx SpecContext) {
					var job batchv1.Job
					err = k8sClient.Get(ctx, types.NamespacedName{Name: MakeImportJobName(backup2, 0), Namespace: nsController}, &job)
					Expect(err).NotTo(HaveOccurred())
				}, "3s", "1s").Should(Succeed())
			},
			Entry("snap size < transfer part size", int64(testutil.FakeRBDSnapshotSize)+1, 1),
			Entry("snap size = transfer part size", int64(testutil.FakeRBDSnapshotSize), 1),
			Entry("snap size > transfer part size", int64(testutil.FakeRBDSnapshotSize-1), 2),
		)
	})
})
