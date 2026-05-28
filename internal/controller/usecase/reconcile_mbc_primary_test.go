package usecase_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/cybozu-go/mantle/internal/controller/infra"
	"github.com/cybozu-go/mantle/internal/controller/usecase"
	"github.com/cybozu-go/mantle/test/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	aerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

var k8sClient client.Client //nolint:gochecknoglobals

func TestMain(m *testing.M) {
	kubernetesVersion := os.Getenv("ENVTEST_KUBERNETES_VERSION")
	if kubernetesVersion == "" {
		kubernetesVersion = "1.34.0" // Set default value to make VSCode's Go extension work.
	}

	binaryAssetsDirectory := os.Getenv("ENVTEST_BIN_DIR")
	if binaryAssetsDirectory == "" {
		binaryAssetsDirectory = "../../../bin" // Set default value to make VSCode's Go extension work.
	}

	testEnv := &envtest.Environment{
		CRDDirectoryPaths:           []string{filepath.Join("..", "..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing:       true,
		DownloadBinaryAssets:        true,
		DownloadBinaryAssetsVersion: "v" + kubernetesVersion,
		BinaryAssetsDirectory:       binaryAssetsDirectory,
	}

	cfg, err := testEnv.Start()
	if err != nil {
		panic(err)
	}

	if cfg == nil {
		panic("cfg is nil")
	}

	defer func() {
		err := testEnv.Stop()
		if err != nil {
			panic(err)
		}
	}()

	err = mantlev1.AddToScheme(scheme.Scheme)
	if err != nil {
		panic(err)
	}

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		panic(err)
	}

	m.Run()
}

func newUsecase(cephClusterID, cronJobNamespace string) *usecase.ReconcileMBCPrimary {
	return usecase.NewReconcileMBCPrimary(
		cephClusterID,
		infra.NewKubernetesClient(k8sClient),
		cronJobNamespace,
		"",
		"sa-name",
		"controller-image",
	)
}

type testEnvFixture struct {
	mbcName          string
	mbcNamespace     string
	cronJobNamespace string
	cephClusterID    string
	pvcName          string
}

func setupTestEnv(t *testing.T, cephClusterID string) *testEnvFixture {
	t.Helper()

	fixture := &testEnvFixture{
		mbcName:          util.GetUniqueName("mbc-"),
		mbcNamespace:     util.GetUniqueName("ns-"),
		cronJobNamespace: util.GetUniqueName("ns-"),
		cephClusterID:    cephClusterID,
		pvcName:          util.GetUniqueName("pvc-"),
	}

	scName := util.GetUniqueName("sc-")

	for _, ns := range []string{fixture.mbcNamespace, fixture.cronJobNamespace} {
		err := k8sClient.Create(t.Context(), &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns},
		})
		require.NoError(t, err)
	}

	err := k8sClient.Create(t.Context(), &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: scName},
		Provisioner: "test.rbd.csi.ceph.com",
		Parameters:  map[string]string{"clusterID": cephClusterID},
	})
	require.NoError(t, err)

	err = k8sClient.Create(t.Context(), &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fixture.pvcName,
			Namespace: fixture.mbcNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	})
	require.NoError(t, err)

	return fixture
}

func (f *testEnvFixture) createMBC(
	t *testing.T,
) *mantlev1.MantleBackupConfig {
	t.Helper()

	mbc := &mantlev1.MantleBackupConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      f.mbcName,
			Namespace: f.mbcNamespace,
		},
		Spec: mantlev1.MantleBackupConfigSpec{
			PVC:      f.pvcName,
			Schedule: "0 0 * * *",
			Expire:   "2w",
			Suspend:  false,
		},
	}

	err := k8sClient.Create(t.Context(), mbc)
	require.NoError(t, err)

	return mbc
}

func (f *testEnvFixture) mbcNN() types.NamespacedName {
	return types.NamespacedName{Name: f.mbcName, Namespace: f.mbcNamespace}
}

func (f *testEnvFixture) provisionMBC(
	t *testing.T, useCase *usecase.ReconcileMBCPrimary,
) {
	t.Helper()

	err := useCase.Run(t.Context(), f.mbcNN())
	require.NoError(t, err)

	err = useCase.Run(t.Context(), f.mbcNN())
	require.NoError(t, err)
}

func TestRun_FinalizePath_Success(t *testing.T) {
	t.Parallel()

	fixture := setupTestEnv(t, util.GetUniqueName("ceph-cluster-id"))
	mbc := fixture.createMBC(t)
	useCase := newUsecase(fixture.cephClusterID, fixture.cronJobNamespace)

	fixture.provisionMBC(t, useCase)

	cronJobNN := types.NamespacedName{
		Name:      domain.GetMBCCronJobName(mbc),
		Namespace: fixture.cronJobNamespace,
	}

	err := k8sClient.Get(t.Context(), cronJobNN, &batchv1.CronJob{})
	require.NoError(t, err)

	err = k8sClient.Delete(t.Context(), mbc)
	require.NoError(t, err)

	err = useCase.Run(t.Context(), fixture.mbcNN())
	require.NoError(t, err)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		err = k8sClient.Get(t.Context(), cronJobNN, &batchv1.CronJob{})
		assert.Error(collect, err)
		assert.True(collect, aerrors.IsNotFound(err))
	}, 5*time.Second, 1*time.Second)

	err = useCase.Run(t.Context(), fixture.mbcNN())
	require.NoError(t, err)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		err = k8sClient.Get(
			t.Context(), fixture.mbcNN(), &mantlev1.MantleBackupConfig{},
		)
		assert.Error(collect, err)
		assert.True(collect, aerrors.IsNotFound(err))
	}, 5*time.Second, 1*time.Second)
}

func TestRun_ProvisionPath_FullLifecycle(t *testing.T) {
	t.Parallel()

	cephClusterID := util.GetUniqueName("ceph-cluster-id")
	fixture := setupTestEnv(t, cephClusterID)
	mbc := fixture.createMBC(t)
	useCase := newUsecase(cephClusterID, fixture.cronJobNamespace)

	err := useCase.Run(t.Context(), fixture.mbcNN())
	require.NoError(t, err)

	var updatedMBC mantlev1.MantleBackupConfig

	err = k8sClient.Get(t.Context(), fixture.mbcNN(), &updatedMBC)
	require.NoError(t, err)
	require.Contains(t, updatedMBC.Finalizers, domain.MantleBackupConfigFinalizerName)
	require.Equal(t, cephClusterID,
		updatedMBC.Annotations[domain.MantleBackupConfigAnnotationManagedClusterID])

	err = useCase.Run(t.Context(), fixture.mbcNN())
	require.NoError(t, err)

	cronJobNN := types.NamespacedName{
		Name:      domain.GetMBCCronJobName(mbc),
		Namespace: fixture.cronJobNamespace,
	}

	var cronJob batchv1.CronJob

	err = k8sClient.Get(t.Context(), cronJobNN, &cronJob)
	require.NoError(t, err)
	require.Equal(t, "0 0 * * *", cronJob.Spec.Schedule)
	require.False(t, *cronJob.Spec.Suspend)
	require.Equal(t, batchv1.ForbidConcurrent, cronJob.Spec.ConcurrencyPolicy)
	require.Equal(t, "sa-name",
		cronJob.Spec.JobTemplate.Spec.Template.Spec.ServiceAccountName)
	require.Equal(t, "controller-image",
		cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Image)
}

func TestRun_ProvisionPath_NotResponsibleCluster(t *testing.T) {
	t.Parallel()

	fixture := setupTestEnv(t, "other-cluster-id")
	mbc := fixture.createMBC(t)
	useCase := newUsecase("my-cluster-id", fixture.cronJobNamespace)

	err := useCase.Run(t.Context(), fixture.mbcNN())
	require.NoError(t, err)

	var updatedMBC mantlev1.MantleBackupConfig

	err = k8sClient.Get(t.Context(), fixture.mbcNN(), &updatedMBC)
	require.NoError(t, err)
	require.NotContains(t, updatedMBC.Finalizers, domain.MantleBackupConfigFinalizerName)

	cronJobNN := types.NamespacedName{
		Name:      domain.GetMBCCronJobName(mbc),
		Namespace: fixture.cronJobNamespace,
	}

	err = k8sClient.Get(t.Context(), cronJobNN, &batchv1.CronJob{})
	require.True(t, aerrors.IsNotFound(err))
}

func TestRun_ProvisionPath_CronJobUpdate(t *testing.T) {
	t.Parallel()

	cephClusterID := util.GetUniqueName("ceph-cluster-id")
	fixture := setupTestEnv(t, cephClusterID)
	mbc := fixture.createMBC(t)
	useCase := newUsecase(cephClusterID, fixture.cronJobNamespace)

	fixture.provisionMBC(t, useCase)

	cronJobNN := types.NamespacedName{
		Name:      domain.GetMBCCronJobName(mbc),
		Namespace: fixture.cronJobNamespace,
	}

	var cronJob batchv1.CronJob

	err := k8sClient.Get(t.Context(), cronJobNN, &cronJob)
	require.NoError(t, err)
	require.Equal(t, "0 0 * * *", cronJob.Spec.Schedule)
	require.False(t, *cronJob.Spec.Suspend)

	var updatedMBC mantlev1.MantleBackupConfig

	err = k8sClient.Get(t.Context(), fixture.mbcNN(), &updatedMBC)
	require.NoError(t, err)

	updatedMBC.Spec.Schedule = "0 12 * * *"
	updatedMBC.Spec.Suspend = true
	err = k8sClient.Update(t.Context(), &updatedMBC)
	require.NoError(t, err)

	err = useCase.Run(t.Context(), fixture.mbcNN())
	require.NoError(t, err)

	err = k8sClient.Get(t.Context(), cronJobNN, &cronJob)
	require.NoError(t, err)
	require.Equal(t, "0 12 * * *", cronJob.Spec.Schedule)
	require.True(t, *cronJob.Spec.Suspend)
}
