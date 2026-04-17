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
	aerrors "k8s.io/apimachinery/pkg/api/errors"
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
	return usecase.NewReconcileMBCPrimary(cephClusterID, infra.NewKubernetesClient(k8sClient), cronJobNamespace)
}

//nolint:funlen // After we implement the provision logic, the length will be reduced.
func TestRun_FinalizePath_Success(t *testing.T) {
	t.Parallel()

	mbcName := util.GetUniqueName("mbc-")
	mbcNamespace := util.GetUniqueName("ns-")
	cronJobNamespace := util.GetUniqueName("ns-")
	cephClusterID := util.GetUniqueName("managed-ceph-cluster-id")

	// Create namespaces
	for _, ns := range []string{mbcNamespace, cronJobNamespace} {
		err := k8sClient.Create(t.Context(), &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns},
		})
		require.NoError(t, err)
	}

	// Create MBC
	mbc := &mantlev1.MantleBackupConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mbcName,
			Namespace: mbcNamespace,
			// FIXME: After we implement the provision logic, we should not set
			// the finalizer and annotation manually in this test.
			Finalizers: []string{
				domain.MantleBackupConfigFinalizerName,
			},
			Annotations: map[string]string{
				domain.MantleBackupConfigAnnotationManagedClusterID: cephClusterID,
			},
		},
		Spec: mantlev1.MantleBackupConfigSpec{
			PVC:      "pvc-name",
			Schedule: "0 0 * * *",
			Expire:   "2w",
			Suspend:  false,
		},
	}
	err := k8sClient.Create(t.Context(), mbc)
	require.NoError(t, err)

	// Create CronJob
	// FIXME: After we implement the provision logic, we should not create the CronJob manually in this test.
	err = k8sClient.Create(t.Context(), &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      domain.GetMBCCronJobName(mbc),
			Namespace: cronJobNamespace,
		},
		Spec: batchv1.CronJobSpec{
			Schedule: "0 0 * * *",
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name:  "backup",
								Image: "backup-image",
							}},
							RestartPolicy: corev1.RestartPolicyOnFailure,
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	// Delete MBC to trigger the finalization logic
	err = k8sClient.Delete(t.Context(), mbc)
	require.NoError(t, err)

	// Run the usecase
	useCase := newUsecase(cephClusterID, cronJobNamespace)
	err = useCase.Run(
		t.Context(),
		types.NamespacedName{Name: mbcName, Namespace: mbcNamespace},
	)
	require.NoError(t, err)

	// Verify that the CronJob is deleted
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		err = k8sClient.Get(
			t.Context(),
			types.NamespacedName{Name: domain.GetMBCCronJobName(mbc), Namespace: cronJobNamespace},
			&batchv1.CronJob{},
		)
		assert.Error(collect, err)
		assert.True(collect, aerrors.IsNotFound(err))
	}, 5*time.Second, 1*time.Second)

	// Run the usecase a second time to verify that the finalizer is removed.
	// The first Run deletes the CronJob; the second Run should remove the finalizer.
	err = useCase.Run(
		t.Context(),
		types.NamespacedName{Name: mbcName, Namespace: mbcNamespace},
	)
	require.NoError(t, err)

	// Verify that the MBC has been garbage-collected by the API server.
	// Once the finalizer is removed from a resource with DeletionTimestamp,
	// Kubernetes deletes it automatically.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		err = k8sClient.Get(
			t.Context(),
			types.NamespacedName{Name: mbcName, Namespace: mbcNamespace},
			&mantlev1.MantleBackupConfig{},
		)
		assert.Error(collect, err)
		assert.True(collect, aerrors.IsNotFound(err))
	}, 5*time.Second, 1*time.Second)
}
