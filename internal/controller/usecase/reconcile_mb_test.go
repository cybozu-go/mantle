package usecase_test

import (
	"context"
	"testing"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
	"github.com/cybozu-go/mantle/internal/controller/usecase"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	mbManagedCephClusterID = "managed-ceph-cluster-id"
	mbOtherCephClusterID   = "other-ceph-cluster-id"
	mbNamespace            = "mb-namespace"
	mbName                 = "mb-name"
	mbPVCName              = "pvc-name"
)

// fakeMBKubernetesClient is a usecase.KubernetesClient backed by the fake client of
// controller-runtime. It records the calls that the use case makes to it.
type fakeMBKubernetesClient struct {
	client.Client

	pvcClusterIDs    map[string]string
	clusterIDLookups []string
}

var _ usecase.KubernetesClient = (*fakeMBKubernetesClient)(nil)

func (c *fakeMBKubernetesClient) ApplyReconcilerOperations(context.Context, []domain.Operation) error {
	return nil
}

func (c *fakeMBKubernetesClient) GetCephClusterIDFromPVC(
	_ context.Context, pvc *corev1.PersistentVolumeClaim,
) (string, error) {
	c.clusterIDLookups = append(c.clusterIDLookups, pvc.Name)

	return c.pvcClusterIDs[pvc.Name], nil
}

type mbRunner interface {
	Run(ctx context.Context, backupNamespacedName types.NamespacedName) *reconcile.Result
}

type mbRole struct {
	name string
	// opts are applied to every MantleBackup so that it passes the ownership guard of the role.
	opts      []func(*mantlev1.MantleBackup)
	newRunner func(k8sClient usecase.KubernetesClient) mbRunner
}

func mbRoles() []mbRole {
	return []mbRole{
		{
			name: "standalone",
			opts: nil,
			newRunner: func(k8sClient usecase.KubernetesClient) mbRunner {
				return usecase.NewReconcileMBStandalone(k8sClient, mbManagedCephClusterID)
			},
		},
		{
			name: "primary",
			opts: nil,
			newRunner: func(k8sClient usecase.KubernetesClient) mbRunner {
				return usecase.NewReconcileMBPrimary(k8sClient, mbManagedCephClusterID)
			},
		},
		{
			name: "secondary",
			opts: []func(*mantlev1.MantleBackup){func(backup *mantlev1.MantleBackup) {
				backup.Annotations[domain.AnnotRemoteUID] = "remote-uid"
			}},
			newRunner: func(k8sClient usecase.KubernetesClient) mbRunner {
				return usecase.NewReconcileMBSecondary(k8sClient, mbManagedCephClusterID)
			},
		},
	}
}

func newTestMB(role mbRole, opts ...func(*mantlev1.MantleBackup)) *mantlev1.MantleBackup {
	backup := &mantlev1.MantleBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:        mbName,
			Namespace:   mbNamespace,
			Labels:      map[string]string{},
			Annotations: map[string]string{},
		},
		Spec: mantlev1.MantleBackupSpec{
			PVC: mbPVCName,
		},
	}
	for _, opt := range append(role.opts, opts...) {
		opt(backup)
	}

	return backup
}

func withMBClusterIDLabel(clusterID string) func(*mantlev1.MantleBackup) {
	return func(backup *mantlev1.MantleBackup) {
		backup.Labels[domain.LabelClusterID] = clusterID
	}
}

func newTestPVC() *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mbPVCName,
			Namespace: mbNamespace,
		},
	}
}

func newFakeMBKubernetesClient(t *testing.T, objects ...client.Object) *fakeMBKubernetesClient {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, mantlev1.AddToScheme(scheme))

	return &fakeMBKubernetesClient{
		Client: fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(objects...).
			WithStatusSubresource(&mantlev1.MantleBackup{}).
			Build(),
		pvcClusterIDs:    map[string]string{},
		clusterIDLookups: nil,
	}
}

type managedBackupCheckTestCase struct {
	name string
	// backupOpts are the options of the MantleBackup. A nil backupOpts means
	// that the MantleBackup does not exist.
	backupOpts []func(*mantlev1.MantleBackup)
	// pvcClusterID is the Ceph cluster ID of the PVC. A nil pvcClusterID means
	// that the PVC does not exist.
	pvcClusterID             *string
	expectedResult           *reconcile.Result
	expectedClusterIDLookups []string
}

func managedBackupCheckTestCases() []managedBackupCheckTestCase {
	return []managedBackupCheckTestCase{
		{
			name:                     "succeeds when the MantleBackup is not found",
			backupOpts:               nil,
			pvcClusterID:             nil,
			expectedResult:           reconcile.Succeeded(),
			expectedClusterIDLookups: nil,
		},
		{
			name: "skips the backup labeled for another cluster without the role-specific processing",
			backupOpts: []func(*mantlev1.MantleBackup){
				withMBClusterIDLabel(mbOtherCephClusterID),
			},
			pvcClusterID:             nil,
			expectedResult:           reconcile.Succeeded(),
			expectedClusterIDLookups: nil,
		},
		{
			name:                     "skips the unlabeled backup whose PVC belongs to another cluster",
			backupOpts:               []func(*mantlev1.MantleBackup){},
			pvcClusterID:             new(mbOtherCephClusterID),
			expectedResult:           reconcile.Succeeded(),
			expectedClusterIDLookups: []string{mbPVCName},
		},
		{
			name:                     "requeues the unlabeled backup whose PVC is not found",
			backupOpts:               []func(*mantlev1.MantleBackup){},
			pvcClusterID:             nil,
			expectedResult:           reconcile.Requeue(),
			expectedClusterIDLookups: nil,
		},
		{
			name: "processes the backup labeled for the managed cluster without looking up the PVC",
			backupOpts: []func(*mantlev1.MantleBackup){
				withMBClusterIDLabel(mbManagedCephClusterID),
			},
			pvcClusterID:             nil,
			expectedResult:           reconcile.ContinueWithLegacyReconcile(),
			expectedClusterIDLookups: nil,
		},
		{
			name:                     "processes the unlabeled backup whose PVC belongs to the managed cluster",
			backupOpts:               []func(*mantlev1.MantleBackup){},
			pvcClusterID:             new(mbManagedCephClusterID),
			expectedResult:           reconcile.ContinueWithLegacyReconcile(),
			expectedClusterIDLookups: []string{mbPVCName},
		},
	}
}

func TestReconcileMB_Run_ManagedBackupCheck(t *testing.T) {
	t.Parallel()

	for _, role := range mbRoles() {
		for _, testCase := range managedBackupCheckTestCases() {
			t.Run(role.name+"/"+testCase.name, func(t *testing.T) {
				t.Parallel()

				// Arrange
				objects := []client.Object{}
				if testCase.backupOpts != nil {
					objects = append(objects, newTestMB(role, testCase.backupOpts...))
				}

				if testCase.pvcClusterID != nil {
					objects = append(objects, newTestPVC())
				}

				k8sClient := newFakeMBKubernetesClient(t, objects...)
				if testCase.pvcClusterID != nil {
					k8sClient.pvcClusterIDs[mbPVCName] = *testCase.pvcClusterID
				}

				runner := role.newRunner(k8sClient)

				// Act
				result := runner.Run(t.Context(), types.NamespacedName{Namespace: mbNamespace, Name: mbName})

				// Assert
				require.Equal(t, testCase.expectedResult, result)
				require.Equal(t, testCase.expectedClusterIDLookups, k8sClient.clusterIDLookups)
			})
		}
	}
}
