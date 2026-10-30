package infra_test

import (
	"context"
	"errors"
	"testing"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/cybozu-go/mantle/internal/controller/infra"
	"github.com/stretchr/testify/require"
	aerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func newMB(name string) *mantlev1.MantleBackup {
	return &mantlev1.MantleBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "mb-namespace",
		},
	}
}

// newKubernetesClient returns a KubernetesClient whose MantleBackup deletions and
// expiration schedules are recorded to calls in the order they are made. Deleting
// a MantleBackup named in failingDeletes fails.
func newKubernetesClient(
	t *testing.T, calls *[]string, failingDeletes []string, objects ...client.Object,
) *infra.KubernetesClient {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, mantlev1.AddToScheme(scheme))

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(
				ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption,
			) error {
				*calls = append(*calls, "delete "+obj.GetName())
				for _, name := range failingDeletes {
					if obj.GetName() == name {
						return errors.New("delete error")
					}
				}

				return c.Delete(ctx, obj, opts...)
			},
		}).
		Build()

	return infra.NewKubernetesClient(fakeClient, infra.WithExpireQueue(func(evt event.GenericEvent) {
		*calls = append(*calls, "schedule-expire "+evt.Object.GetName())
	}))
}

func TestKubernetesClient_ApplyReconcilerOperations_InOrder(t *testing.T) {
	t.Parallel()

	// Arrange
	calls := []string{}
	k8sClient := newKubernetesClient(t, &calls, nil, newMB("mb-b"))
	operations := []domain.Operation{
		&domain.ScheduleExpireOperation{Backup: newMB("mb-a")},
		&domain.DeleteMantleBackupOperation{Backup: newMB("mb-b")},
		&domain.ScheduleExpireOperation{Backup: newMB("mb-c")},
	}

	// Act
	err := k8sClient.ApplyReconcilerOperations(t.Context(), operations)

	// Assert
	require.NoError(t, err)
	require.Equal(t, []string{"schedule-expire mb-a", "delete mb-b", "schedule-expire mb-c"}, calls)

	err = k8sClient.Get(t.Context(), types.NamespacedName{Namespace: "mb-namespace", Name: "mb-b"},
		&mantlev1.MantleBackup{})
	require.True(t, aerrors.IsNotFound(err))
}

func TestKubernetesClient_ApplyReconcilerOperations_StopsAtFailure(t *testing.T) {
	t.Parallel()

	// Arrange
	calls := []string{}
	k8sClient := newKubernetesClient(t, &calls, []string{"mb-b"}, newMB("mb-b"))
	operations := []domain.Operation{
		&domain.ScheduleExpireOperation{Backup: newMB("mb-a")},
		&domain.DeleteMantleBackupOperation{Backup: newMB("mb-b")},
		&domain.ScheduleExpireOperation{Backup: newMB("mb-c")},
	}

	// Act
	err := k8sClient.ApplyReconcilerOperations(t.Context(), operations)

	// Assert
	require.Error(t, err)
	require.Equal(t, []string{"schedule-expire mb-a", "delete mb-b"}, calls)
}

func TestKubernetesClient_ApplyReconcilerOperations_DeleteMissingMantleBackup(t *testing.T) {
	t.Parallel()

	// Arrange
	calls := []string{}
	k8sClient := newKubernetesClient(t, &calls, nil)
	operations := []domain.Operation{
		&domain.DeleteMantleBackupOperation{Backup: newMB("mb-a")},
	}

	// Act
	err := k8sClient.ApplyReconcilerOperations(t.Context(), operations)

	// Assert
	require.NoError(t, err)
	require.Equal(t, []string{"delete mb-a"}, calls)
}
