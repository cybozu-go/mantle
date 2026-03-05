package usecase

import (
	"context"
	"fmt"

	"github.com/cybozu-go/mantle/internal/controller/domain"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// KubernetesClient is an interface for Kubernetes client operations used by reconcilers.
type KubernetesClient interface {
	client.Client
	ApplyReconcilerOperations(ctx context.Context, operations []domain.Operation) error
}

func getResource[
	T any,
	PT interface {
		*T
		client.Object
	},
](ctx context.Context, k8sClient client.Client, name string, namespace string) (*T, error) {
	obj := new(T)

	err := k8sClient.Get(ctx, types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, PT(obj))
	if err != nil {
		return nil, fmt.Errorf("failed to get %T %s/%s: %w", obj, namespace, name, err)
	}

	return obj, nil
}
