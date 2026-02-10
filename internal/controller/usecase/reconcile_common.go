package usecase

import (
	"context"
	"fmt"

	"github.com/cybozu-go/mantle/internal/controller/domain"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type KubernetesClient interface {
	client.Client
	DispatchReconcilerEvents(ctx context.Context, events []domain.Event)
}

func getResource[
	T any,
	PT interface {
		*T
		client.Object
	},
](ctx context.Context, k8sClient client.Client, name string, namespace string) (PT, error) {
	obj := PT(new(T))
	if err := k8sClient.Get(ctx, types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}, obj); err != nil {
		return nil, fmt.Errorf("failed to get %T %s/%s: %w", obj, namespace, name, err)
	}

	return obj, nil
}
