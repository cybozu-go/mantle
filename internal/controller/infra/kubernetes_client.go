package infra

import (
	"context"
	"errors"
	"fmt"

	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/cybozu-go/mantle/internal/controller/usecase"
	aerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type KubernetesClient struct {
	client.Client
}

var _ usecase.KubernetesClient = (*KubernetesClient)(nil)

func NewKubernetesClient(c client.Client) *KubernetesClient {
	return &KubernetesClient{c}
}

func (c *KubernetesClient) ApplyReconcilerOperations(ctx context.Context, operations []domain.Operation) error {
	logger := log.FromContext(ctx)
	var dispatchError error

	for _, i := range operations {
		var err error

		switch operation := i.(type) {
		case *domain.DeleteMBCCronJobOperation:
			logger.Info("deleting CronJob for MBC",
				"name", operation.CronJob.Name, "namespace", operation.CronJob.Namespace)
			err = c.Delete(ctx, operation.CronJob, &client.DeleteOptions{
				Preconditions: &metav1.Preconditions{
					UID:             &operation.CronJob.UID,
					ResourceVersion: &operation.CronJob.ResourceVersion,
				},
			})
			if aerrors.IsNotFound(err) {
				err = nil
			}

		default:
			err = fmt.Errorf("unknown operation type: %T", i)
		}

		dispatchError = errors.Join(dispatchError, err)
	}

	return dispatchError
}
