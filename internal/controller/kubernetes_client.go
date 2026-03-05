package controller

import (
	"context"
	"errors"

	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/cybozu-go/mantle/internal/controller/usecase"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type kubernetesClient struct {
	client.Client
}

var _ usecase.KubernetesClient = (*kubernetesClient)(nil)

func (c *kubernetesClient) ApplyReconcilerOperations(ctx context.Context, operations []domain.Operation) error {
	var dispatchError error

	for _, i := range operations {
		var err error

		switch operation := i.(type) {
		case *domain.DeleteMBCCronJobOperation:
			err = c.Delete(ctx, operation.CronJob, &client.DeleteOptions{
				Preconditions: &metav1.Preconditions{
					UID:             &operation.CronJob.UID,
					ResourceVersion: &operation.CronJob.ResourceVersion,
				},
			})
		}

		dispatchError = errors.Join(dispatchError, err)
	}

	return dispatchError
}
