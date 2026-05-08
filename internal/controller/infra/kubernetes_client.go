package infra

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/cybozu-go/mantle/internal/controller/usecase"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	aerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
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

		case *domain.CreateOrUpdateMBCCronJobOperation:
			logger.Info("creating or updating CronJob for MBC",
				"name", operation.CronJobName, "namespace", operation.CronJobNamespace)
			err = c.createOrUpdateMBCCronJob(ctx, operation)

		default:
			err = fmt.Errorf("unknown operation type: %T", i)
		}

		dispatchError = errors.Join(dispatchError, err)
	}

	return dispatchError
}

func (c *KubernetesClient) createOrUpdateMBCCronJob(ctx context.Context, op *domain.CreateOrUpdateMBCCronJobOperation) error {
	cronJob := &batchv1.CronJob{}
	cronJob.SetName(op.CronJobName)
	cronJob.SetNamespace(op.CronJobNamespace)

	result, err := ctrl.CreateOrUpdate(ctx, c.Client, cronJob, func() error {
		cronJob.Spec.Schedule = op.Schedule
		cronJob.Spec.Suspend = &op.Suspend
		cronJob.Spec.ConcurrencyPolicy = batchv1.ForbidConcurrent
		var startingDeadlineSeconds int64 = 3600
		cronJob.Spec.StartingDeadlineSeconds = &startingDeadlineSeconds
		var backoffLimit int32 = 10
		cronJob.Spec.JobTemplate.Spec.BackoffLimit = &backoffLimit

		podSpec := &cronJob.Spec.JobTemplate.Spec.Template.Spec
		podSpec.ServiceAccountName = op.ServiceAccountName
		podSpec.RestartPolicy = corev1.RestartPolicyOnFailure

		if len(podSpec.Containers) == 0 {
			podSpec.Containers = append(podSpec.Containers, corev1.Container{})
		}
		container := &podSpec.Containers[0]
		container.Name = "backup"
		container.Image = op.Image
		container.Command = []string{
			"/manager",
			"backup",
			"--name", op.MBCName,
			"--namespace", op.MBCNamespace,
		}
		container.ImagePullPolicy = corev1.PullIfNotPresent

		envName := "JOB_NAME"
		envIndex := slices.IndexFunc(container.Env, func(e corev1.EnvVar) bool {
			return e.Name == envName
		})
		if envIndex == -1 {
			container.Env = append(container.Env, corev1.EnvVar{Name: envName})
			envIndex = len(container.Env) - 1
		}
		env := &container.Env[envIndex]
		if env.ValueFrom == nil {
			env.ValueFrom = &corev1.EnvVarSource{}
		}
		if env.ValueFrom.FieldRef == nil {
			env.ValueFrom.FieldRef = &corev1.ObjectFieldSelector{}
		}
		env.ValueFrom.FieldRef.FieldPath = "metadata.labels['batch.kubernetes.io/job-name']"

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to create or update CronJob: %s: %w", op.CronJobName, err)
	}
	if result != controllerutil.OperationResultNone {
		log.FromContext(ctx).Info("CronJob successfully created or updated: " + op.CronJobName)
	}

	return nil
}
