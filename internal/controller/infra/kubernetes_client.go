package infra

import (
	"context"
	"errors"
	"fmt"
	"slices"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/cybozu-go/mantle/internal/controller/usecase"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	aerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var errExpireQueueNotConfigured = errors.New("expire queue is not configured")

type KubernetesClient struct {
	client.Client
	enqueueExpire func(event.GenericEvent)
}

var _ usecase.KubernetesClient = (*KubernetesClient)(nil)

// KubernetesClientOption configures a KubernetesClient.
type KubernetesClientOption func(*KubernetesClient)

// WithExpireQueue sets the function that ScheduleExpireOperation uses to enqueue
// a MantleBackup to be reconciled when it expires.
func WithExpireQueue(enqueue func(event.GenericEvent)) KubernetesClientOption {
	return func(c *KubernetesClient) {
		c.enqueueExpire = enqueue
	}
}

func NewKubernetesClient(c client.Client, opts ...KubernetesClientOption) *KubernetesClient {
	k8sClient := &KubernetesClient{Client: c}
	for _, opt := range opts {
		opt(k8sClient)
	}

	return k8sClient
}

func (c *KubernetesClient) GetCephClusterIDFromPVC(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (string, error) {
	return GetCephClusterIDFromPVC(ctx, c.Client, pvc)
}

// ApplyReconcilerOperations executes the operations in the given order. It stops at
// the first failure and doesn't execute the remaining operations, because a later
// operation may depend on the success of an earlier one.
func (c *KubernetesClient) ApplyReconcilerOperations(ctx context.Context, operations []domain.Operation) error {
	logger := log.FromContext(ctx)

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

		case *domain.DeleteMantleBackupOperation:
			err = c.deleteMantleBackup(ctx, operation.Backup)

		case *domain.ScheduleExpireOperation:
			err = c.scheduleExpire(operation.Backup)

		default:
			err = fmt.Errorf("unknown operation type: %T", i)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (c *KubernetesClient) deleteMantleBackup(ctx context.Context, backup *mantlev1.MantleBackup) error {
	err := c.Delete(ctx, backup)
	if err != nil && !aerrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete MantleBackup: %s/%s: %w", backup.Namespace, backup.Name, err)
	}

	return nil
}

func (c *KubernetesClient) scheduleExpire(backup *mantlev1.MantleBackup) error {
	if c.enqueueExpire == nil {
		return errExpireQueueNotConfigured
	}

	c.enqueueExpire(event.GenericEvent{Object: backup})

	return nil
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
