package controller

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	aerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	MantleBackupConfigFinalizerName              = "mantlebackupconfig.mantle.cybozu.io/finalizer"
	MantleBackupConfigAnnotationManagedClusterID = "mantlebackupconfig.mantle.cybozu.io/managed-cluster-id"
	MantleBackupConfigCronJobNamePrefix          = "mbc-"
)

// MantleBackupConfigReconciler reconciles a MantleBackupConfig object.
type MantleBackupConfigReconciler struct {
	Client      client.Client
	Scheme      *runtime.Scheme
	logic       *MantleBackupConfigPrimaryReconcileLogic
	role        string
	cronJobInfo *cronJobInfo
}

func NewMantleBackupConfigReconciler(
	cli client.Client,
	scheme *runtime.Scheme,
	managedCephClusterID string,
	overwriteMBCSchedule string,
	role string,
) *MantleBackupConfigReconciler {
	return &MantleBackupConfigReconciler{
		Client: cli,
		Scheme: scheme,
		logic: NewMantleBackupConfigPrimaryReconcileLogic(
			managedCephClusterID,
			overwriteMBCSchedule,
		),
		role:        role,
		cronJobInfo: nil,
	}
}

func (r *MantleBackupConfigReconciler) dispatchEvent(ctx context.Context, event Event) error {
	switch e := event.(type) {
	case CreateOrUpdateMantleBackupConfigCronJobEvent:
		err := r.createOrUpdateCronJob(
			ctx,
			e.mbcName,
			e.mbcNamespace,
			e.cronJobName,
			e.cronJobNamespace,
			e.schedule,
			e.suspend,
		)

		return err

	case UpdateMantleBackupConfigEvent:
		return r.Client.Update(ctx, e.MBC)

	case DeleteMBCCronJobEvent:
		err := r.Client.Delete(
			ctx,
			&batchv1.CronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      e.name,
					Namespace: e.namespace,
				},
			},
			&client.DeleteOptions{
				Preconditions: e.preconditions,
			},
		)
		if aerrors.IsNotFound(err) {
			return nil
		}

		return err

	case DeleteMBCFinalizerEvent:
		mbc := mantlev1.MantleBackupConfig{}
		if err := r.Client.Get(ctx, types.NamespacedName{
			Name:      e.mbcName,
			Namespace: e.mbcNamespace,
		}, &mbc); err != nil {
			return fmt.Errorf("failed to get mbc to remove finalizer: %w", err)
		}

		controllerutil.RemoveFinalizer(&mbc, MantleBackupConfigFinalizerName)
		if err := r.Client.Update(ctx, &mbc); err != nil {
			return fmt.Errorf("failed to remove mbc finalizer: %w", err)
		}

		return nil

	default:
		return fmt.Errorf("unknown event type: %T", event)
	}
}

//+kubebuilder:rbac:groups=mantle.cybozu.io,resources=mantlebackupconfigs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=mantle.cybozu.io,resources=mantlebackupconfigs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=mantle.cybozu.io,resources=mantlebackupconfigs/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
//+kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the MantleBackupConfig object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.16.3/pkg/reconcile
func (r *MantleBackupConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if r.role == RoleSecondary {
		return ctrl.Result{}, nil
	}

	// Get the CronJob info to be created or updated
	cronJobInfo, err := getCronJobInfo(ctx, r.Client)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("couldn't get cronjob info: %w", err)
	}
	r.logic.SetRunningNamespace(cronJobInfo.namespace)
	r.cronJobInfo = cronJobInfo

	in, err := r.getNecessaryK8sObjects(ctx, req.NamespacedName)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get necessary k8s objects: %w", err)
	}
	if in == nil {
		// The MantleBackupConfig is deleted.
		return ctrl.Result{}, nil
	}

	r.logic.events = []Event{}
	ctrlResult, err := r.logic.Reconcile(in.mbc, in.mbcPVC, in.mbcPVCSC, in.cronJob)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("primary reconcile logic failed: %w", err)
	}

	errs := []error{}
	for _, event := range r.logic.events {
		errs = append(errs, r.dispatchEvent(ctx, event))
	}
	err = errors.Join(errs...)

	return ctrlResult, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *MantleBackupConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// We want to create or update a CronJob for a MantleBackupConfig by its
	// reconciliation whenever the CronJob is edited or deleted by someone. To
	// do this, we need to watch the CronJobs, and when we detect their changes
	// or deletions, we'll extract the MantleBackupConfig's UID from the
	// modified CronJob's name and search for the MantleBackupConfig using it.
	// For the UID lookup, we need to set up an additional indexer for
	// .metadata.uid, which is configured here.
	// cf. https://book.kubebuilder.io/reference/watching-resources/externally-managed
	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&mantlev1.MantleBackupConfig{},
		".metadata.uid",
		func(rawObj client.Object) []string {
			mbc := rawObj.(*mantlev1.MantleBackupConfig)
			if mbc.UID == "" {
				return nil
			}

			return []string{string(mbc.UID)}
		}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&mantlev1.MantleBackupConfig{}).
		Watches(
			&batchv1.CronJob{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, cronJob client.Object) []reconcile.Request {
				// Extract the MantleBackupConfig's UID, look it up, and enqueue its reconciliation.
				name := cronJob.GetName()
				uid, found := strings.CutPrefix(name, MantleBackupConfigCronJobNamePrefix)
				if !found {
					return []reconcile.Request{}
				}
				mbcs := mantlev1.MantleBackupConfigList{}
				if err := r.Client.List(ctx, &mbcs, &client.ListOptions{
					FieldSelector: fields.OneTermEqualSelector(".metadata.uid", uid),
				}); err != nil {
					mgr.GetLogger().Info("List of MantleBackup failed", "error", err)

					return []reconcile.Request{}
				}
				if len(mbcs.Items) != 1 {
					return []reconcile.Request{}
				}

				return []reconcile.Request{
					{
						NamespacedName: types.NamespacedName{
							Name:      mbcs.Items[0].GetName(),
							Namespace: mbcs.Items[0].GetNamespace(),
						},
					},
				}
			}),
		).
		Complete(r)
}

func (r *MantleBackupConfigReconciler) createOrUpdateCronJob(
	ctx context.Context,
	mbcName,
	mbcNamespace,
	cronJobName,
	cronJobNamespace,
	schedule string,
	suspend bool,
) error {
	logger := log.FromContext(ctx)

	cronJob := &batchv1.CronJob{}
	cronJob.SetName(cronJobName)
	cronJob.SetNamespace(cronJobNamespace)

	op, err := ctrl.CreateOrUpdate(ctx, r.Client, cronJob, func() error {
		cronJob.Spec.Schedule = schedule
		cronJob.Spec.Suspend = &suspend
		cronJob.Spec.ConcurrencyPolicy = batchv1.ForbidConcurrent
		var startingDeadlineSeconds int64 = 3600
		cronJob.Spec.StartingDeadlineSeconds = &startingDeadlineSeconds
		var backoffLimit int32 = 10
		cronJob.Spec.JobTemplate.Spec.BackoffLimit = &backoffLimit

		podSpec := &cronJob.Spec.JobTemplate.Spec.Template.Spec
		podSpec.ServiceAccountName = r.cronJobInfo.serviceAccountName
		podSpec.RestartPolicy = corev1.RestartPolicyOnFailure

		if len(podSpec.Containers) == 0 {
			podSpec.Containers = append(podSpec.Containers, corev1.Container{})
		}
		container := &podSpec.Containers[0]
		container.Name = "backup"
		container.Image = r.cronJobInfo.image
		container.Command = []string{
			"/manager",
			"backup",
			"--name", mbcName,
			"--namespace", mbcNamespace,
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
		return fmt.Errorf("failed to create CronJob: %s: %w", cronJobName, err)
	}
	if op != controllerutil.OperationResultNone {
		logger.Info("CronJob successfully created: " + cronJobName)
	}

	return nil
}

func getMBCCronJobName(mbc *mantlev1.MantleBackupConfig) string {
	return MantleBackupConfigCronJobNamePrefix + string(mbc.UID)
}

var getRunningPod func(ctx context.Context, client client.Client) (*corev1.Pod, error) = getRunningPodImpl

func getRunningPodImpl(ctx context.Context, client client.Client) (*corev1.Pod, error) {
	name, ok := os.LookupEnv("POD_NAME")
	if !ok {
		return nil, errors.New("POD_NAME not found")
	}
	namespace, ok := os.LookupEnv("POD_NAMESPACE")
	if !ok {
		return nil, errors.New("POD_NAMESPACE not found")
	}
	var pod corev1.Pod
	if err := client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &pod); err != nil {
		return nil, fmt.Errorf("failed to get pod: %w", err)
	}

	return &pod, nil
}

type cronJobInfo struct {
	namespace, serviceAccountName, image string
}

func getCronJobInfo(ctx context.Context, client client.Client) (*cronJobInfo, error) {
	runningPod, err := getRunningPod(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("failed to get running pod: %w", err)
	}
	if len(runningPod.Spec.Containers) == 0 {
		return nil, errors.New("failed to get running container")
	}
	namespace := runningPod.Namespace
	serviceAccountName := runningPod.Spec.ServiceAccountName
	image := runningPod.Spec.Containers[0].Image

	return &cronJobInfo{namespace, serviceAccountName, image}, nil
}

func getK8sObject[T any, PT interface {
	*T
	client.Object
}](ctx context.Context, c client.Client, namespacedName types.NamespacedName) (PT, error) {
	obj := PT(new(T))

	err := c.Get(ctx, namespacedName, obj)
	if err != nil {
		if aerrors.IsNotFound(err) {
			return nil, nil
		}

		return nil, err
	}

	return obj, nil
}

type mbcLogicInput struct {
	mbc      *mantlev1.MantleBackupConfig
	mbcPVC   *corev1.PersistentVolumeClaim
	mbcPVCSC *storagev1.StorageClass
	cronJob  *batchv1.CronJob
}

func (r *MantleBackupConfigReconciler) getNecessaryK8sObjects(ctx context.Context, mbcNamespacedName types.NamespacedName) (*mbcLogicInput, error) {
	mbc, err := getK8sObject[mantlev1.MantleBackupConfig](ctx, r.Client, mbcNamespacedName)
	if err != nil {
		return nil, fmt.Errorf("failed to get MantleBackupConfig: %w", err)
	}
	if mbc == nil {
		return nil, nil
	}

	mbcPVC, err := getK8sObject[corev1.PersistentVolumeClaim](ctx, r.Client,
		types.NamespacedName{Name: mbc.Spec.PVC, Namespace: mbc.Namespace})
	if err != nil {
		return nil, fmt.Errorf("failed to get PVC: %w", err)
	}

	var mbcPVCSC *storagev1.StorageClass
	if mbcPVC != nil && mbcPVC.Spec.StorageClassName != nil {
		mbcPVCSC, err = getK8sObject[storagev1.StorageClass](ctx, r.Client,
			types.NamespacedName{Name: *mbcPVC.Spec.StorageClassName})
		if err != nil {
			return nil, fmt.Errorf("failed to get StorageClass: %w", err)
		}
	}

	cronJob, err := getK8sObject[batchv1.CronJob](ctx, r.Client,
		types.NamespacedName{
			Name:      getMBCCronJobName(mbc),
			Namespace: r.logic.runningNamespace,
		})
	if err != nil {
		return nil, fmt.Errorf("failed to get CronJob: %w", err)
	}

	return &mbcLogicInput{
		mbc:      mbc,
		mbcPVC:   mbcPVC,
		mbcPVCSC: mbcPVCSC,
		cronJob:  cronJob,
	}, nil
}
