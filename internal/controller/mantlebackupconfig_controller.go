package controller

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/errors"
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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	MantleBackupConfigFinalizerName     = "mantlebackupconfig.mantle.cybozu.io/finalizer"
	MantleBackupConfigCronJobNamePrefix = "mbc-"
)

// MantleBackupConfigReconciler reconciles a MantleBackupConfig object
type MantleBackupConfigReconciler struct {
	Client               client.Client
	Scheme               *runtime.Scheme
	managedCephClusterID string
	overwriteMBCSchedule string
	role                 string
}

func NewMantleBackupConfigReconciler(
	cli client.Client,
	scheme *runtime.Scheme,
	managedCephClusterID string,
	overwriteMBCSchedule string,
	role string,
) *MantleBackupConfigReconciler {
	return &MantleBackupConfigReconciler{cli, scheme, managedCephClusterID, overwriteMBCSchedule, role}
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
	logger := log.FromContext(ctx)

	if r.role == RoleSecondary {
		return ctrl.Result{}, nil
	}

	// Get the CronJob info to be created or updated
	cronJobInfo, err := getCronJobInfo(ctx, r.Client)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("couldn't get cronjob info: %w", err)
	}

	// Get MantleBackupConfig.
	var mbc mantlev1.MantleBackupConfig
	if err := r.Client.Get(ctx, req.NamespacedName, &mbc); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("MantleBackupConfig not found", "error", err)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get MantleBackupConfig: %w", err)
	}

	// When the deletionTimestamp is set, remove the finalizer and finish reconciling.
	if !mbc.ObjectMeta.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&mbc, MantleBackupConfigFinalizerName) {
			// Delete the CronJob. If we failed to delete it because it's not found, ignore the error.
			logger.Info("start deleting cronjobs")
			if err := r.deleteCronJob(ctx, &mbc, cronJobInfo.namespace); err != nil && !errors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("failed to delete cronjob: %w", err)
			}

			controllerutil.RemoveFinalizer(&mbc, MantleBackupConfigFinalizerName)
			if err := r.Client.Update(ctx, &mbc); err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to remove mbc finalizer: %w", err)
			}
		}
		return ctrl.Result{}, nil
	}

	// Check if we're in charge of the given mbc.
	var pvc corev1.PersistentVolumeClaim
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: mbc.Namespace, Name: mbc.Spec.PVC}, &pvc); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get PVC: %s: %s: %w", mbc.Namespace, mbc.Spec.PVC, err)
	}
	clusterID, err := getCephClusterIDFromPVC(ctx, r.Client, &pvc)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get Ceph cluster ID: %s: %s: %w", mbc.Namespace, mbc.Spec.PVC, err)
	}
	if clusterID != r.managedCephClusterID {
		logger.Info("the target pvc is not managed by this controller")
		return ctrl.Result{}, nil
	}

	// Set the finalizer if it's not yet set.
	if !controllerutil.ContainsFinalizer(&mbc, MantleBackupConfigFinalizerName) {
		controllerutil.AddFinalizer(&mbc, MantleBackupConfigFinalizerName)
		if err := r.Client.Update(ctx, &mbc); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to add mbc finalizer: %w", err)
		}
	}

	// Create or update the CronJob
	if err := r.createOrUpdateCronJob(
		ctx,
		&mbc,
		cronJobInfo.namespace,
		cronJobInfo.serviceAccountName,
		cronJobInfo.image,
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to create or update cronjob: %w", err)
	}

	return ctrl.Result{}, nil
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

func (r *MantleBackupConfigReconciler) createOrUpdateCronJob(ctx context.Context, mbc *mantlev1.MantleBackupConfig, namespace, serviceAccountName, image string) error {
	logger := log.FromContext(ctx)
	cronJobName := getMBCCronJobName(mbc)

	cronJob := &batchv1.CronJob{}
	cronJob.SetName(cronJobName)
	cronJob.SetNamespace(namespace)

	op, err := ctrl.CreateOrUpdate(ctx, r.Client, cronJob, func() error {
		cronJob.Spec.Schedule = mbc.Spec.Schedule
		if r.overwriteMBCSchedule != "" {
			cronJob.Spec.Schedule = r.overwriteMBCSchedule
		}

		cronJob.Spec.Suspend = &mbc.Spec.Suspend
		cronJob.Spec.ConcurrencyPolicy = batchv1.ForbidConcurrent
		var startingDeadlineSeconds int64 = 3600
		cronJob.Spec.StartingDeadlineSeconds = &startingDeadlineSeconds
		var backoffLimit int32 = 10
		cronJob.Spec.JobTemplate.Spec.BackoffLimit = &backoffLimit

		podSpec := &cronJob.Spec.JobTemplate.Spec.Template.Spec
		podSpec.ServiceAccountName = serviceAccountName
		podSpec.RestartPolicy = corev1.RestartPolicyOnFailure

		if len(podSpec.Containers) == 0 {
			podSpec.Containers = append(podSpec.Containers, corev1.Container{})
		}
		container := &podSpec.Containers[0]
		container.Name = "backup"
		container.Image = image
		container.Command = []string{
			"/manager",
			"backup",
			"--name", mbc.GetName(),
			"--namespace", mbc.GetNamespace(),
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
		logger.Info(fmt.Sprintf("CronJob successfully created: %s", cronJobName))
	}

	return nil
}

func (r *MantleBackupConfigReconciler) deleteCronJob(ctx context.Context, mbc *mantlev1.MantleBackupConfig, namespace string) error {
	var cronJob batchv1.CronJob
	if err := r.Client.Get(
		ctx,
		types.NamespacedName{Name: getMBCCronJobName(mbc), Namespace: namespace},
		&cronJob,
	); err != nil {
		return err
	}

	uid := cronJob.GetUID()
	resourceVersion := cronJob.GetResourceVersion()
	return r.Client.Delete(
		ctx,
		&cronJob,
		&client.DeleteOptions{
			Preconditions: &metav1.Preconditions{
				UID:             &uid,
				ResourceVersion: &resourceVersion,
			},
		},
	)
}

func getMBCCronJobName(mbc *mantlev1.MantleBackupConfig) string {
	return MantleBackupConfigCronJobNamePrefix + string(mbc.ObjectMeta.UID)
}

var getRunningPod func(ctx context.Context, client client.Client) (*corev1.Pod, error) = getRunningPodImpl

func getRunningPodImpl(ctx context.Context, client client.Client) (*corev1.Pod, error) {
	name, ok := os.LookupEnv("POD_NAME")
	if !ok {
		return nil, fmt.Errorf("POD_NAME not found")
	}
	namespace, ok := os.LookupEnv("POD_NAMESPACE")
	if !ok {
		return nil, fmt.Errorf("POD_NAMESPACE not found")
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
		return nil, fmt.Errorf("failed to get running container")
	}
	namespace := runningPod.Namespace
	serviceAccountName := runningPod.Spec.ServiceAccountName
	image := runningPod.Spec.Containers[0].Image
	return &cronJobInfo{namespace, serviceAccountName, image}, nil
}
