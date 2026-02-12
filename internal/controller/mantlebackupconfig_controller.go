package controller

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/cybozu-go/mantle/internal/controller/usecase"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

// MantleBackupConfigReconciler reconciles a MantleBackupConfig object.
type MantleBackupConfigReconciler struct {
	Client               client.Client
	Scheme               *runtime.Scheme
	role                 string
	managedCephClusterID string
	overwriteMBCSchedule string
	cronJobInfo          *cronJobInfo
	uc                   *usecase.ReconcileMBCInPrimary
}

func NewMantleBackupConfigReconciler(
	cli client.Client,
	scheme *runtime.Scheme,
	managedCephClusterID string,
	overwriteMBCSchedule string,
	role string,
) *MantleBackupConfigReconciler {
	return &MantleBackupConfigReconciler{
		Client:               cli,
		Scheme:               scheme,
		role:                 role,
		managedCephClusterID: managedCephClusterID,
		overwriteMBCSchedule: overwriteMBCSchedule,
		cronJobInfo:          nil,
		uc:                   nil,
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

	if r.uc == nil {
		// Get the CronJob info to be created or updated
		cronJobInfo, err := getCronJobInfo(ctx, r.Client)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("couldn't get cronjob info: %w", err)
		}
		r.cronJobInfo = cronJobInfo
		r.uc = usecase.NewReconcileMBCInPrimary(
			domain.NewMBCPrimaryReconciler(
				r.overwriteMBCSchedule,
				r.managedCephClusterID,
				r.cronJobInfo.serviceAccountName,
				r.cronJobInfo.image,
				r.cronJobInfo.namespace,
			),
			&kubernetesClient{r.Client},
			r.cronJobInfo.namespace,
		)
	}

	if err := r.uc.Run(ctx, req.Name, req.Namespace); err != nil {
		return ctrl.Result{}, fmt.Errorf("usecase ReconcileMBCInPrimary failed: %w", err)
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
				uid, found := strings.CutPrefix(name, domain.MantleBackupConfigCronJobNamePrefix)
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

type kubernetesClient struct {
	client.Client
}

var _ usecase.KubernetesClient = (*kubernetesClient)(nil)

func (c *kubernetesClient) DispatchReconcilerEvents(ctx context.Context, events []domain.Event) error {
	var dispatchError error

	for _, i := range events {
		var err error

		switch event := i.(type) {
		case *domain.CreateOrUpdateMBCCronJobEvent:
			if event.CronJob.CreationTimestamp.IsZero() { // Create
				err = c.Create(ctx, event.CronJob)
			} else { // Update
				err = c.Update(ctx, event.CronJob)
			}

		case *domain.DeleteMBCCronJobEvent:
			err = c.Delete(ctx, event.CronJob, &client.DeleteOptions{
				Preconditions: &metav1.Preconditions{
					UID:             &event.CronJob.UID,
					ResourceVersion: &event.CronJob.ResourceVersion,
				},
			})
		}

		dispatchError = errors.Join(dispatchError, err)
	}

	return dispatchError
}
