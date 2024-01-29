package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	backupv1 "github.com/cybozu-go/rbd-backup-system/api/v1"
)

// RBDPVCBackupReconciler reconciles a RBDPVCBackup object
type RBDPVCBackupReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

const (
	RBDPVCBackupFinalizerName = "rbdpvcbackup.backup.cybozu.com/finalizer"
)

//+kubebuilder:rbac:groups=backup.cybozu.com,resources=rbdpvcbackups,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=backup.cybozu.com,resources=rbdpvcbackups/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=backup.cybozu.com,resources=rbdpvcbackups/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the RBDPVCBackup object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.16.3/pkg/reconcile
func (r *RBDPVCBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var rpBackup backupv1.RBDPVCBackup
	err := r.Get(ctx, req.NamespacedName, &rpBackup)
	if errors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}
	if err != nil {
		logger.Error(err, "unable to get RBDPVCBackup", "name", req.NamespacedName)
		return ctrl.Result{}, err
	}

	if !rpBackup.ObjectMeta.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&rpBackup, RBDPVCBackupFinalizerName) {
			// TODO delete rbd snapshot.

			controllerutil.RemoveFinalizer(&rpBackup, RBDPVCBackupFinalizerName)
			err = r.Update(ctx, &rpBackup)
			if err != nil {
				return ctrl.Result{}, err
			}
		}

		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&rpBackup, RBDPVCBackupFinalizerName) {
		controllerutil.AddFinalizer(&rpBackup, RBDPVCBackupFinalizerName)
		err = r.Update(ctx, &rpBackup)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	if rpBackup.Status.Conditions != "" {
		return ctrl.Result{}, nil
	}

	rpBackup.Status.Conditions = backupv1.RBDPVCBackupConditionsCreating
	err = r.Status().Update(ctx, &rpBackup)
	if err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *RBDPVCBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&backupv1.RBDPVCBackup{}).
		Complete(r)
}
