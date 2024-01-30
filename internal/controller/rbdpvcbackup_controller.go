package controller

import (
	"context"
	"io"
	"os/exec"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

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

func executeCommand(command []string, input io.Reader) ([]byte, error) {
	cmd := exec.Command(command[0], command[1:]...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	defer stdout.Close()

	if input != nil {
		stdin, err := cmd.StdinPipe()
		if err != nil {
			return nil, err
		}
		go func() {
			defer stdin.Close()
			if _, err = io.Copy(stdin, input); err != nil {
				logger.Error("failed to io.Copy", "error", err)
			}
		}()
	}

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	r, err := io.ReadAll(stdout)
	if err != nil {
		return r, err
	}

	if err := cmd.Wait(); err != nil {
		return r, err
	}

	return r, nil
}

//+kubebuilder:rbac:groups=backup.cybozu.com,resources=rbdpvcbackups,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=backup.cybozu.com,resources=rbdpvcbackups/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=backup.cybozu.com,resources=rbdpvcbackups/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch
//+kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch

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
	var rpBackup backupv1.RBDPVCBackup
	err := r.Get(ctx, req.NamespacedName, &rpBackup)
	if errors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}
	if err != nil {
		logger.Error("unable to get RBDPVCBackup", "name", req.NamespacedName, "error", err)
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
	logger.Info("sat: foo!")
	command := []string{"ceph", "-s"}
	out, err := executeCommand(command, nil)
	if err != nil {
		logger.Error("unable to run ceph -s", "error", err)
	}
	logger.Info(string(out))

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
