package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	backupv1 "github.com/cybozu-go/mantle/api/v1"
)

// MantleBackupReconciler reconciles a MantleBackup object
type MantleBackupReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

type Snapshot struct {
	Id        int    `json:"id,omitempty"`
	Name      string `json:"name,omitempty"`
	Size      int    `json:"size,omitempty"`
	Protected bool   `json:"protected,string,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
}

const (
	MantleBackupFinalizerName = "mantlebackup.mantle.cybozu.io/finalizer"
)

// NewMantleBackupReconciler returns NodeReconciler.
func NewMantleBackupReconciler(client client.Client, scheme *runtime.Scheme) *MantleBackupReconciler {
	return &MantleBackupReconciler{
		Client: client,
		Scheme: scheme,
	}
}

func executeCommandImpl(command []string, input io.Reader) ([]byte, error) {
	cmd := exec.Command(command[0], command[1:]...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	defer func() {
		err := stdout.Close()
		if err != nil {
			logger.Error("failed to stdout.Close", "error", err)
		}
	}()

	if input != nil {
		stdin, err := cmd.StdinPipe()
		if err != nil {
			return nil, err
		}
		go func() {
			defer func() {
				err := stdin.Close()
				if err != nil {
					logger.Error("failed to stdin.Close", "error", err)
				}
			}()
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

var executeCommand = executeCommandImpl

func (r *MantleBackupReconciler) updateStatus(ctx context.Context, backup *backupv1.MantleBackup, condition metav1.Condition) error {
	meta.SetStatusCondition(&backup.Status.Conditions, condition)
	err := r.Status().Update(ctx, backup)
	if err != nil {
		logger.Error("failed to update status", "status", backup.Status, "error", err)
		return err
	}
	return nil
}

func (r *MantleBackupReconciler) createRBDSnapshot(ctx context.Context, poolName, imageName string, backup *backupv1.MantleBackup) (ctrl.Result, error) {
	command := []string{"rbd", "snap", "create", poolName + "/" + imageName + "@" + backup.Name}
	_, err := executeCommand(command, nil)
	if err != nil {
		command = []string{"rbd", "snap", "ls", poolName + "/" + imageName, "--format=json"}
		out, err := executeCommand(command, nil)
		if err != nil {
			logger.Info("failed to run `rbd snap ls`", "poolName", poolName, "imageName", imageName, "error", err)
			err2 := r.updateStatus(ctx, backup, metav1.Condition{Type: backupv1.BackupConditionReadyToUse, Status: metav1.ConditionFalse, Reason: backupv1.BackupReasonFailedToCreateBackup})
			if err2 != nil {
				return ctrl.Result{}, err2
			}
			return ctrl.Result{Requeue: true}, nil
		}
		var snapshots []Snapshot
		err = json.Unmarshal(out, &snapshots)
		if err != nil {
			logger.Error("failed to unmarshal json", "json", out, "error", err)
			err2 := r.updateStatus(ctx, backup, metav1.Condition{Type: backupv1.BackupConditionReadyToUse, Status: metav1.ConditionFalse, Reason: backupv1.BackupReasonFailedToCreateBackup})
			if err2 != nil {
				return ctrl.Result{}, err2
			}
			return ctrl.Result{Requeue: true}, err
		}
		existSnapshot := false
		for _, s := range snapshots {
			if s.Name == backup.Name {
				existSnapshot = true
				break
			}
		}
		if !existSnapshot {
			logger.Info("snapshot does not exists", "snapshotName", backup.Name)
			err2 := r.updateStatus(ctx, backup, metav1.Condition{Type: backupv1.BackupConditionReadyToUse, Status: metav1.ConditionFalse, Reason: backupv1.BackupReasonFailedToCreateBackup})
			if err2 != nil {
				return ctrl.Result{}, err2
			}
			return ctrl.Result{Requeue: true}, nil
		}
	}
	return ctrl.Result{}, nil
}

//+kubebuilder:rbac:groups=mantle.cybozu.io,resources=mantlebackups,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=mantle.cybozu.io,resources=mantlebackups/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=mantle.cybozu.io,resources=mantlebackups/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch
//+kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the MantleBackup object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.16.3/pkg/reconcile
func (r *MantleBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var backup backupv1.MantleBackup
	err := r.Get(ctx, req.NamespacedName, &backup)
	if errors.IsNotFound(err) {
		logger.Info("MantleBackup is not found", "name", backup.Name, "error", err)
		return ctrl.Result{}, nil
	}
	if err != nil {
		logger.Error("failed to get MantleBackup", "name", req.NamespacedName, "error", err)
		return ctrl.Result{}, err
	}

	pvcNamespace := backup.Namespace
	pvcName := backup.Spec.PVC
	var pvc corev1.PersistentVolumeClaim
	err = r.Get(ctx, types.NamespacedName{Namespace: pvcNamespace, Name: pvcName}, &pvc)
	if err != nil {
		logger.Error("failed to get PVC", "namespace", pvcNamespace, "name", pvcName, "error", err)
		err2 := r.updateStatus(ctx, &backup, metav1.Condition{Type: backupv1.BackupConditionReadyToUse, Status: metav1.ConditionFalse, Reason: backupv1.BackupReasonFailedToCreateBackup})
		if err2 != nil {
			return ctrl.Result{}, err2
		}
		if errors.IsNotFound(err) {
			if !backup.ObjectMeta.DeletionTimestamp.IsZero() {
				if controllerutil.ContainsFinalizer(&backup, MantleBackupFinalizerName) {
					controllerutil.RemoveFinalizer(&backup, MantleBackupFinalizerName)
					err = r.Update(ctx, &backup)
					if err != nil {
						logger.Error("failed to remove finalizer", "finalizer", MantleBackupFinalizerName, "error", err)
						return ctrl.Result{}, err
					}
				}

				return ctrl.Result{}, nil
			}
		}
		return ctrl.Result{}, err
	}

	if pvc.Status.Phase != corev1.ClaimBound {
		err := r.updateStatus(ctx, &backup, metav1.Condition{Type: backupv1.BackupConditionReadyToUse, Status: metav1.ConditionFalse, Reason: backupv1.BackupReasonFailedToCreateBackup})
		if err != nil {
			return ctrl.Result{}, err
		}

		if pvc.Status.Phase == corev1.ClaimPending {
			logger.Info("waiting for PVC bound.")
			return ctrl.Result{Requeue: true}, nil
		} else {
			logger.Error("PVC phase is neither bound nor pending", "status.phase", pvc.Status.Phase)
			return ctrl.Result{}, fmt.Errorf("PVC phase is neither bound nor pending (status.phase: %s)", pvc.Status.Phase)
		}
	}

	pvName := pvc.Spec.VolumeName
	var pv corev1.PersistentVolume
	err = r.Get(ctx, types.NamespacedName{Namespace: req.NamespacedName.Namespace, Name: pvName}, &pv)
	if err != nil {
		logger.Error("failed to get PV", "namespace", req.NamespacedName.Namespace, "name", pvName, "error", err)
		err2 := r.updateStatus(ctx, &backup, metav1.Condition{Type: backupv1.BackupConditionReadyToUse, Status: metav1.ConditionFalse, Reason: backupv1.BackupReasonFailedToCreateBackup})
		if err2 != nil {
			return ctrl.Result{}, err2
		}

		return ctrl.Result{}, err
	}

	imageName, ok := pv.Spec.CSI.VolumeAttributes["imageName"]
	if !ok {
		return ctrl.Result{}, fmt.Errorf("failed to get imageName from PV")
	}
	poolName, ok := pv.Spec.CSI.VolumeAttributes["pool"]
	if !ok {
		return ctrl.Result{}, fmt.Errorf("failed to get pool from PV")
	}

	if !backup.ObjectMeta.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&backup, MantleBackupFinalizerName) {
			command := []string{"rbd", "snap", "rm", poolName + "/" + imageName + "@" + backup.Name}
			_, err = executeCommand(command, nil)
			if err != nil {
				if exitError, ok := err.(*exec.ExitError); ok {
					waitStatus := exitError.Sys().(syscall.WaitStatus)
					exitCode := waitStatus.ExitStatus()
					if exitCode != int(syscall.ENOENT) {
						logger.Error("failed to remove rbd snapshot", "poolName", poolName, "imageName", imageName, "snapshotName", backup.Name, "exitCode", exitCode, "error", err)
						return ctrl.Result{Requeue: true}, nil
					}
				}
				logger.Info("rbd snapshot has already been removed", "poolName", poolName, "imageName", imageName, "snapshotName", backup.Name, "error", err)
			}

			controllerutil.RemoveFinalizer(&backup, MantleBackupFinalizerName)
			err = r.Update(ctx, &backup)
			if err != nil {
				logger.Error("failed to remove finalizer", "finalizer", MantleBackupFinalizerName, "error", err)
				return ctrl.Result{}, err
			}
		}

		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&backup, MantleBackupFinalizerName) {
		controllerutil.AddFinalizer(&backup, MantleBackupFinalizerName)
		err = r.Update(ctx, &backup)
		if err != nil {
			logger.Error("failed to add finalizer", "finalizer", MantleBackupFinalizerName, "error", err)
			return ctrl.Result{}, err
		}
		err := r.updateStatus(ctx, &backup, metav1.Condition{Type: backupv1.BackupConditionReadyToUse, Status: metav1.ConditionFalse, Reason: backupv1.BackupReasonNone})
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	if meta.FindStatusCondition(backup.Status.Conditions, backupv1.BackupConditionReadyToUse).Status == metav1.ConditionTrue {
		return ctrl.Result{}, nil
	}

	result, err := r.createRBDSnapshot(ctx, poolName, imageName, &backup)
	if err != nil {
		return result, err
	}

	backup.Status.CreatedAt = metav1.NewTime(time.Now())
	err = r.updateStatus(ctx, &backup, metav1.Condition{Type: backupv1.BackupConditionReadyToUse, Status: metav1.ConditionTrue, Reason: backupv1.BackupReasonNone})
	if err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MantleBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&backupv1.MantleBackup{}).
		Complete(r)
}
