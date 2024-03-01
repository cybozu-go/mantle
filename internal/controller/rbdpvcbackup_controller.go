package controller

import (
	"context"
	"encoding/json"
	"io"
	"os/exec"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	backupv1 "github.com/cybozu-go/rbd-backup-system/api/v1"
)

// RBDPVCBackupReconciler reconciles a RBDPVCBackup object
type RBDPVCBackupReconciler struct {
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
	RBDPVCBackupFinalizerName = "rbdpvcbackup.backup.cybozu.com/finalizer"
)

var (
	rbdPVCBackupGVK = schema.GroupVersionKind{Group: "backup.cybozu.com", Version: "v1", Kind: "RBDPVCBackup"}
)

// NewRBDPVCBackupReconciler returns NodeReconciler.
func NewRBDPVCBackupReconciler(client client.Client, scheme *runtime.Scheme) *RBDPVCBackupReconciler {
	return &RBDPVCBackupReconciler{
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

var executeCommand = executeCommandImpl

func (r *RBDPVCBackupReconciler) updateConditions(ctx context.Context, backup *backupv1.RBDPVCBackup, conditions string) error {
	backup.Status.Conditions = conditions
	err := r.Status().Update(ctx, backup)
	if err != nil {
		logger.Error("failed to update status", "conditions", backup.Status.Conditions, "error", err)
		return err
	}
	return nil
}

//+kubebuilder:rbac:groups=backup.cybozu.com,resources=rbdpvcbackups,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=backup.cybozu.com,resources=rbdpvcbackups/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=backup.cybozu.com,resources=rbdpvcbackups/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch
//+kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch

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
	var backup backupv1.RBDPVCBackup
	err := r.Get(ctx, req.NamespacedName, &backup)
	if errors.IsNotFound(err) {
		logger.Info("RBDPVCBackup is not found", "name", backup.Name, "error", err)
		return ctrl.Result{}, nil
	}
	if err != nil {
		logger.Error("failed to get RBDPVCBackup", "name", req.NamespacedName, "error", err)
		return ctrl.Result{}, err
	}

	pvcNamespace := backup.Namespace
	pvcName := backup.Spec.PVC
	var pvc corev1.PersistentVolumeClaim
	err = r.Get(ctx, types.NamespacedName{Namespace: pvcNamespace, Name: pvcName}, &pvc)
	if err != nil {
		logger.Error("failed to get PVC", "namespace", pvcNamespace, "name", pvcName, "error", err)
		err2 := r.updateConditions(ctx, &backup, backupv1.RBDPVCBackupConditionsFailed)
		if err2 != nil {
			return ctrl.Result{}, err2
		}
		return reconcile.Result{}, err
	}
	for pvc.Status.Phase == "Pending" {
		logger.Info("waiting for PVC bound.")
		time.Sleep(1 * time.Second)

		err = r.Get(ctx, types.NamespacedName{Namespace: pvcNamespace, Name: pvcName}, &pvc)
		if err != nil {
			logger.Error("failed to get PVC", "namespace", pvcNamespace, "name", pvcName, "error", err)
			err2 := r.updateConditions(ctx, &backup, backupv1.RBDPVCBackupConditionsFailed)
			if err2 != nil {
				return ctrl.Result{}, err2
			}
			return reconcile.Result{}, err
		}
	}

	pvName := pvc.Spec.VolumeName
	var pv corev1.PersistentVolume
	err = r.Get(ctx, types.NamespacedName{Namespace: req.NamespacedName.Namespace, Name: pvName}, &pv)
	if err != nil {
		logger.Error("failed to get PV", "namespace", req.NamespacedName.Namespace, "name", pvName, "error", err)
		err2 := r.updateConditions(ctx, &backup, backupv1.RBDPVCBackupConditionsFailed)
		if err2 != nil {
			return ctrl.Result{}, err2
		}
		return reconcile.Result{}, err
	}

	imageName := pv.Spec.CSI.VolumeAttributes["imageName"]
	poolName := pv.Spec.CSI.VolumeAttributes["pool"]

	if !backup.ObjectMeta.DeletionTimestamp.IsZero() {
		if backup.Status.Conditions != backupv1.RBDPVCBackupConditionsDeleting {
			err = r.updateConditions(ctx, &backup, backupv1.RBDPVCBackupConditionsDeleting)
			if err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{Requeue: true}, nil
		}

		if controllerutil.ContainsFinalizer(&backup, RBDPVCBackupFinalizerName) {
			command := []string{"rbd", "snap", "rm", poolName + "/" + imageName + "@" + backup.Name}
			_, err = executeCommand(command, nil)
			if err != nil {
				if exitError, ok := err.(*exec.ExitError); ok {
					waitStatus := exitError.Sys().(syscall.WaitStatus)
					exitCode := waitStatus.ExitStatus()
					if exitCode != int(syscall.ENOENT) {
						logger.Error("failed to remove rbd snapshot", "poolName", poolName, "imageName", imageName, "snapshotName", backup.Name, "exitCode", exitCode, "error", err)
						err2 := r.updateConditions(ctx, &backup, backupv1.RBDPVCBackupConditionsFailed)
						if err2 != nil {
							return ctrl.Result{}, err2
						}
						return ctrl.Result{Requeue: true}, nil
					}
				}
				logger.Info("rbd snapshot has already been removed", "poolName", poolName, "imageName", imageName, "snapshotName", backup.Name, "error", err)
			}

			controllerutil.RemoveFinalizer(&backup, RBDPVCBackupFinalizerName)
			err = r.Update(ctx, &backup)
			if err != nil {
				logger.Error("failed to remove finalizer", "finalizer", RBDPVCBackupFinalizerName, "error", err)
				return ctrl.Result{}, err
			}
		}

		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&backup, RBDPVCBackupFinalizerName) {
		controllerutil.AddFinalizer(&backup, RBDPVCBackupFinalizerName)
		logger.Info("set finalizer\n")
		err = r.Update(ctx, &backup)
		if err != nil {
			logger.Error("failed to add finalizer", "finalizer", RBDPVCBackupFinalizerName, "error", err)
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	if backup.Status.Conditions == backupv1.RBDPVCBackupConditionsBound || backup.Status.Conditions == backupv1.RBDPVCBackupConditionsDeleting {
		return ctrl.Result{}, nil
	}

	if backup.Status.Conditions != backupv1.RBDPVCBackupConditionsCreating {
		err := r.updateConditions(ctx, &backup, backupv1.RBDPVCBackupConditionsCreating)
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	command := []string{"rbd", "snap", "create", poolName + "/" + imageName + "@" + backup.Name}
	_, err = executeCommand(command, nil)
	if err != nil {
		command = []string{"rbd", "snap", "ls", poolName + "/" + imageName, "--format=json"}
		out, err := executeCommand(command, nil)
		if err != nil {
			logger.Info("failed to run `rbd snap ls`", "poolName", poolName, "imageName", imageName, "error", err)
			return ctrl.Result{Requeue: true}, nil
		}
		var snapshots []Snapshot
		err = json.Unmarshal([]byte(out), &snapshots)
		if err != nil {
			logger.Error("failed to unmarshal json", "json", out, "error", err)
			err2 := r.updateConditions(ctx, &backup, backupv1.RBDPVCBackupConditionsFailed)
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
			logger.Info("snapshot not exists", "snapshotName", backup.Name)
			return ctrl.Result{Requeue: true}, nil
		}
	}

	backup.Status.CreatedAt = metav1.NewTime(time.Now())
	backup.Status.Conditions = backupv1.RBDPVCBackupConditionsBound
	err = r.Status().Update(ctx, &backup)
	if err != nil {
		logger.Error("failed to update status", "createdAt", backup.Status.CreatedAt, "conditions", backup.Status.Conditions, "error", err)
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
