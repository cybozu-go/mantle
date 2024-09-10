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
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/pkg/controller/proto"
)

const (
	labelLocalBackupTargetPVCUID  = "mantle.cybozu.io/local-backup-target-pvc-uid"
	labelRemoteBackupTargetPVCUID = "mantle.cybozu.io/remote-backup-target-pvc-uid"
	annotRemoteUID                = "mantle.cybozu.io/remote-uid"
	finalizerMantleBackup         = "mantlebackup.mantle.cybozu.io/finalizer"
)

// MantleBackupReconciler reconciles a MantleBackup object
type MantleBackupReconciler struct {
	client.Client
	Scheme               *runtime.Scheme
	managedCephClusterID string
	role                 string
	primarySettings      *PrimarySettings // This should be non-nil if and only if role equals 'primary'.
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
func NewMantleBackupReconciler(client client.Client, scheme *runtime.Scheme, managedCephClusterID, role string, primarySettings *PrimarySettings) *MantleBackupReconciler {
	return &MantleBackupReconciler{
		Client:               client,
		Scheme:               scheme,
		managedCephClusterID: managedCephClusterID,
		role:                 role,
		primarySettings:      primarySettings,
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

func (r *MantleBackupReconciler) updateStatus(ctx context.Context, backup *mantlev1.MantleBackup, condition metav1.Condition) error {
	err := updateMantleBackupStatus(ctx, r.Client, backup, func() error {
		meta.SetStatusCondition(&backup.Status.Conditions, condition)
		return nil
	})
	if err != nil {
		logger.Error("failed to update status", "status", backup.Status, "error", err)
		return err
	}
	return nil
}

func (r *MantleBackupReconciler) removeRBDSnapshot(poolName, imageName, snapshotName string) error {
	command := []string{"rbd", "snap", "rm", poolName + "/" + imageName + "@" + snapshotName}
	_, err := executeCommand(command, nil)
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			waitStatus := exitError.Sys().(syscall.WaitStatus)
			exitCode := waitStatus.ExitStatus()
			if exitCode != int(syscall.ENOENT) {
				logger.Error("failed to remove rbd snapshot", "poolName", poolName, "imageName", imageName, "snapshotName", snapshotName, "exitCode", exitCode, "error", err)
				return fmt.Errorf("failed to remove rbd snapshot")
			}
		}
		logger.Info("rbd snapshot has already been removed", "poolName", poolName, "imageName", imageName, "snapshotName", snapshotName, "error", err)
	}
	return nil
}

func listRBDSnapshots(poolName, imageName string) ([]Snapshot, error) {
	command := []string{"rbd", "snap", "ls", poolName + "/" + imageName, "--format=json"}
	out, err := executeCommand(command, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to execute `rbd snap ls`: %s: %s: %w", poolName, imageName, err)
	}

	var snapshots []Snapshot
	err = json.Unmarshal(out, &snapshots)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal the output of `rbd snap ls`: %s: %s: %w", poolName, imageName, err)
	}

	return snapshots, nil
}

func findRBDSnapshot(poolName, imageName, snapshotName string) (*Snapshot, error) {
	snapshots, err := listRBDSnapshots(poolName, imageName)
	if err != nil {
		return nil, err
	}
	for _, s := range snapshots {
		if s.Name == snapshotName {
			return &s, nil
		}
	}
	return nil, fmt.Errorf("snapshot not found: %s: %s: %s", poolName, imageName, snapshotName)
}

func (r *MantleBackupReconciler) createRBDSnapshot(ctx context.Context, poolName, imageName string, backup *mantlev1.MantleBackup) (ctrl.Result, error) {
	command := []string{"rbd", "snap", "create", poolName + "/" + imageName + "@" + backup.Name}
	_, err := executeCommand(command, nil)
	if err != nil {
		_, err := findRBDSnapshot(poolName, imageName, backup.Name)
		if err != nil {
			logger.Error("failed to find rbd snapshot", "error", err)
			err := r.updateStatus(ctx, backup, metav1.Condition{
				Type:   mantlev1.BackupConditionReadyToUse,
				Status: metav1.ConditionFalse,
				Reason: mantlev1.BackupReasonFailedToCreateBackup,
			})
			if err != nil {
				return ctrl.Result{}, err
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
//+kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the MantleBackup object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.16.3/pkg/reconcile
//
// Reconcile is the main component of mantle-controller, so let's admit that Reconcile can be complex by `nolint:gocyclo`
//
//nolint:gocyclo
func (r *MantleBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var backup mantlev1.MantleBackup
	logger := logger.With("MantleBackup", req.NamespacedName)

	if r.role == RoleSecondary {
		return ctrl.Result{}, nil
	}

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
		err2 := r.updateStatus(ctx, &backup, metav1.Condition{Type: mantlev1.BackupConditionReadyToUse, Status: metav1.ConditionFalse, Reason: mantlev1.BackupReasonFailedToCreateBackup})
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

	clusterID, err := getCephClusterIDFromPVC(ctx, logger, r.Client, &pvc)
	if err != nil {
		logger.Error("failed to get clusterID from PVC", "namespace", req.Namespace, "name", pvcName, "error", err)
		err2 := r.updateStatus(ctx, &backup, metav1.Condition{Type: mantlev1.BackupConditionReadyToUse, Status: metav1.ConditionFalse, Reason: mantlev1.BackupReasonFailedToCreateBackup})
		if err2 != nil {
			return ctrl.Result{}, err2
		}

		return ctrl.Result{}, err
	}
	if clusterID != r.managedCephClusterID {
		logger.Info("clusterID not matched", "namespace", req.Namespace, "backup", backup.Name, "pvc", pvcName, "clusterID", clusterID, "managedCephClusterID", r.managedCephClusterID)
		return ctrl.Result{}, nil
	}

	if pvc.Status.Phase != corev1.ClaimBound {
		err := r.updateStatus(ctx, &backup, metav1.Condition{Type: mantlev1.BackupConditionReadyToUse, Status: metav1.ConditionFalse, Reason: mantlev1.BackupReasonFailedToCreateBackup})
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
		err2 := r.updateStatus(ctx, &backup, metav1.Condition{Type: mantlev1.BackupConditionReadyToUse, Status: metav1.ConditionFalse, Reason: mantlev1.BackupReasonFailedToCreateBackup})
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
			err := r.removeRBDSnapshot(poolName, imageName, backup.Name)
			if err != nil {
				return ctrl.Result{}, err
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
		err := r.updateStatus(ctx, &backup, metav1.Condition{Type: mantlev1.BackupConditionReadyToUse, Status: metav1.ConditionFalse, Reason: mantlev1.BackupReasonNone})
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Make sure that the reconciled MantleBackup is created not by the secondary mantle but by the primary mantle.
	if backup.Labels != nil {
		_, ok1 := backup.Labels[labelLocalBackupTargetPVCUID]
		_, ok2 := backup.Labels[labelRemoteBackupTargetPVCUID]
		if ok1 && ok2 {
			logger.Warn(
				"skipping to reconcile the MantleBackup created by a remote mantle-controller to prevent accidental data loss",
				"name", backup.GetName(),
				"namespace", backup.GetNamespace(),
			)
			return ctrl.Result{}, nil
		}
	}

	// Attach local-backup-target-pvc-uid label
	if _, err := ctrl.CreateOrUpdate(ctx, r.Client, &backup, func() error {
		if backup.Labels == nil {
			backup.Labels = map[string]string{}
		}
		backup.Labels[labelLocalBackupTargetPVCUID] = string(pvc.GetUID())
		return nil
	}); err != nil {
		return ctrl.Result{}, nil
	}

	result, err := r.createRBDSnapshot(ctx, poolName, imageName, &backup)
	if err != nil {
		return result, err
	}

	if err := updateMantleBackupStatus(ctx, r.Client, &backup, func() error {
		pvcJs, err := json.Marshal(pvc)
		if err != nil {
			logger.Error("failed to marshal PVC", "error", err)
			return err
		}
		backup.Status.PVCManifest = string(pvcJs)

		pvJs, err := json.Marshal(pv)
		if err != nil {
			logger.Error("failed to marshal PV", "error", err)
			return err
		}
		backup.Status.PVManifest = string(pvJs)

		snapshot, err := findRBDSnapshot(poolName, imageName, backup.Name)
		if err != nil {
			return err
		}
		backup.Status.SnapID = &snapshot.Id

		createdAt, err := time.Parse("Mon Jan  2 15:04:05 2006", snapshot.Timestamp)
		if err != nil {
			return err
		}
		backup.Status.CreatedAt = metav1.NewTime(createdAt)

		meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
			Type: mantlev1.BackupConditionReadyToUse, Status: metav1.ConditionTrue, Reason: mantlev1.BackupReasonNone})
		return nil
	}); err != nil {
		logger.Error("failed to update MantleBackup status", "status", backup.Status, "error", err)
		return ctrl.Result{}, err
	}

	if r.role == RolePrimary {
		if err := r.replicate(ctx, &backup, &pvc); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MantleBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mantlev1.MantleBackup{}).
		Complete(r)
}

func (r *MantleBackupReconciler) replicate(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	pvc *corev1.PersistentVolumeClaim,
) error {
	// Make sure the arguments are valid
	if backup.Status.SnapID == nil {
		return fmt.Errorf("backup.Status.SnapID should not be nil: %s: %s", backup.GetName(), backup.GetNamespace())
	}

	// Make sure all of the preceding backups for the same PVC have already been replicated.
	var backupList mantlev1.MantleBackupList
	if err := r.Client.List(ctx, &backupList, &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(map[string]string{labelLocalBackupTargetPVCUID: string(pvc.GetUID())}),
	}); err != nil {
		return err
	}
	for _, backup1 := range backupList.Items {
		cond := meta.FindStatusCondition(backup1.Status.Conditions, mantlev1.BackupConditionSyncedToRemote)
		if (backup1.Status.SnapID == nil || *backup1.Status.SnapID < *backup.Status.SnapID) &&
			backup1.ObjectMeta.DeletionTimestamp.IsZero() &&
			(cond == nil || cond.Status != metav1.ConditionTrue) {
			return fmt.Errorf("waiting for preceding backups to be replicated: %s: %s", backup.GetName(), backup.GetNamespace())
		}
	}

	// Create a PVC that should be sent to the secondary mantle.
	var pvcSent corev1.PersistentVolumeClaim
	pvcSent.SetName(pvc.GetName())
	pvcSent.SetNamespace(pvc.GetNamespace())
	pvcSent.SetAnnotations(map[string]string{
		annotRemoteUID: string(pvc.GetUID()),
	})
	pvcSent.Spec = pvc.Spec
	pvcSentJson, err := json.Marshal(pvcSent)
	if err != nil {
		return err
	}

	// Call CreateOrUpdatePVC
	client := r.primarySettings.Client
	resp, err := client.CreateOrUpdatePVC(
		ctx,
		&proto.CreateOrUpdatePVCRequest{
			Pvc: string(pvcSentJson),
		},
	)
	if err != nil {
		return err
	}

	// Create a MantleBackup that should be sent to the secondary mantle.
	var backupSent mantlev1.MantleBackup
	backupSent.SetName(backup.GetName())
	backupSent.SetNamespace(backup.GetNamespace())
	backupSent.SetAnnotations(map[string]string{
		annotRemoteUID: string(backup.GetUID()),
	})
	backupSent.SetLabels(map[string]string{
		labelLocalBackupTargetPVCUID:  resp.Uid,
		labelRemoteBackupTargetPVCUID: string(pvc.GetUID()),
	})
	backupSent.SetFinalizers([]string{finalizerMantleBackup})
	backupSent.Spec = backup.Spec
	backupSent.Status.CreatedAt = backup.Status.CreatedAt
	backupSentJson, err := json.Marshal(backupSent)
	if err != nil {
		return err
	}

	// Call CreateOrUpdateMantleBackup.
	if _, err := client.CreateOrUpdateMantleBackup(
		ctx,
		&proto.CreateOrUpdateMantleBackupRequest{
			MantleBackup: string(backupSentJson),
		},
	); err != nil {
		return err
	}

	// Update the status of the MantleBackup.
	if err := r.updateStatus(ctx, backup, metav1.Condition{
		Type:   mantlev1.BackupConditionSyncedToRemote,
		Status: metav1.ConditionTrue,
		Reason: mantlev1.BackupReasonNone,
	}); err != nil {
		return err
	}

	return nil
}
