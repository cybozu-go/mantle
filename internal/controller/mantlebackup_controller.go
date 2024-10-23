package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/ceph"
	"github.com/cybozu-go/mantle/pkg/controller/proto"
	corev1 "k8s.io/api/core/v1"
	aerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kube-openapi/pkg/validation/strfmt"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	MantleBackupFinalizerName = "mantlebackup.mantle.cybozu.io/finalizer"

	labelLocalBackupTargetPVCUID  = "mantle.cybozu.io/local-backup-target-pvc-uid"
	labelRemoteBackupTargetPVCUID = "mantle.cybozu.io/remote-backup-target-pvc-uid"
	annotRemoteUID                = "mantle.cybozu.io/remote-uid"
	annotDiffFrom                 = "mantle.cybozu.io/diff-from"
	annotDiffTo                   = "mantle.cybozu.io/diff-to"
	annotRetainIfExpired          = "mantle.cybozu.io/retain-if-expired"
	annotSyncMode                 = "mantle.cybozu.io/sync-mode"

	syncModeFull        = "full"
	syncModeIncremental = "incremental"
)

// MantleBackupReconciler reconciles a MantleBackup object
type MantleBackupReconciler struct {
	client.Client
	Scheme               *runtime.Scheme
	ceph                 ceph.CephCmd
	managedCephClusterID string
	role                 string
	primarySettings      *PrimarySettings // This should be non-nil if and only if role equals 'primary'.
	expireQueueCh        chan event.GenericEvent
}

// NewMantleBackupReconciler returns NodeReconciler.
func NewMantleBackupReconciler(client client.Client, scheme *runtime.Scheme, managedCephClusterID, role string, primarySettings *PrimarySettings) *MantleBackupReconciler {
	return &MantleBackupReconciler{
		Client:               client,
		Scheme:               scheme,
		ceph:                 ceph.NewCephCmd(),
		managedCephClusterID: managedCephClusterID,
		role:                 role,
		primarySettings:      primarySettings,
		expireQueueCh:        make(chan event.GenericEvent),
	}
}

func (r *MantleBackupReconciler) updateStatusCondition(ctx context.Context, backup *mantlev1.MantleBackup, condition metav1.Condition) error {
	logger := log.FromContext(ctx)
	err := updateStatus(ctx, r.Client, backup, func() error {
		meta.SetStatusCondition(&backup.Status.Conditions, condition)
		return nil
	})
	if err != nil {
		logger.Error(err, "failed to update status", "status", backup.Status)
		return err
	}
	return nil
}

func (r *MantleBackupReconciler) removeRBDSnapshot(ctx context.Context, poolName, imageName, snapshotName string) error {
	logger := log.FromContext(ctx)
	rmErr := r.ceph.RBDSnapRm(poolName, imageName, snapshotName)
	if rmErr != nil {
		_, findErr := ceph.FindRBDSnapshot(r.ceph, poolName, imageName, snapshotName)
		if findErr != nil && findErr != ceph.ErrSnapshotNotFound {
			err := errors.Join(rmErr, findErr)
			logger.Error(err, "failed to remove rbd snapshot", "poolName", poolName, "imageName", imageName, "snapshotName", snapshotName)
			return fmt.Errorf("failed to remove rbd snapshot: %w", err)
		}
		logger.Info("rbd snapshot has already been removed", "poolName", poolName, "imageName", imageName, "snapshotName", snapshotName)
		return nil
	}
	return nil
}

func (r *MantleBackupReconciler) createRBDSnapshot(ctx context.Context, poolName, imageName string, backup *mantlev1.MantleBackup) (*ceph.RBDSnapshot, error) {
	logger := log.FromContext(ctx)
	createErr := r.ceph.RBDSnapCreate(poolName, imageName, backup.Name)
	snap, findErr := ceph.FindRBDSnapshot(r.ceph, poolName, imageName, backup.Name)
	if findErr != nil {
		logger.Error(errors.Join(createErr, findErr), "failed to find rbd snapshot")
		updateStatusErr := r.updateStatusCondition(ctx, backup, metav1.Condition{
			Type:   mantlev1.BackupConditionReadyToUse,
			Status: metav1.ConditionFalse,
			Reason: mantlev1.BackupReasonFailedToCreateBackup,
		})
		return nil, errors.Join(createErr, findErr, updateStatusErr)
	}
	return snap, nil
}

func (r *MantleBackupReconciler) checkPVCInManagedCluster(ctx context.Context, backup *mantlev1.MantleBackup, pvc *corev1.PersistentVolumeClaim) error {
	logger := log.FromContext(ctx)
	clusterID, err := getCephClusterIDFromPVC(ctx, r.Client, pvc)
	if err != nil {
		logger.Error(err, "failed to get clusterID from PVC", "namespace", pvc.Namespace, "name", pvc.Name)
		err2 := r.updateStatusCondition(ctx, backup, metav1.Condition{Type: mantlev1.BackupConditionReadyToUse, Status: metav1.ConditionFalse, Reason: mantlev1.BackupReasonFailedToCreateBackup})
		if err2 != nil {
			return err2
		}

		return err
	}
	if clusterID != r.managedCephClusterID {
		logger.Info("clusterID not matched", "pvc", pvc.Name, "clusterID", clusterID, "managedCephClusterID", r.managedCephClusterID)
		return errSkipProcessing
	}

	return nil
}

func (r *MantleBackupReconciler) isPVCBound(ctx context.Context, backup *mantlev1.MantleBackup, pvc *corev1.PersistentVolumeClaim) (bool, error) {
	logger := log.FromContext(ctx)
	if pvc.Status.Phase != corev1.ClaimBound {
		err := r.updateStatusCondition(ctx, backup, metav1.Condition{Type: mantlev1.BackupConditionReadyToUse, Status: metav1.ConditionFalse, Reason: mantlev1.BackupReasonFailedToCreateBackup})
		if err != nil {
			return false, err
		}

		if pvc.Status.Phase == corev1.ClaimPending {
			return false, nil
		} else {
			logger.Info("PVC phase is neither bound nor pending", "status.phase", pvc.Status.Phase)
			return false, fmt.Errorf("PVC phase is neither bound nor pending (status.phase: %s)", pvc.Status.Phase)
		}
	}
	return true, nil
}

type snapshotTarget struct {
	pvc       *corev1.PersistentVolumeClaim
	pv        *corev1.PersistentVolume
	imageName string
	poolName  string
}

var errSkipProcessing = fmt.Errorf("skip processing")

type errTargetPVCNotFound struct {
	error
}

func isErrTargetPVCNotFound(err error) bool {
	_, ok := err.(errTargetPVCNotFound)
	return ok
}

func (r *MantleBackupReconciler) getSnapshotTarget(ctx context.Context, backup *mantlev1.MantleBackup) (
	*snapshotTarget,
	ctrl.Result,
	error,
) {
	logger := log.FromContext(ctx)
	pvcNamespace := backup.Namespace
	pvcName := backup.Spec.PVC
	var pvc corev1.PersistentVolumeClaim
	err := r.Get(ctx, types.NamespacedName{Namespace: pvcNamespace, Name: pvcName}, &pvc)
	if err != nil {
		logger.Error(err, "failed to get PVC", "namespace", pvcNamespace, "name", pvcName)
		err2 := r.updateStatusCondition(ctx, backup, metav1.Condition{Type: mantlev1.BackupConditionReadyToUse, Status: metav1.ConditionFalse, Reason: mantlev1.BackupReasonFailedToCreateBackup})
		if err2 != nil {
			return nil, ctrl.Result{}, err2
		}
		if aerrors.IsNotFound(err) {
			return nil, ctrl.Result{}, errTargetPVCNotFound{err}
		}
		return nil, ctrl.Result{}, err
	}

	if err := r.checkPVCInManagedCluster(ctx, backup, &pvc); err != nil {
		return nil, ctrl.Result{}, err
	}

	ok, err := r.isPVCBound(ctx, backup, &pvc)
	if err != nil {
		return nil, ctrl.Result{}, err
	}
	if !ok {
		logger.Info("waiting for PVC bound.")
		return nil, ctrl.Result{Requeue: true}, nil
	}

	pvName := pvc.Spec.VolumeName
	var pv corev1.PersistentVolume
	err = r.Get(ctx, types.NamespacedName{Name: pvName}, &pv)
	if err != nil {
		logger.Error(err, "failed to get PV", "name", pvName)
		err2 := r.updateStatusCondition(ctx, backup, metav1.Condition{Type: mantlev1.BackupConditionReadyToUse, Status: metav1.ConditionFalse, Reason: mantlev1.BackupReasonFailedToCreateBackup})
		if err2 != nil {
			return nil, ctrl.Result{}, err2
		}

		return nil, ctrl.Result{}, err
	}

	imageName, ok := pv.Spec.CSI.VolumeAttributes["imageName"]
	if !ok {
		return nil, ctrl.Result{}, fmt.Errorf("failed to get imageName from PV")
	}
	poolName, ok := pv.Spec.CSI.VolumeAttributes["pool"]
	if !ok {
		return nil, ctrl.Result{}, fmt.Errorf("failed to get pool from PV")
	}

	return &snapshotTarget{&pvc, &pv, imageName, poolName}, ctrl.Result{}, nil
}

// expire deletes the backup if it is already expired. Otherwise it schedules deletion.
// Note that this function does not use requeue to scheduled deletion because the caller
// will do other tasks after this function returns.
func (r *MantleBackupReconciler) expire(ctx context.Context, backup *mantlev1.MantleBackup) error {
	logger := log.FromContext(ctx)
	if backup.Status.CreatedAt.IsZero() {
		// the RBD snapshot has not be taken yet, do nothing.
		return nil
	}

	if v, ok := backup.Annotations[annotRetainIfExpired]; ok && v == "true" {
		// retain this backup.
		// If the annotation is deleted, reconciliation will run, so no need to schedule.
		return nil
	}

	expire, err := strfmt.ParseDuration(backup.Spec.Expire)
	if err != nil {
		return err
	}
	expireAt := backup.Status.CreatedAt.Add(expire)
	if time.Now().UTC().After(expireAt) {
		// already expired, delete it immediately.
		logger.Info("delete expired backup", "createdAt", backup.Status.CreatedAt, "expire", expire)
		return r.Delete(ctx, backup)
	}

	// not expired yet. schedule deletion.
	// The event may be sent many times, but it is safe because workqueue AddAfter
	// deduplicates events for same object.
	r.expireQueueCh <- event.GenericEvent{
		Object: backup,
	}

	return nil
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
func (r *MantleBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	var backup mantlev1.MantleBackup

	err := r.Get(ctx, req.NamespacedName, &backup)
	if aerrors.IsNotFound(err) {
		logger.Info("MantleBackup is not found", "error", err)
		return ctrl.Result{}, nil
	}
	if err != nil {
		logger.Error(err, "failed to get MantleBackup")
		return ctrl.Result{}, err
	}

	switch r.role {
	case RoleStandalone:
		return r.reconcileAsStandalone(ctx, &backup)
	case RolePrimary:
		return r.reconcileAsPrimary(ctx, &backup)
	case RoleSecondary:
		return r.reconcileAsSecondary(ctx, &backup)
	}

	panic("unreachable")
}

func (r *MantleBackupReconciler) reconcileAsStandalone(ctx context.Context, backup *mantlev1.MantleBackup) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if isCreatedWhenMantleControllerWasSecondary(backup) {
		logger.Info(
			"skipping to reconcile the MantleBackup created by a remote mantle-controller to prevent accidental data loss",
		)
		return ctrl.Result{}, nil
	}

	target, result, getSnapshotTargetErr := r.getSnapshotTarget(ctx, backup)
	switch {
	case getSnapshotTargetErr == errSkipProcessing:
		return ctrl.Result{}, nil
	case isErrTargetPVCNotFound(getSnapshotTargetErr):
		// deletion logic may run.
	case getSnapshotTargetErr == nil:
	default:
		return ctrl.Result{}, getSnapshotTargetErr
	}
	if result.Requeue {
		return result, nil
	}

	if !backup.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.finalizeStandalone(ctx, backup, target, isErrTargetPVCNotFound(getSnapshotTargetErr))
	}

	if getSnapshotTargetErr != nil {
		return ctrl.Result{}, getSnapshotTargetErr
	}

	if !controllerutil.ContainsFinalizer(backup, MantleBackupFinalizerName) {
		controllerutil.AddFinalizer(backup, MantleBackupFinalizerName)

		if err := r.Update(ctx, backup); err != nil {
			logger.Error(err, "failed to add finalizer", "finalizer", MantleBackupFinalizerName)
			return ctrl.Result{}, err
		}
		err := r.updateStatusCondition(ctx, backup, metav1.Condition{Type: mantlev1.BackupConditionReadyToUse, Status: metav1.ConditionFalse, Reason: mantlev1.BackupReasonNone})
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	if err := r.expire(ctx, backup); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.provisionRBDSnapshot(ctx, backup, target); err != nil {
		return ctrl.Result{}, err
	}

	// Skip replication if SyncedToRemote condition is true.
	if meta.IsStatusConditionTrue(backup.Status.Conditions, mantlev1.BackupConditionSyncedToRemote) {
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

func (r *MantleBackupReconciler) reconcileAsPrimary(ctx context.Context, backup *mantlev1.MantleBackup) (ctrl.Result, error) {
	result, err := r.reconcileAsStandalone(ctx, backup)
	if err != nil || !result.IsZero() {
		return result, err
	}
	return r.replicate(ctx, backup)
}

func (r *MantleBackupReconciler) reconcileAsSecondary(ctx context.Context, backup *mantlev1.MantleBackup) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if !isCreatedWhenMantleControllerWasSecondary(backup) {
		logger.Info(
			"skipping to reconcile the MantleBackup created by a different mantle-controller to prevent accidental data loss",
		)
		return ctrl.Result{}, nil
	}

	target, result, getSnapshotTargetErr := r.getSnapshotTarget(ctx, backup)
	switch {
	case getSnapshotTargetErr == errSkipProcessing:
		return ctrl.Result{}, nil
	case isErrTargetPVCNotFound(getSnapshotTargetErr):
		// deletion logic may run.
	case getSnapshotTargetErr == nil:
	default:
		return ctrl.Result{}, getSnapshotTargetErr
	}
	if result.Requeue {
		return result, nil
	}

	if !backup.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.finalizeSecondary(ctx, backup, target, isErrTargetPVCNotFound(getSnapshotTargetErr))
	}

	if getSnapshotTargetErr != nil {
		return ctrl.Result{}, getSnapshotTargetErr
	}

	if err := r.expire(ctx, backup); err != nil {
		return ctrl.Result{}, err
	}

	if !meta.IsStatusConditionTrue(backup.Status.Conditions, mantlev1.BackupConditionReadyToUse) {
		result, err := r.startImport(ctx, backup, target)
		if err != nil || !result.IsZero() {
			return result, err
		}
	}

	return r.secondaryCleanup(ctx, backup)
}

func scheduleExpire(_ context.Context, evt event.GenericEvent, q workqueue.RateLimitingInterface) {
	backup := evt.Object.(*mantlev1.MantleBackup)
	// the parse never fails because expire method checked it.
	expire, _ := strfmt.ParseDuration(backup.Spec.Expire)
	q.AddAfter(
		ctrl.Request{
			NamespacedName: types.NamespacedName{
				Namespace: backup.GetNamespace(),
				Name:      backup.GetName(),
			},
		},
		time.Until(backup.Status.CreatedAt.Add(expire)),
	)
}

// SetupWithManager sets up the controller with the Manager.
func (r *MantleBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mantlev1.MantleBackup{}).
		WatchesRawSource(
			&source.Channel{Source: r.expireQueueCh},
			handler.Funcs{GenericFunc: scheduleExpire},
		).
		Complete(r)
}

func (r *MantleBackupReconciler) replicate(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
) (ctrl.Result, error) {
	result, err := r.replicateManifests(ctx, backup)
	if err != nil || result != (ctrl.Result{}) {
		return result, err
	}
	prepareResult, err := r.prepareForDataSynchronization(ctx, backup, r.primarySettings.Client)
	if err != nil {
		return ctrl.Result{}, err
	}

	// FIXME: Delete this code after implementing export().
	prepareResult.isSecondaryMantleBackupReadyToUse = true

	if prepareResult.isSecondaryMantleBackupReadyToUse {
		return r.primaryCleanup(ctx, backup)
	}
	return r.export(ctx, backup, r.primarySettings.Client, prepareResult)
}

func (r *MantleBackupReconciler) replicateManifests(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
) (ctrl.Result, error) {
	// Unmarshal the PVC manifest stored in the status of the MantleBackup resource.
	var pvc corev1.PersistentVolumeClaim
	if err := json.Unmarshal([]byte(backup.Status.PVCManifest), &pvc); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to unmarshal the PVC stored in the status of the MantleBackup resource: %w", err)
	}

	// Make sure the arguments are valid
	if backup.Status.SnapID == nil {
		return ctrl.Result{}, fmt.Errorf("backup.Status.SnapID should not be nil: %s: %s", backup.GetName(), backup.GetNamespace())
	}

	// Make sure all of the preceding backups for the same PVC have already been replicated.
	var backupList mantlev1.MantleBackupList
	if err := r.Client.List(ctx, &backupList, &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(map[string]string{labelLocalBackupTargetPVCUID: string(pvc.GetUID())}),
	}); err != nil {
		return ctrl.Result{}, err
	}
	for _, backup1 := range backupList.Items {
		if backup1.Status.SnapID == nil ||
			*backup1.Status.SnapID < *backup.Status.SnapID &&
				backup1.ObjectMeta.DeletionTimestamp.IsZero() &&
				!meta.IsStatusConditionTrue(backup1.Status.Conditions, mantlev1.BackupConditionSyncedToRemote) {
			return ctrl.Result{Requeue: true}, nil
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
	pvcSent.Spec.VolumeName = "" // The VolumeName should be blank.
	pvcSentJson, err := json.Marshal(pvcSent)
	if err != nil {
		return ctrl.Result{}, err
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
		return ctrl.Result{}, err
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
	backupSent.SetFinalizers([]string{MantleBackupFinalizerName})
	backupSent.Spec = backup.Spec
	backupSent.Status.CreatedAt = backup.Status.CreatedAt
	backupSentJson, err := json.Marshal(backupSent)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Call CreateOrUpdateMantleBackup.
	if _, err := client.CreateOrUpdateMantleBackup(
		ctx,
		&proto.CreateOrUpdateMantleBackupRequest{
			MantleBackup: string(backupSentJson),
		},
	); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *MantleBackupReconciler) provisionRBDSnapshot(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	target *snapshotTarget,
) error {
	logger := log.FromContext(ctx)

	// Attach local-backup-target-pvc-uid label before trying to create a RBD
	// snapshot corresponding to the given MantleBackup, so that we can make
	// sure that every MantleBackup that has a RBD snapshot is labelled with
	// local-backup-target-pvc-uid.
	if _, err := ctrl.CreateOrUpdate(ctx, r.Client, backup, func() error {
		if backup.Labels == nil {
			backup.Labels = map[string]string{}
		}
		backup.Labels[labelLocalBackupTargetPVCUID] = string(target.pvc.GetUID())
		return nil
	}); err != nil {
		return err
	}

	// If the given MantleBackup is not ready to use, create a new RBD snapshot and update its status.
	if meta.IsStatusConditionTrue(
		backup.Status.Conditions,
		mantlev1.BackupConditionReadyToUse,
	) {
		return nil
	}

	snapshot, err := r.createRBDSnapshot(ctx, target.poolName, target.imageName, backup)
	if err != nil {
		return err
	}

	if err := updateStatus(ctx, r.Client, backup, func() error {
		pvcJs, err := json.Marshal(target.pvc)
		if err != nil {
			logger.Error(err, "failed to marshal PVC")
			return err
		}
		backup.Status.PVCManifest = string(pvcJs)

		pvJs, err := json.Marshal(target.pv)
		if err != nil {
			logger.Error(err, "failed to marshal PV")
			return err
		}
		backup.Status.PVManifest = string(pvJs)

		backup.Status.SnapID = &snapshot.Id
		backup.Status.CreatedAt = metav1.NewTime(snapshot.Timestamp.Time)

		meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
			Type: mantlev1.BackupConditionReadyToUse, Status: metav1.ConditionTrue, Reason: mantlev1.BackupReasonNone})
		return nil
	}); err != nil {
		logger.Error(err, "failed to update MantleBackup status", "status", backup.Status)
		return err
	}

	return nil
}

// isCreatedWhenMantleControllerWasSecondary returns true iff the MantleBackup
// is created by the secondary mantle.
func isCreatedWhenMantleControllerWasSecondary(backup *mantlev1.MantleBackup) bool {
	_, ok := backup.Annotations[annotRemoteUID]
	return ok
}

func (r *MantleBackupReconciler) finalizeStandalone(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	target *snapshotTarget,
	targetPVCNotFound bool,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	if _, ok := backup.GetAnnotations()[annotDiffTo]; ok {
		return ctrl.Result{Requeue: true}, nil
	}

	// primaryClean() is called in finalizeStandalone() to delete resources for
	// exported and uploaded snapshots in both standalone and primary Mantle.
	result, err := r.primaryCleanup(ctx, backup)
	if err != nil || result != (ctrl.Result{}) {
		return result, err
	}

	if !controllerutil.ContainsFinalizer(backup, MantleBackupFinalizerName) {
		return ctrl.Result{}, nil
	}

	if !targetPVCNotFound {
		err := r.removeRBDSnapshot(ctx, target.poolName, target.imageName, backup.Name)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	controllerutil.RemoveFinalizer(backup, MantleBackupFinalizerName)
	if err := r.Update(ctx, backup); err != nil {
		logger.Error(err, "failed to remove finalizer", "finalizer", MantleBackupFinalizerName)
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *MantleBackupReconciler) finalizeSecondary(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	target *snapshotTarget,
	targetPVCNotFound bool,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	if _, ok := backup.GetAnnotations()[annotDiffTo]; ok {
		return ctrl.Result{Requeue: true}, nil
	}

	result, err := r.secondaryCleanup(ctx, backup)
	if err != nil || result != (ctrl.Result{}) {
		return result, err
	}

	if !controllerutil.ContainsFinalizer(backup, MantleBackupFinalizerName) {
		return ctrl.Result{}, nil
	}

	if !targetPVCNotFound {
		err := r.removeRBDSnapshot(ctx, target.poolName, target.imageName, backup.Name)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	controllerutil.RemoveFinalizer(backup, MantleBackupFinalizerName)
	if err := r.Update(ctx, backup); err != nil {
		logger.Error(err, "failed to remove finalizer", "finalizer", MantleBackupFinalizerName)
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

type dataSyncPrepareResult struct {
	isIncremental                     bool // NOTE: The value is forcibly set to false if isSecondaryMantleBackupReadyToUse is true.
	isSecondaryMantleBackupReadyToUse bool
	diffFrom                          *mantlev1.MantleBackup // non-nil value iff isIncremental is true.
}

func (r *MantleBackupReconciler) prepareForDataSynchronization(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	msc proto.MantleServiceClient,
) (*dataSyncPrepareResult, error) {
	exportTargetPVCUID, ok := backup.GetLabels()[labelLocalBackupTargetPVCUID]
	if !ok {
		return nil, fmt.Errorf(`"%s" label is missing`, labelLocalBackupTargetPVCUID)
	}
	resp, err := msc.ListMantleBackup(
		ctx,
		&proto.ListMantleBackupRequest{
			PvcUID:    exportTargetPVCUID,
			Namespace: backup.GetNamespace(),
		},
	)
	if err != nil {
		return nil, err
	}
	secondaryBackups := make([]mantlev1.MantleBackup, 0)
	err = json.Unmarshal(resp.MantleBackupList, &secondaryBackups)
	if err != nil {
		return nil, err
	}
	secondaryBackupMap := convertToMap(secondaryBackups)

	isSecondaryMantleBackupReadyToUse := false
	secondaryBackup, ok := secondaryBackupMap[backup.GetName()]
	if !ok {
		return nil, fmt.Errorf("secondary MantleBackup not found: %s, %s",
			backup.GetName(), backup.GetNamespace())
	}
	isSecondaryMantleBackupReadyToUse = meta.IsStatusConditionTrue(
		secondaryBackup.Status.Conditions,
		mantlev1.BackupConditionReadyToUse,
	)

	if isSecondaryMantleBackupReadyToUse {
		return &dataSyncPrepareResult{
			isIncremental:                     false,
			isSecondaryMantleBackupReadyToUse: true,
			diffFrom:                          nil,
		}, nil
	}

	if syncMode, ok := backup.GetAnnotations()[annotSyncMode]; ok {
		switch syncMode {
		case syncModeFull:
			return &dataSyncPrepareResult{
				isIncremental:                     false,
				isSecondaryMantleBackupReadyToUse: isSecondaryMantleBackupReadyToUse,
				diffFrom:                          nil,
			}, nil
		case syncModeIncremental:
			diffFromName, ok := backup.GetAnnotations()[annotDiffFrom]
			if !ok {
				return nil, fmt.Errorf(`"%s" annotation is missing`, annotDiffFrom)
			}

			var diffFrom mantlev1.MantleBackup
			err = r.Client.Get(ctx, types.NamespacedName{
				Name:      diffFromName,
				Namespace: backup.GetNamespace(),
			}, &diffFrom)
			if err != nil {
				return nil, err
			}

			return &dataSyncPrepareResult{
				isIncremental:                     true,
				isSecondaryMantleBackupReadyToUse: isSecondaryMantleBackupReadyToUse,
				diffFrom:                          &diffFrom,
			}, nil
		default:
			return nil, fmt.Errorf("unknown sync mode: %s", syncMode)
		}
	}

	var primaryBackupList mantlev1.MantleBackupList
	// TODO: Perhaps, we may have to use the client without cache.
	err = r.Client.List(ctx, &primaryBackupList, &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(map[string]string{labelLocalBackupTargetPVCUID: exportTargetPVCUID}),
		Namespace:     backup.GetNamespace(),
	})
	if err != nil {
		return nil, err
	}

	diffFrom := searchForDiffOriginMantleBackup(backup, primaryBackupList.Items, secondaryBackupMap)
	isIncremental := (diffFrom != nil)

	return &dataSyncPrepareResult{
		isIncremental:                     isIncremental,
		isSecondaryMantleBackupReadyToUse: isSecondaryMantleBackupReadyToUse,
		diffFrom:                          diffFrom,
	}, nil
}

func convertToMap(mantleBackups []mantlev1.MantleBackup) map[string]*mantlev1.MantleBackup {
	m := make(map[string]*mantlev1.MantleBackup)
	for _, mantleBackup := range mantleBackups {
		mantleBackup := mantleBackup
		m[mantleBackup.GetName()] = &mantleBackup
	}
	return m
}

func searchForDiffOriginMantleBackup(
	backup *mantlev1.MantleBackup,
	primaryBackups []mantlev1.MantleBackup,
	secondaryBackupMap map[string]*mantlev1.MantleBackup,
) *mantlev1.MantleBackup {
	var diffOrigin *mantlev1.MantleBackup
	for _, primaryBackup := range primaryBackups {
		primaryBackup := primaryBackup
		secondaryBackup, ok := secondaryBackupMap[primaryBackup.Name]
		if !ok {
			continue
		}
		if !meta.IsStatusConditionTrue(primaryBackup.Status.Conditions, mantlev1.BackupConditionReadyToUse) ||
			!meta.IsStatusConditionTrue(secondaryBackup.Status.Conditions, mantlev1.BackupConditionReadyToUse) {
			continue
		}
		if !primaryBackup.DeletionTimestamp.IsZero() || !secondaryBackup.DeletionTimestamp.IsZero() {
			continue
		}
		if *backup.Status.SnapID <= *primaryBackup.Status.SnapID {
			continue
		}
		if diffOrigin == nil || *diffOrigin.Status.SnapID < *primaryBackup.Status.SnapID {
			diffOrigin = &primaryBackup
		}
	}

	return diffOrigin
}

func (r *MantleBackupReconciler) export(
	_ context.Context,
	_ *mantlev1.MantleBackup,
	_ proto.MantleServiceClient,
	prepareResult *dataSyncPrepareResult,
) (ctrl.Result, error) { //nolint:unparam
	if prepareResult.isIncremental {
		return ctrl.Result{}, fmt.Errorf("incremental backup is not implemented")
	}
	return ctrl.Result{}, nil
}

func (r *MantleBackupReconciler) startImport(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	target *snapshotTarget,
) (ctrl.Result, error) { //nolint:unparam
	if result, err := r.isExportDataAlreadyUploaded(ctx, backup); err != nil || !result.IsZero() {
		return result, err
	}

	// Requeue if the PV is smaller than the PVC. (This may be the case if pvc-autoresizer is used.)
	if isPVSmallerThanPVC(target.pv, target.pvc) {
		return ctrl.Result{Requeue: true}, nil
	}

	if err := r.updateStatusManifests(ctx, backup, target.pv, target.pvc); err != nil {
		return ctrl.Result{}, err
	}

	if result, err := r.reconcileDiscardJob(ctx, backup, target); err != nil || !result.IsZero() {
		return result, err
	}

	if result, err := r.reconcileImportJob(ctx, backup, target); err != nil || !result.IsZero() {
		return result, err
	}

	return ctrl.Result{}, nil
}

func (r *MantleBackupReconciler) isExportDataAlreadyUploaded(
	_ context.Context,
	_ *mantlev1.MantleBackup,
) (ctrl.Result, error) { //nolint:unparam
	return ctrl.Result{}, nil
}

func isPVSmallerThanPVC(
	pv *corev1.PersistentVolume,
	pvc *corev1.PersistentVolumeClaim,
) bool {
	return pv.Spec.Capacity.Storage().Cmp(*pvc.Spec.Resources.Requests.Storage()) == -1
}

func (r *MantleBackupReconciler) updateStatusManifests(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	pv *corev1.PersistentVolume,
	pvc *corev1.PersistentVolumeClaim,
) error {
	if backup.Status.PVManifest != "" || backup.Status.PVCManifest != "" {
		return nil
	}
	return updateStatus(ctx, r.Client, backup, func() error {
		pvJSON, err := json.Marshal(*pv)
		if err != nil {
			return err
		}
		backup.Status.PVManifest = string(pvJSON)

		pvcJSON, err := json.Marshal(*pvc)
		if err != nil {
			return err
		}
		backup.Status.PVCManifest = string(pvcJSON)

		return nil
	})
}

func (r *MantleBackupReconciler) reconcileDiscardJob(
	_ context.Context,
	backup *mantlev1.MantleBackup,
	_ *snapshotTarget,
) (ctrl.Result, error) { //nolint:unparam
	if backup.GetAnnotations()[annotSyncMode] != syncModeFull {
		return ctrl.Result{}, nil
	}

	// FIXME: implement here later

	return ctrl.Result{}, nil
}

func (r *MantleBackupReconciler) reconcileImportJob(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	_ *snapshotTarget,
) (ctrl.Result, error) { //nolint:unparam
	// FIXME: implement here later

	if err := r.updateStatusCondition(ctx, backup, metav1.Condition{
		Type:   mantlev1.BackupConditionReadyToUse,
		Status: metav1.ConditionTrue,
		Reason: mantlev1.BackupReasonNone,
	}); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *MantleBackupReconciler) primaryCleanup(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
) (ctrl.Result, error) { // nolint:unparam
	if !backup.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Update the status of the MantleBackup.
	if err := r.updateStatusCondition(ctx, backup, metav1.Condition{
		Type:   mantlev1.BackupConditionSyncedToRemote,
		Status: metav1.ConditionTrue,
		Reason: mantlev1.BackupReasonNone,
	}); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *MantleBackupReconciler) secondaryCleanup(
	_ context.Context,
	_ *mantlev1.MantleBackup,
) (ctrl.Result, error) { // nolint:unparam
	return ctrl.Result{}, nil
}
