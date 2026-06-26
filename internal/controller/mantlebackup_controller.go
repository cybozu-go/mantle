package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	_ "embed"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/ceph"
	"github.com/cybozu-go/mantle/internal/controller/internal/objectstorage"
	"github.com/cybozu-go/mantle/internal/controller/metrics"
	"github.com/cybozu-go/mantle/pkg/controller/proto"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	aerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kube-openapi/pkg/validation/strfmt"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	MantleBackupFinalizerName = "mantlebackup.mantle.cybozu.io/finalizer"

	labelClusterID                = "mantle.cybozu.io/cluster-id"
	labelLocalBackupTargetPVCUID  = "mantle.cybozu.io/local-backup-target-pvc-uid"
	labelRemoteBackupTargetPVCUID = "mantle.cybozu.io/remote-backup-target-pvc-uid"
	labelAppNameValue             = "mantle"
	labelComponentExportData      = "export-data"
	labelComponentExportJob       = "export-job"
	labelComponentUploadJob       = "upload-job"
	labelComponentImportJob       = "import-job"
	labelComponentVerifyJob       = "verify-job"
	labelComponentVerifyVolume    = "verify-volume"
	labelComponentZeroOutJob      = "zeroout-job"
	labelComponentZeroOutVolume   = "zeroout-volume"
	annotRemoteUID                = "mantle.cybozu.io/remote-uid"
	annotDiffFrom                 = "mantle.cybozu.io/diff-from"
	annotDiffTo                   = "mantle.cybozu.io/diff-to"
	annotRetainIfExpired          = "mantle.cybozu.io/retain-if-expired"
	annotSyncMode                 = "mantle.cybozu.io/sync-mode"

	MantleExportJobPrefix     = "mantle-export-"
	MantleUploadJobPrefix     = "mantle-upload-"
	MantleExportDataPVCPrefix = "mantle-export-"
	MantleImportJobPrefix     = "mantle-import-"
	mantleVerifyImagePrefix   = "mantle-verify-"
	MantleVerifyJobPrefix     = "mantle-verify-"
	mantleVerifyPVCPrefix     = "mantle-verify-"
	mantleVerifyPVPrefix      = "mantle-verify-"
	MantleZeroOutJobPrefix    = "mantle-zeroout-"
	MantleZeroOutPVCPrefix    = "mantle-zeroout-"
	MantleZeroOutPVPrefix     = "mantle-zeroout-"

	syncModeFull        = "full"
	syncModeIncremental = "incremental"

	EnvExportJobScript = "EXPORT_JOB_SCRIPT"
	EnvUploadJobScript = "UPLOAD_JOB_SCRIPT"
	EnvImportJobScript = "IMPORT_JOB_SCRIPT"

	nonRootFSGroup = int64(10000)
	nonRootGroupID = int64(10000)
	nonRootUserID  = int64(10000)
)

var (
	//go:embed script/job-export.sh
	EmbedJobExportScript string
	//go:embed script/job-upload.sh
	EmbedJobUploadScript string
	//go:embed script/job-import.sh
	EmbedJobImportScript string
)

type ObjectStorageSettings struct {
	CACertConfigMap *string
	CACertKey       *string
	BucketName      string
	Endpoint        string
}

type ProxySettings struct {
	HttpProxy  string
	HttpsProxy string
	NoProxy    string
}

// MantleBackupReconciler reconciles a MantleBackup object.
type MantleBackupReconciler struct {
	client.Client
	Scheme                 *runtime.Scheme
	ceph                   ceph.CephCmd
	managedCephClusterID   string
	role                   string
	primarySettings        *PrimarySettings   // This should be non-nil if and only if role equals 'primary'.
	secondarySettings      *SecondarySettings // This should be non-nil if and only if role equals 'secondary'.
	expireQueueCh          chan event.GenericEvent
	podImage               string
	envSecret              string
	objectStorageSettings  *ObjectStorageSettings // This should be non-nil if and only if role equals 'primary' or 'secondary'.
	objectStorageClient    objectstorage.Bucket
	proxySettings          *ProxySettings
	backupTransferPartSize resource.Quantity
}

// NewMantleBackupReconciler returns NodeReconciler.
func NewMantleBackupReconciler(
	client client.Client,
	scheme *runtime.Scheme,
	managedCephClusterID,
	role string,
	primarySettings *PrimarySettings,
	secondarySettings *SecondarySettings,
	podImage string,
	envSecret string,
	objectStorageSettings *ObjectStorageSettings,
	proxySettings *ProxySettings,
	backupTransferPartSize resource.Quantity,
) *MantleBackupReconciler {
	return &MantleBackupReconciler{
		Client:                 client,
		Scheme:                 scheme,
		ceph:                   ceph.NewCephCmd(),
		managedCephClusterID:   managedCephClusterID,
		role:                   role,
		primarySettings:        primarySettings,
		secondarySettings:      secondarySettings,
		expireQueueCh:          make(chan event.GenericEvent),
		podImage:               podImage,
		envSecret:              envSecret,
		objectStorageSettings:  objectStorageSettings,
		proxySettings:          proxySettings,
		backupTransferPartSize: backupTransferPartSize,
	}
}

func (r *MantleBackupReconciler) removeRBDSnapshot(ctx context.Context, poolName, imageName, snapshotName string) *reconcileResult {
	logger := log.FromContext(ctx)
	var imageID string
	imageNames, err := r.ceph.RBDLs(poolName)
	if err != nil {
		return reconcileFailed("failed to list RBD images: %w", err)
	}
	if slices.Contains(imageNames, imageName) {
		info, err := r.ceph.RBDInfo(poolName, imageName)
		if err != nil {
			return reconcileFailed("failed to get RBD info: %w", err)
		}
		imageID = info.ID
	} else {
		trashList, err := r.ceph.RBDTrashLs(poolName)
		if err != nil {
			return reconcileFailed("failed to list RBD trash: %w", err)
		}
		for _, trash := range trashList {
			if trash.Name == imageName {
				imageID = trash.ID

				break
			}
		}
		// If the image is not found in both active images and trash, it means the snapshot has already been removed, so it returns nil to allow finalization to proceed without error.
		if len(imageID) == 0 {
			logger.Info("rbd image not found, assuming snapshot has already been removed", "poolName", poolName, "imageName", imageName, "snapshotName", snapshotName)

			return nil
		}
	}

	rmErr := r.ceph.RBDSnapRm(poolName, imageID, snapshotName)
	if rmErr != nil {
		snapshots, lsErr := r.ceph.RBDSnapLsByID(poolName, imageID)
		if lsErr != nil {
			return reconcileFailed("failed to ensure rbd snapshot is removed: %s/%s@%s: %w", poolName, imageID, snapshotName, errors.Join(rmErr, lsErr))
		}
		for _, snap := range snapshots {
			if snap.Name == snapshotName {
				return reconcileFailed("failed to remove rbd snapshot: %w", rmErr)
			}
		}
		logger.Info("rbd snapshot has already been removed", "poolName", poolName, "imageName", imageName, "snapshotName", snapshotName)

		return nil
	}

	return nil
}

func (r *MantleBackupReconciler) removeRBDSnapshotFromBackup(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
) *reconcileResult {
	if backup.Status.PVManifest == "" {
		return nil
	}
	pool, image, result := r.getPoolAndImageFromStatusPVManifest(backup)
	if result.shouldReturn() {
		return result.wrapIfError("failed to get pool and image from PV manifest")
	}

	return r.removeRBDSnapshot(ctx, pool, image, backup.Name)
}

func (r *MantleBackupReconciler) createRBDSnapshot(
	ctx context.Context, poolName, imageName string, backup *mantlev1.MantleBackup,
) (*ceph.RBDSnapshot, *reconcileResult) {
	logger := log.FromContext(ctx)
	createErr := r.ceph.RBDSnapCreate(poolName, imageName, backup.Name)
	snap, findErr := ceph.FindRBDSnapshot(r.ceph, poolName, imageName, backup.Name)
	if findErr != nil {
		logger.Error(errors.Join(createErr, findErr), "failed to find rbd snapshot")

		return nil, reconcileFailed("failed to find rbd snapshot: %w", errors.Join(createErr, findErr))
	}

	return snap, nil
}

func (r *MantleBackupReconciler) checkPVCBound(ctx context.Context, pvc *corev1.PersistentVolumeClaim) *reconcileResult {
	logger := log.FromContext(ctx)
	if pvc.Status.Phase != corev1.ClaimBound {
		if pvc.Status.Phase == corev1.ClaimPending {
			return reconcileRequeue()
		} else {
			logger.Info("PVC phase is neither bound nor pending", "status.phase", pvc.Status.Phase)

			return reconcileFailed("PVC phase is neither bound nor pending (status.phase: %s)", pvc.Status.Phase)
		}
	}

	return nil
}

type snapshotTarget struct {
	pvc       *corev1.PersistentVolumeClaim
	pv        *corev1.PersistentVolume
	imageName string
	poolName  string
}

func (r *MantleBackupReconciler) getSnapshotTarget(ctx context.Context, backup *mantlev1.MantleBackup) (
	*snapshotTarget,
	*reconcileResult,
) {
	logger := log.FromContext(ctx)
	pvcNamespace := backup.Namespace
	pvcName := backup.Spec.PVC
	var pvc corev1.PersistentVolumeClaim
	if err := r.Get(ctx, types.NamespacedName{Namespace: pvcNamespace, Name: pvcName}, &pvc); err != nil {
		if aerrors.IsNotFound(err) {
			return nil, reconcileRequeue()
		}

		logger.Error(err, "failed to get PVC", "namespace", pvcNamespace, "name", pvcName)

		return nil, reconcileFailed("failed to get PVC(%s/%s): %w", pvcNamespace, pvcName, err)
	}

	// Return an error if the PVC has been re-created after the first call.
	if uid, ok := backup.GetLabels()[labelLocalBackupTargetPVCUID]; ok && uid != string(pvc.GetUID()) {
		return nil, reconcileFailed("PVC UID does not match the backup target")
	}

	if result := r.checkPVCBound(ctx, &pvc); result.shouldReturn() {
		return nil, result.wrapIfError("failed to check the PVC bound")
	}

	pvName := pvc.Spec.VolumeName
	var pv corev1.PersistentVolume
	if err := r.Get(ctx, types.NamespacedName{Name: pvName}, &pv); err != nil {
		logger.Error(err, "failed to get PV", "name", pvName)

		return nil, reconcileFailed("failed to get PV: %w", err)
	}

	imageName, ok := pv.Spec.CSI.VolumeAttributes["imageName"]
	if !ok {
		return nil, reconcileFailed("failed to get imageName from PV")
	}
	poolName, ok := pv.Spec.CSI.VolumeAttributes["pool"]
	if !ok {
		return nil, reconcileFailed("failed to get pool from PV")
	}

	return &snapshotTarget{&pvc, &pv, imageName, poolName}, nil
}

// expire deletes the backup if expired, or schedules deletion via expireQueueCh if not yet.
func (r *MantleBackupReconciler) expire(ctx context.Context, backup *mantlev1.MantleBackup) *reconcileResult {
	logger := log.FromContext(ctx)
	if backup.Status.CreatedAt.IsZero() {
		// the RBD snapshot has not be taken yet, do nothing.
		return nil
	}

	if !backup.DeletionTimestamp.IsZero() {
		// DeletionTimestamp is already set; let the caller handle finalization.
		return nil
	}

	if v, ok := backup.Annotations[annotRetainIfExpired]; ok && v == "true" {
		// retain this backup.
		// If the annotation is deleted, reconciliation will run, so no need to schedule.
		return nil
	}

	expire, err := strfmt.ParseDuration(backup.Spec.Expire)
	if err != nil {
		return reconcileFailed("failed to parse expire duration: %w", err)
	}
	expireAt := backup.Status.CreatedAt.Add(expire)
	if time.Now().UTC().After(expireAt) {
		// already expired, delete it immediately.
		logger.Info("delete expired backup", "createdAt", backup.Status.CreatedAt, "expire", expire)

		if err := r.Delete(ctx, backup); err != nil && !aerrors.IsNotFound(err) {
			return reconcileFailed("failed to delete expired backup: %w", err)
		}

		return reconcileSucceeded()
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
//+kubebuilder:rbac:groups="batch",resources=jobs,verbs=get;list;watch;create;update;patch;delete

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

	if err := r.Get(ctx, req.NamespacedName, &backup); err != nil {
		if aerrors.IsNotFound(err) {
			logger.Info("MantleBackup is not found", "error", err)

			return ctrl.Result{}, nil
		}
		logger.Error(err, "failed to get MantleBackup")

		return ctrl.Result{}, err
	}

	if result := r.checkManagedBackup(ctx, &backup); result.shouldReturn() {
		return result.toCtrlResult()
	}
	logger.Info("starting reconciliation", "namespace", backup.Namespace, "name", backup.Name, "backupUID", string(backup.GetUID()))

	switch r.role {
	case RoleStandalone:
		return r.reconcileAsStandalone(ctx, &backup).toCtrlResult()
	case RolePrimary:
		return r.reconcileAsPrimary(ctx, &backup).toCtrlResult()
	case RoleSecondary:
		return r.reconcileAsSecondary(ctx, &backup).toCtrlResult()
	}

	panic("unreachable")
}

// checkManagedBackup checks if the MantleBackup resource is managed by this controller.
func (r *MantleBackupReconciler) checkManagedBackup(ctx context.Context, backup *mantlev1.MantleBackup) *reconcileResult {
	// If the MantleBackup resource has a label indicating that it is managed by this controller.
	if backup.Labels != nil {
		if clusterID, ok := backup.Labels[labelClusterID]; ok {
			if clusterID == r.managedCephClusterID {
				return nil
			}
			// The MantleBackup resource is managed by a different controller, so we return a successful reconcile result to stop further processing.
			return reconcileSucceeded()
		}
	}

	var pvc corev1.PersistentVolumeClaim
	if err := r.Get(ctx, types.NamespacedName{Namespace: backup.Namespace, Name: backup.Spec.PVC}, &pvc); err != nil {
		if aerrors.IsNotFound(err) {
			return reconcileRequeue()
		}

		return reconcileFailed("failed to get the PVC specified in the MantleBackup resource: %w", err)
	}

	clusterID, err := getCephClusterIDFromPVC(ctx, r.Client, &pvc)
	if err != nil {
		return reconcileFailed("failed to get the Ceph cluster ID from the PVC: %w", err)
	}

	// If the cluster ID of the PVC does not match the managed Ceph cluster ID, it means that the PVC is not managed by this controller.
	// In this case, we return a successful reconcile result to stop further processing.
	if clusterID != r.managedCephClusterID {
		return reconcileSucceeded()
	}

	return nil
}

func (r *MantleBackupReconciler) reconcileLocalBackup(ctx context.Context, backup *mantlev1.MantleBackup) *reconcileResult {
	logger := log.FromContext(ctx)

	if isCreatedWhenMantleControllerWasSecondary(backup) {
		logger.Info(
			"skipping to reconcile the MantleBackup created by a remote mantle-controller to prevent accidental data loss",
		)

		return reconcileSucceeded()
	}

	if result := r.expire(ctx, backup); result.shouldReturn() {
		return result.wrapIfError("failed to expire backup")
	}

	if !backup.DeletionTimestamp.IsZero() {
		return r.finalizeStandalone(ctx, backup)
	}

	target, result := r.getSnapshotTarget(ctx, backup)
	if result.shouldReturn() {
		return result.wrapIfError("failed to get snapshot target")
	}

	updated := false
	if backup.Labels == nil {
		backup.Labels = make(map[string]string)
	}
	if _, ok := backup.Labels[labelClusterID]; !ok {
		backup.Labels[labelClusterID] = r.managedCephClusterID
		updated = true
	}
	// Attach local-backup-target-pvc-uid label before trying to create a RBD
	// snapshot corresponding to the given MantleBackup, so that we can make
	// sure that every MantleBackup that has a RBD snapshot is labelled with
	// local-backup-target-pvc-uid.
	if _, ok := backup.Labels[labelLocalBackupTargetPVCUID]; !ok {
		backup.Labels[labelLocalBackupTargetPVCUID] = string(target.pvc.GetUID())
		updated = true
	}
	if controllerutil.AddFinalizer(backup, MantleBackupFinalizerName) {
		updated = true
	}

	if updated {
		if err := r.Update(ctx, backup); err != nil {
			logger.Error(err, "failed to update MantleBackup")

			return reconcileFailed("failed to update MantleBackup: %w", err)
		}
	}

	if !backup.IsSnapshotCaptured() {
		if result := r.provisionRBDSnapshot(ctx, backup, target); result.shouldReturn() {
			return result.wrapIfError("failed to provision RBD snapshot")
		}
	}

	return nil
}

func (r *MantleBackupReconciler) reconcileAsStandalone(ctx context.Context, backup *mantlev1.MantleBackup) *reconcileResult {
	if result := r.reconcileLocalBackup(ctx, backup); result.shouldReturn() {
		return result.wrapIfError("failed to reconcile local backup")
	}

	if !backup.DeletionTimestamp.IsZero() {
		return reconcileSucceeded()
	}

	if !backup.IsVerifiedTrue() && !backup.IsVerifiedFalse() {
		if result := r.verify(ctx, backup); result.shouldReturn() {
			return result.wrapIfError("failed to verify backup")
		}

		return reconcileRequeue()
	}

	return r.primaryCleanup(ctx, backup)
}

func (r *MantleBackupReconciler) reconcileAsPrimary(ctx context.Context, backup *mantlev1.MantleBackup) *reconcileResult {
	if result := r.reconcileLocalBackup(ctx, backup); result.shouldReturn() {
		return result.wrapIfError("failed to reconcile local backup")
	}

	if !backup.DeletionTimestamp.IsZero() {
		return reconcileSucceeded()
	}

	if !backup.IsVerifiedTrue() && !backup.IsVerifiedFalse() {
		if result := r.verify(ctx, backup); result.shouldReturn() {
			return result.wrapIfError("failed to verify backup")
		}
	}

	if !backup.IsSynced() {
		if result := r.replicate(ctx, backup); result.shouldReturn() {
			return result.wrapIfError("failed to replicate backup")
		}
	}

	if (!backup.IsVerifiedTrue() && !backup.IsVerifiedFalse()) || !backup.IsSynced() {
		return reconcileRequeue()
	}

	return r.primaryCleanup(ctx, backup)
}

func (r *MantleBackupReconciler) reconcileAsSecondary(ctx context.Context, backup *mantlev1.MantleBackup) *reconcileResult {
	logger := log.FromContext(ctx)

	if result := r.prepareObjectStorageClient(ctx); result.shouldReturn() {
		return result.wrapIfError("failed to prepare object storage client")
	}

	if !isCreatedWhenMantleControllerWasSecondary(backup) {
		logger.Info(
			"skipping to reconcile the MantleBackup created by a different mantle-controller to prevent accidental data loss",
		)

		return reconcileSucceeded()
	}

	if result := r.expire(ctx, backup); result.shouldReturn() {
		return result.wrapIfError("failed to expire backup")
	}

	if !backup.DeletionTimestamp.IsZero() {
		return r.finalizeSecondary(ctx, backup)
	}

	target, result := r.getSnapshotTarget(ctx, backup)
	if result.shouldReturn() {
		return result.wrapIfError("failed to get snapshot target")
	}

	if !backup.IsSnapshotCaptured() {
		if result := r.startImport(ctx, backup, target); result.shouldReturn() {
			return result.wrapIfError("failed to start import")
		}
	}

	if backup.IsSnapshotCaptured() && !backup.IsVerifiedTrue() && !backup.IsVerifiedFalse() {
		if result := r.verify(ctx, backup); result.shouldReturn() {
			return result.wrapIfError("failed to verify backup")
		}

		return reconcileRequeue()
	}

	return r.secondaryCleanup(ctx, backup, true)
}

func scheduleExpire(_ context.Context, evt event.TypedGenericEvent[client.Object], q workqueue.TypedRateLimitingInterface[ctrl.Request]) {
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

func newMantleBackupReconcilerRateLimiter[T comparable]() workqueue.TypedRateLimiter[T] {
	// Use 3 minutes as the max delay for the exponential backoff to speed backups up.
	// The rest is the same as the default rate limiter.
	// cf. https://github.com/kubernetes/client-go/blob/18a1faa115ed571de5af3e8f0f9c02973769ceb3/util/workqueue/default_rate_limiters.go#L50-L56
	return workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[T](5*time.Millisecond, 3*time.Minute),
		&workqueue.TypedBucketRateLimiter[T]{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
	)
}

// SetupWithManager sets up the controller with the Manager.
func (r *MantleBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Ensure a zero-value metric is exported even if no MantleBackup exists.
	metrics.BackupDurationSeconds.With(prometheus.Labels{
		"persistentvolumeclaim": "",
		"resource_namespace":    "",
	})

	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.TypedOptions[reconcile.Request]{
			RateLimiter: newMantleBackupReconcilerRateLimiter[reconcile.Request](),
		}).
		For(&mantlev1.MantleBackup{}).
		WatchesRawSource(
			source.TypedChannel(r.expireQueueCh, handler.Funcs{GenericFunc: scheduleExpire}),
		).
		Complete(r)
}

func (r *MantleBackupReconciler) replicate(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
) *reconcileResult {
	logger := log.FromContext(ctx)

	if result := r.replicateManifests(ctx, backup); result.shouldReturn() {
		return result.wrapIfError("failed to replicate manifests")
	}
	prepareResult, result := r.prepareForDataSynchronization(ctx, backup, r.primarySettings.Client)
	if result.shouldReturn() {
		return result.wrapIfError("failed to prepare for data synchronization")
	}

	if prepareResult.isSecondaryMantleBackupSnapshotCaptured {
		if result := r.updateMantleBackupCondition(
			ctx, backup,
			mantlev1.BackupConditionSyncedToRemote,
			metav1.ConditionTrue,
			mantlev1.ConditionReasonSyncedToRemoteNoProblem,
		); result.shouldReturn() {
			return result.wrapIfError("failed to set SyncedToRemote condition to True")
		}
		logger.Info("succeeded to sync a backup to the remote ceph cluster")

		duration := time.Since(backup.GetCreationTimestamp().Time).Seconds()
		metrics.BackupDurationSeconds.With(prometheus.Labels{
			"persistentvolumeclaim": backup.Spec.PVC,
			"resource_namespace":    backup.GetNamespace(),
		}).Observe(duration)

		return reconcileRequeue()
	}

	return r.startExportAndUpload(ctx, backup, prepareResult)
}

func (r *MantleBackupReconciler) replicateManifests(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
) *reconcileResult {
	// Unmarshal the PVC manifest stored in the status of the MantleBackup resource.
	var pvc corev1.PersistentVolumeClaim
	if err := json.Unmarshal([]byte(backup.Status.PVCManifest), &pvc); err != nil {
		return reconcileFailed("failed to unmarshal the PVC stored in the status of the MantleBackup resource: %w", err)
	}

	// Make sure the arguments are valid
	if backup.Status.SnapID == nil {
		return reconcileFailed("backup.Status.SnapID should not be nil: %s: %s", backup.GetName(), backup.GetNamespace())
	}

	// Make sure all of the preceding backups for the same PVC have already been replicated.
	var backupList mantlev1.MantleBackupList
	if err := r.List(ctx, &backupList, &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(map[string]string{labelLocalBackupTargetPVCUID: string(pvc.GetUID())}),
	}); err != nil {
		return reconcileFailed("failed to list MantleBackup resources for the same PVC: %w", err)
	}
	for _, backup1 := range backupList.Items {
		if backup1.Status.SnapID == nil ||
			*backup1.Status.SnapID < *backup.Status.SnapID &&
				backup1.DeletionTimestamp.IsZero() &&
				!backup1.IsSynced() {
			return reconcileRequeue()
		}
	}

	// Create a PVC that should be sent to the secondary mantle.
	var pvcSent corev1.PersistentVolumeClaim
	pvcSent.SetName(pvc.GetName())
	pvcSent.SetNamespace(pvc.GetNamespace())
	pvcSent.SetAnnotations(map[string]string{
		annotRemoteUID: string(pvc.GetUID()),
	})
	pvcSent.Spec = *pvc.Spec.DeepCopy()
	capacity, err := resource.ParseQuantity(strconv.FormatInt(*backup.Status.SnapSize, 10))
	if err != nil {
		return reconcileFailed("failed to parse quantity: %w", err)
	}
	pvcSent.Spec.Resources = corev1.VolumeResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceStorage: capacity,
		},
	}
	pvcSentJson, err := json.Marshal(pvcSent)
	if err != nil {
		return reconcileFailed("failed to marshal the PVC to be sent to the secondary mantle: %w", err)
	}

	// Call CreateOrUpdatePVC
	client := r.primarySettings.Client
	resp, err := client.CreateOrUpdatePVC(
		ctx,
		&proto.CreateOrUpdatePVCRequest{
			Pvc: pvcSentJson,
		},
	)
	if err != nil {
		return reconcileFailed("failed to call CreateOrUpdatePVC: %w", err)
	}

	// Create a MantleBackup that should be sent to the secondary mantle.
	var backupSent mantlev1.MantleBackup
	backupSent.SetName(backup.GetName())
	backupSent.SetNamespace(backup.GetNamespace())
	backupSent.SetAnnotations(map[string]string{
		annotRemoteUID: string(backup.GetUID()),
	})
	backupSent.SetLabels(map[string]string{
		labelLocalBackupTargetPVCUID:  resp.GetUid(),
		labelRemoteBackupTargetPVCUID: string(pvc.GetUID()),
	})
	backupSent.SetFinalizers([]string{MantleBackupFinalizerName})
	backupSent.Spec = backup.Spec
	backupSent.Status.CreatedAt = backup.Status.CreatedAt
	backupSent.Status.SnapSize = backup.Status.SnapSize
	backupSent.Status.TransferPartSize = backup.Status.TransferPartSize
	backupSentJson, err := json.Marshal(backupSent)
	if err != nil {
		return reconcileFailed("failed to marshal the MantleBackup to be sent to the secondary mantle: %w", err)
	}

	// Call CreateMantleBackup.
	if _, err := client.CreateMantleBackup(
		ctx,
		&proto.CreateMantleBackupRequest{
			MantleBackup: backupSentJson,
		},
	); err != nil {
		return reconcileFailed("failed to call CreateMantleBackup: %w", err)
	}

	return nil
}

func (r *MantleBackupReconciler) verify(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
) *reconcileResult {
	logger := log.FromContext(ctx)
	logger.Info("starting verification reconciliation", "backupUID", string(backup.GetUID()))

	var storedPVC corev1.PersistentVolumeClaim
	if err := json.Unmarshal([]byte(backup.Status.PVCManifest), &storedPVC); err != nil {
		return reconcileFailed("failed to unmarshal PVC manifest: %w", err)
	}

	var storedPV corev1.PersistentVolume
	if err := json.Unmarshal([]byte(backup.Status.PVManifest), &storedPV); err != nil {
		return reconcileFailed("failed to unmarshal PV manifest: %w", err)
	}

	// create a clone by the snapshot bound to the MB
	if err := createCloneByPV(ctx, r.ceph, &storedPV, backup.Name, MakeVerifyImageName(backup)); err != nil {
		return reconcileFailed("failed to create a clone by the snapshot: %w", err)
	}

	// create a static PV with the clone
	if result := r.createStaticPVIfNotExists(
		ctx,
		&storedPV,
		MakeVerifyImageName(backup),
		corev1.ResourceList{
			corev1.ResourceStorage: *resource.NewQuantity(*backup.Status.SnapSize, resource.BinarySI),
		},
		MakeVerifyPVName(backup),
		MakeVerifyPVCName(backup),
		labelComponentVerifyVolume,
		true,
	); result.shouldReturn() {
		return result.wrapIfError("failed to create a static PV with the clone")
	}

	// create a PVC with the PV
	if result := r.createStaticPVCIfNotExists(
		ctx,
		MakeVerifyPVCName(backup),
		MakeVerifyPVName(backup),
		labelComponentVerifyVolume,
		storedPVC.Spec.Resources,
	); result.shouldReturn() {
		return result.wrapIfError("failed to create a PVC with the PV")
	}

	// create a Job to execute e2fsck on the PVC
	if result := r.createOrUpdateVerifyJob(
		ctx,
		MakeVerifyJobName(backup),
		MakeVerifyPVCName(backup),
	); result.shouldReturn() {
		return result.wrapIfError("failed to create a Job to execute e2fsck on the PVC")
	}

	// wait for the Job to complete
	jobFinished, jobSucceeded, result := r.checkJobStatus(ctx, MakeVerifyJobName(backup))
	if result.shouldReturn() {
		return result.wrapIfError("failed to check the Job status")
	}
	if !jobFinished {
		// still running
		return nil
	}

	// update MB's conditions field
	condition := metav1.ConditionTrue
	reason := mantlev1.ConditionReasonVerifiedSuccess
	if !jobSucceeded {
		condition = metav1.ConditionFalse
		reason = mantlev1.ConditionReasonVerifiedFailed
	}
	if result := r.updateMantleBackupCondition(
		ctx, backup,
		mantlev1.BackupConditionVerified,
		condition,
		reason,
	); result.shouldReturn() {
		return result.wrapIfError("failed to update MantleBackup condition")
	}

	return nil
}

// checkJobStatus checks the status of the Job with the given name.
// It returns (is job finished, is job succeeded, reconcileResult).
func (r *MantleBackupReconciler) checkJobStatus(ctx context.Context, jobName string) (bool, bool, *reconcileResult) {
	var job batchv1.Job
	if err := r.Get(
		ctx,
		types.NamespacedName{
			Namespace: r.managedCephClusterID,
			Name:      jobName,
		},
		&job,
	); err != nil {
		if aerrors.IsNotFound(err) {
			// The cache must be stale.
			return false, false, nil
		}

		return false, false, reconcileFailed("failed to get Job for checking job status: %w", err)
	}

	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true, true, nil
		}
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true, false, nil
		}
	}

	// job is still running
	return false, false, nil
}

func (r *MantleBackupReconciler) provisionRBDSnapshot(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	target *snapshotTarget,
) *reconcileResult {
	logger := log.FromContext(ctx)

	// Attach local-backup-target-pvc-uid label before trying to create a RBD
	// snapshot corresponding to the given MantleBackup, so that we can make
	// sure that every MantleBackup that has a RBD snapshot is labelled with
	// local-backup-target-pvc-uid.
	key := client.ObjectKeyFromObject(backup)
	if err := r.Get(ctx, key, backup); err != nil {
		return reconcileFailed("failed to get MantleBackup: %w", err)
	}
	if backup.Labels == nil {
		backup.Labels = map[string]string{}
	}
	backup.Labels[labelLocalBackupTargetPVCUID] = string(target.pvc.GetUID())
	if err := r.Update(ctx, backup); err != nil {
		return reconcileFailed("failed to update MantleBackup: %w", err)
	}

	// Save PVManifest and PVCManifest before creating the RBD snapshot.
	// This guarantees that removeRBDSnapshotFromBackup can locate and remove
	// the snapshot even if the controller crashes after createRBDSnapshot but
	// before the subsequent updateStatus that saves SnapID.
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

		return nil
	}); err != nil {
		logger.Error(err, "failed to update MantleBackup status", "status", backup.Status)

		return reconcileFailed("failed to update MantleBackup status: %w", err)
	}

	snapshot, result := r.createRBDSnapshot(ctx, target.poolName, target.imageName, backup)
	if result.shouldReturn() {
		return result.wrapIfError("failed to create RBD snapshot")
	}

	if err := updateStatus(ctx, r.Client, backup, func() error {
		backup.Status.SnapID = &snapshot.Id
		backup.Status.CreatedAt = metav1.NewTime(snapshot.Timestamp.Time)
		backup.Status.SnapSize = &snapshot.Size

		if backup.Status.TransferPartSize == nil {
			// .status.transferPartSize isn't necessary in the standalone mode,
			// but its value must be set before CreateMantleBackup RPC, so we
			// set it here.
			backup.Status.TransferPartSize = &r.backupTransferPartSize
		}

		meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
			Type: mantlev1.BackupConditionSnapshotCaptured, Status: metav1.ConditionTrue, Reason: mantlev1.ConditionReasonSnapshotCapturedNoProblem})

		return nil
	}); err != nil {
		logger.Error(err, "failed to update MantleBackup status", "status", backup.Status)

		return reconcileFailed("failed to update MantleBackup status: %w", err)
	}
	logger.Info("succeeded to create a backup")

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
) *reconcileResult {
	logger := log.FromContext(ctx)
	if _, ok := backup.GetAnnotations()[annotDiffTo]; ok {
		return nil
	}

	if !controllerutil.ContainsFinalizer(backup, MantleBackupFinalizerName) {
		return nil
	}

	// primaryClean() is called in finalizeStandalone() to delete resources for
	// exported and uploaded snapshots in both standalone and primary Mantle.
	if result := r.primaryCleanup(ctx, backup); result.shouldReturn() {
		return result.wrapIfError("failed to cleanup resources for exported and uploaded snapshots")
	}

	if result := r.removeRBDSnapshotFromBackup(ctx, backup); result.shouldReturn() {
		return result.wrapIfError("failed to remove RBD snapshot from backup")
	}

	controllerutil.RemoveFinalizer(backup, MantleBackupFinalizerName)
	if err := r.Update(ctx, backup); err != nil {
		logger.Error(err, "failed to remove finalizer", "finalizer", MantleBackupFinalizerName)

		return reconcileFailed("failed to remove finalizer: %w", err)
	}
	logger.Info("succeeded to delete a backup")

	return nil
}

func (r *MantleBackupReconciler) finalizeSecondary(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
) *reconcileResult {
	logger := log.FromContext(ctx)
	if _, ok := backup.GetAnnotations()[annotDiffTo]; ok {
		return nil
	}

	if !controllerutil.ContainsFinalizer(backup, MantleBackupFinalizerName) {
		return nil
	}

	if result := r.secondaryCleanup(ctx, backup, false); result.shouldReturn() {
		return result.wrapIfError("failed to cleanup resources for exported and uploaded snapshots")
	}

	if result := r.removeRBDSnapshotFromBackup(ctx, backup); result.shouldReturn() {
		return result.wrapIfError("failed to remove RBD snapshot from backup")
	}

	controllerutil.RemoveFinalizer(backup, MantleBackupFinalizerName)
	if err := r.Update(ctx, backup); err != nil {
		logger.Error(err, "failed to remove finalizer", "finalizer", MantleBackupFinalizerName)

		return reconcileFailed("failed to remove finalizer: %w", err)
	}

	return nil
}

type dataSyncPrepareResult struct {
	isIncremental                           bool // NOTE: The value is forcibly set to false if isSecondaryMantleBackupSnapshotCaptured is true.
	isSecondaryMantleBackupSnapshotCaptured bool
	diffFrom                                *mantlev1.MantleBackup // non-nil value iff isIncremental is true.
}

func (r *MantleBackupReconciler) prepareForDataSynchronization(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	msc proto.MantleServiceClient,
) (*dataSyncPrepareResult, *reconcileResult) {
	exportTargetPVCUID, ok := backup.GetLabels()[labelLocalBackupTargetPVCUID]
	if !ok {
		return nil, reconcileFailed(`"%s" label is missing`, labelLocalBackupTargetPVCUID)
	}
	resp, err := msc.ListMantleBackup(
		ctx,
		&proto.ListMantleBackupRequest{
			PvcUID:    exportTargetPVCUID,
			Namespace: backup.GetNamespace(),
		},
	)
	if err != nil {
		return nil, reconcileFailed("failed to list MantleBackup in the secondary mantle: %w", err)
	}

	secondaryBackups := make([]mantlev1.MantleBackup, 0)
	if err = json.Unmarshal(resp.GetMantleBackupList(), &secondaryBackups); err != nil {
		return nil, reconcileFailed("failed to unmarshal the MantleBackup list: %w", err)
	}
	secondaryBackupMap := convertToMap(secondaryBackups)

	secondaryBackup, ok := secondaryBackupMap[backup.GetName()]
	if !ok {
		return nil, reconcileFailed("secondary MantleBackup not found: %s, %s",
			backup.GetName(), backup.GetNamespace())
	}
	isSecondaryMantleBackupSnapshotCaptured := secondaryBackup.IsSnapshotCaptured()

	if isSecondaryMantleBackupSnapshotCaptured {
		return &dataSyncPrepareResult{
			isIncremental:                           false,
			isSecondaryMantleBackupSnapshotCaptured: true,
			diffFrom:                                nil,
		}, nil
	}

	if syncMode, ok := backup.GetAnnotations()[annotSyncMode]; ok {
		switch syncMode {
		case syncModeFull:
			return &dataSyncPrepareResult{
				isIncremental:                           false,
				isSecondaryMantleBackupSnapshotCaptured: isSecondaryMantleBackupSnapshotCaptured,
				diffFrom:                                nil,
			}, nil
		case syncModeIncremental:
			diffFromName, ok := backup.GetAnnotations()[annotDiffFrom]
			if !ok {
				return nil, reconcileFailed(`"%s" annotation is missing`, annotDiffFrom)
			}

			var diffFrom mantlev1.MantleBackup
			if err := r.Get(ctx, types.NamespacedName{
				Name:      diffFromName,
				Namespace: backup.GetNamespace(),
			}, &diffFrom); err != nil {
				return nil, reconcileFailed("failed to get diff-from MantleBackup: %w", err)
			}

			return &dataSyncPrepareResult{
				isIncremental:                           true,
				isSecondaryMantleBackupSnapshotCaptured: isSecondaryMantleBackupSnapshotCaptured,
				diffFrom:                                &diffFrom,
			}, nil
		default:
			return nil, reconcileFailed("unknown sync mode: %s", syncMode)
		}
	}

	var primaryBackupList mantlev1.MantleBackupList
	// TODO: Perhaps, we may have to use the client without cache.
	if err := r.List(ctx, &primaryBackupList, &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(map[string]string{labelLocalBackupTargetPVCUID: exportTargetPVCUID}),
		Namespace:     backup.GetNamespace(),
	}); err != nil {
		return nil, reconcileFailed("failed to list primary MantleBackup: %w", err)
	}

	diffFrom := searchForDiffOriginMantleBackup(backup, primaryBackupList.Items, secondaryBackupMap)
	isIncremental := (diffFrom != nil)

	return &dataSyncPrepareResult{
		isIncremental:                           isIncremental,
		isSecondaryMantleBackupSnapshotCaptured: isSecondaryMantleBackupSnapshotCaptured,
		diffFrom:                                diffFrom,
	}, nil
}

func convertToMap(mantleBackups []mantlev1.MantleBackup) map[string]*mantlev1.MantleBackup {
	m := make(map[string]*mantlev1.MantleBackup)
	for _, mantleBackup := range mantleBackups {
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
		secondaryBackup, ok := secondaryBackupMap[primaryBackup.Name]
		if !ok {
			continue
		}
		if !primaryBackup.IsSnapshotCaptured() || !secondaryBackup.IsSnapshotCaptured() {
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

func (r *MantleBackupReconciler) startExportAndUpload(
	ctx context.Context,
	targetBackup *mantlev1.MantleBackup,
	prepareResult *dataSyncPrepareResult,
) *reconcileResult {
	sourceBackup := prepareResult.diffFrom
	var sourceBackupName *string
	if sourceBackup != nil {
		s := sourceBackup.GetName()
		sourceBackupName = &s
	}

	if result := r.annotateExportTargetMantleBackup(
		ctx, targetBackup, prepareResult.isIncremental, sourceBackupName,
	); result.shouldReturn() {
		return result.wrapIfError("failed to annotate export target MantleBackup")
	}

	if prepareResult.isIncremental {
		if result := r.annotateExportSourceMantleBackup(ctx, sourceBackup, targetBackup); result.shouldReturn() {
			return result.wrapIfError("failed to annotate export source MantleBackup")
		}
	}

	if _, err := r.primarySettings.Client.SetSynchronizing(
		ctx,
		&proto.SetSynchronizingRequest{
			Name:      targetBackup.GetName(),
			Namespace: targetBackup.GetNamespace(),
			DiffFrom:  sourceBackupName,
		},
	); err != nil {
		return reconcileFailed("failed to call SetSynchronizing RPC: %w", err)
	}

	largestCompletedExportPartNum, result := r.startExport(ctx, targetBackup, sourceBackupName)
	if result.shouldReturn() {
		return result.wrapIfError("failed to export")
	}

	if result := r.startUpload(ctx, targetBackup, largestCompletedExportPartNum); result.shouldReturn() {
		return result.wrapIfError("failed to upload MantleBackup")
	}

	return reconcileRequeue()
}

func (r *MantleBackupReconciler) annotateExportTargetMantleBackup(
	ctx context.Context,
	target *mantlev1.MantleBackup,
	incremental bool,
	sourceName *string,
) *reconcileResult {
	key := client.ObjectKeyFromObject(target)
	if err := r.Get(ctx, key, target); err != nil {
		return reconcileFailed("failed to get export target MantleBackup: %w", err)
	}

	annot := target.GetAnnotations()
	if annot == nil {
		annot = map[string]string{}
	}
	if incremental {
		annot[annotSyncMode] = syncModeIncremental
		annot[annotDiffFrom] = *sourceName
	} else {
		annot[annotSyncMode] = syncModeFull
	}
	target.SetAnnotations(annot)

	if err := r.Update(ctx, target); err != nil {
		return reconcileFailed("failed to update export target MantleBackup: %w", err)
	}

	return nil
}

func (r *MantleBackupReconciler) annotateExportSourceMantleBackup(
	ctx context.Context,
	source *mantlev1.MantleBackup,
	target *mantlev1.MantleBackup,
) *reconcileResult {
	key := client.ObjectKeyFromObject(source)
	if err := r.Get(ctx, key, source); err != nil {
		return reconcileFailed("failed to get export source MantleBackup: %w", err)
	}

	annot := source.GetAnnotations()
	if annot == nil {
		annot = map[string]string{}
	}
	annot[annotDiffTo] = target.GetName()
	source.SetAnnotations(annot)

	if err := r.Update(ctx, source); err != nil {
		return reconcileFailed("failed to update export source MantleBackup: %w", err)
	}

	return nil
}

// startExport reconciles export Jobs and PVCs. Note that it might update
// `targetBackup`. Because of this, the values you get from r.Client.Get could
// be outdated due to its stale cache. Therefore, after we call this function,
// we MUST use `targetBackup` directly and MUST NOT use r.Client.Get (nor
// functions like ctrl.CreateOrUpdate which use Get inside) to fetch its values,
// as they might not be current.
func (r *MantleBackupReconciler) startExport(
	ctx context.Context,
	targetBackup *mantlev1.MantleBackup,
	sourceBackupName *string,
) (int, *reconcileResult) {
	largestCompletedPartNum, result := r.handleCompletedExportJobs(ctx, targetBackup)
	if result.shouldReturn() {
		return -1, result.wrapIfError("failed to handle completed export jobs")
	}

	if ok, result := r.canNewExportJobBeCreated(ctx); result.shouldReturn() {
		return -1, result.wrapIfError("failed to check if a new export Job can be created")
	} else if !ok {
		// skip to create an export Job
		return largestCompletedPartNum, nil
	}

	if ok, result := r.haveAllExportJobsCompleted(targetBackup, largestCompletedPartNum); result.shouldReturn() {
		return -1, result.wrapIfError("failed to check if all export Jobs are completed")
	} else if ok {
		return largestCompletedPartNum, nil
	}

	if result := r.createOrUpdateExportDataPVC(ctx, targetBackup, largestCompletedPartNum); result.shouldReturn() {
		return -1, result.wrapIfError("failed to create or update export data PVC")
	}

	if result := r.createOrUpdateExportJob(ctx, targetBackup, sourceBackupName, largestCompletedPartNum); result.shouldReturn() {
		return -1, result.wrapIfError("failed to create or update export Job")
	}

	return largestCompletedPartNum, nil
}

// handleCompletedJobsOfComponent checks completed {export,upload,import} Jobs and
// returns the latest completed part number. It also deletes the completed Jobs
// other than the latest one.
func (r *MantleBackupReconciler) handleCompletedJobsOfComponent(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	componentLabel string,
	componentPrefix string,
	hookPostJobDeletion *func(partNum int) error,
) (int, *reconcileResult) {
	// List all the Jobs
	var jobList batchv1.JobList
	if err := r.List(ctx, &jobList, &client.ListOptions{
		Namespace: r.managedCephClusterID,
		LabelSelector: labels.SelectorFromSet(map[string]string{
			"app.kubernetes.io/name":      labelAppNameValue,
			"app.kubernetes.io/component": componentLabel,
		}),
	}); err != nil {
		return -1, reconcileFailed("failed to list Jobs: %w", err)
	}

	// Collect the completed Jobs having the correct prefix.
	type CompletedJob struct {
		job     batchv1.Job
		partNum int
	}
	completedJobs := []*CompletedJob{}
	largestPartNum := -1
	for _, job := range jobList.Items {
		if !IsJobConditionTrue(job.Status.Conditions, batchv1.JobComplete) {
			continue
		}

		partNum, ok := ExtractPartNumFromComponentJobName(componentPrefix, job.GetName(), backup)
		if !ok {
			continue
		}

		completedJobs = append(completedJobs, &CompletedJob{
			job:     job,
			partNum: partNum,
		})

		largestPartNum = max(largestPartNum, partNum)
	}

	// Delete the completed Jobs other than the latest one
	for _, job := range completedJobs {
		if job.partNum == largestPartNum {
			continue
		}

		if err := r.Delete(ctx, &job.job, &client.DeleteOptions{
			Preconditions: &metav1.Preconditions{
				UID:             &job.job.UID,
				ResourceVersion: &job.job.ResourceVersion,
			},
			PropagationPolicy: ptr.To(metav1.DeletePropagationBackground),
		}); err != nil {
			return -1, reconcileFailed("failed to delete Job: %s: %w", job.job.GetName(), err)
		}

		if hookPostJobDeletion != nil {
			if err := (*hookPostJobDeletion)(job.partNum); err != nil {
				return -1, reconcileFailed("hookPostJobDeletion failed: %w", err)
			}
		}
	}

	return largestPartNum, nil
}

func IsPartNextToLargestCompletedPart(largestCompletedPartNum *int, partNum int) bool {
	return (largestCompletedPartNum == nil && partNum != 0) ||
		(largestCompletedPartNum != nil && partNum != *largestCompletedPartNum+1)
}

func (r *MantleBackupReconciler) handleCompletedExportJobs(ctx context.Context, backup *mantlev1.MantleBackup) (int, *reconcileResult) {
	return r.handleCompletedJobsOfComponent(ctx, backup, labelComponentExportJob, MantleExportJobPrefix, nil)
}

func (r *MantleBackupReconciler) canNewJobBeCreated(ctx context.Context, maxJobs int, component string) (bool, *reconcileResult) {
	if maxJobs == 0 {
		return true, nil
	}

	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, &client.ListOptions{
		Namespace: r.managedCephClusterID,
		LabelSelector: labels.SelectorFromSet(map[string]string{
			"app.kubernetes.io/name":      labelAppNameValue,
			"app.kubernetes.io/component": component,
		}),
	}); err != nil {
		return false, reconcileFailed("failed to list %s Jobs: %w", component, err)
	}

	// exclude completed jobs
	count := 0
	for _, job := range jobs.Items {
		if !IsJobConditionTrue(job.Status.Conditions, batchv1.JobComplete) {
			count++
		}
	}

	return count < maxJobs, nil
}

func (r *MantleBackupReconciler) canNewExportJobBeCreated(ctx context.Context) (bool, *reconcileResult) {
	return r.canNewJobBeCreated(ctx, r.primarySettings.MaxExportJobs, labelComponentExportJob)
}

func (r *MantleBackupReconciler) canNewImportJobBeCreated(ctx context.Context) (bool, *reconcileResult) {
	if r.secondarySettings == nil {
		return true, nil
	}

	return r.canNewJobBeCreated(ctx, r.secondarySettings.MaxImportJobs, labelComponentImportJob)
}

func (r *MantleBackupReconciler) getPartNumRangeOfExpectedRunningUploadJobs(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	exportedPartNum,
	uploadedPartNum int,
) (int, int, *reconcileResult) {
	if r.primarySettings.MaxUploadJobs == 0 {
		return uploadedPartNum + 1, exportedPartNum, nil
	}

	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, &client.ListOptions{
		Namespace: r.managedCephClusterID,
		LabelSelector: labels.SelectorFromSet(map[string]string{
			"app.kubernetes.io/name":      labelAppNameValue,
			"app.kubernetes.io/component": labelComponentUploadJob,
		}),
	}); err != nil {
		return 0, 0, reconcileFailed("failed to list upload Jobs: %w", err)
	}

	// Count not completed upload Jobs that are NOT related to the backup.
	count := 0
	for _, job := range jobs.Items {
		if IsJobConditionTrue(job.Status.Conditions, batchv1.JobComplete) {
			continue
		}
		_, ok := ExtractPartNumFromUploadJobName(job.GetName(), backup)
		if ok {
			continue
		}
		count++
	}

	throttle := max(0, r.primarySettings.MaxUploadJobs-count)

	return uploadedPartNum + 1, min(exportedPartNum, uploadedPartNum+throttle), nil
}

func (r *MantleBackupReconciler) getPoolAndImageFromStatusPVManifest(backup *mantlev1.MantleBackup) (string, string, *reconcileResult) {
	var pv corev1.PersistentVolume
	if err := json.Unmarshal([]byte(backup.Status.PVManifest), &pv); err != nil {
		return "", "", reconcileFailed("failed to unmarshal status.PVManifest: %w", err)
	}

	return pv.Spec.CSI.VolumeAttributes["pool"], pv.Spec.CSI.VolumeAttributes["imageName"], nil
}

func (r *MantleBackupReconciler) getNumberOfParts(backup *mantlev1.MantleBackup) (int, *reconcileResult) {
	if backup.Status.SnapSize == nil {
		return 0, reconcileFailed("failed to get status.snapSize: %s/%s", backup.GetNamespace(), backup.GetName())
	}
	if backup.Status.TransferPartSize == nil {
		return 0, reconcileFailed("failed to get status.transferPartSize: %s/%s", backup.GetNamespace(), backup.GetName())
	}

	transferPartSize, ok := backup.Status.TransferPartSize.AsInt64()
	if !ok {
		return 0, reconcileFailed("failed to convert transferPartSize to int64: %s/%s: %s",
			backup.GetNamespace(), backup.GetName(), backup.Status.TransferPartSize.String())
	}

	numParts := *backup.Status.SnapSize / transferPartSize
	if *backup.Status.SnapSize%transferPartSize != 0 {
		numParts++
	}

	return int(numParts), nil
}

func (r *MantleBackupReconciler) haveAllExportJobsCompleted(backup *mantlev1.MantleBackup, largestCompletedPartNum int) (bool, *reconcileResult) {
	limit, result := r.getNumberOfParts(backup)
	if result.shouldReturn() {
		return false, result.wrapIfError("failed to get the number of the parts of the exported data")
	}

	return largestCompletedPartNum+1 == limit, nil
}

func (r *MantleBackupReconciler) startUpload(ctx context.Context, targetBackup *mantlev1.MantleBackup, largestCompletedExportPartNum int) *reconcileResult {
	largestCompletedUploadPartNum, result := r.handleCompletedUploadJobs(ctx, targetBackup)
	if result.shouldReturn() {
		return result.wrapIfError("failed to handle completed upload jobs")
	}

	if result := r.createOrUpdateUploadJobs(
		ctx,
		targetBackup,
		largestCompletedExportPartNum,
		largestCompletedUploadPartNum,
	); result.shouldReturn() {
		return result.wrapIfError("failed to create or update upload jobs")
	}

	return nil
}

func (r *MantleBackupReconciler) handleCompletedUploadJobs(ctx context.Context, backup *mantlev1.MantleBackup) (int, *reconcileResult) {
	hook := func(partNum int) error {
		pvc := corev1.PersistentVolumeClaim{}
		pvc.SetName(MakeExportDataPVCName(backup, partNum))
		pvc.SetNamespace(r.managedCephClusterID)
		if err := r.Delete(ctx, &pvc); err != nil && !aerrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete export data PVC: %s/%s: %w", pvc.GetNamespace(), pvc.GetName(), err)
		}

		return nil
	}

	return r.handleCompletedJobsOfComponent(
		ctx,
		backup,
		labelComponentUploadJob,
		MantleUploadJobPrefix,
		&hook,
	)
}

func calculateExportDataPVCSize(transferPartSize *resource.Quantity) (*resource.Quantity, error) {
	if transferPartSize == nil {
		return nil, errors.New("transferPartSize cannot be nil")
	}

	pvcSizeI64, ok := transferPartSize.AsInt64()
	if !ok {
		return nil, fmt.Errorf("failed to convert status.transferPartSize to int64: %s", transferPartSize.String())
	}

	// The margin of the PVC size accounts for the filesystem metadata overhead required in addition
	// to the backup data itself. The lower bound prevents the metadata from taking up a disproportionately
	// large share of the PVC when the backup data size is small.
	if pvcSizeI64 < 512*1024*1024 {
		pvcSizeI64 = 512 * 1024 * 1024
	}
	pvcSizeI64 = pvcSizeI64 * 2

	pvcSize := resource.NewQuantity(pvcSizeI64, transferPartSize.Format)
	if pvcSize == nil {
		return nil, fmt.Errorf("resource.NewQuantity failed: %d %s", pvcSizeI64, string(transferPartSize.Format))
	}

	return pvcSize, nil
}

func (r *MantleBackupReconciler) createOrUpdateExportDataPVC(
	ctx context.Context,
	target *mantlev1.MantleBackup,
	largestCompletedPartNum int,
) *reconcileResult {
	var targetPVC corev1.PersistentVolumeClaim
	if err := json.Unmarshal([]byte(target.Status.PVCManifest), &targetPVC); err != nil {
		return reconcileFailed("failed to unmarshal the PVC manifest: %w", err)
	}

	pvcSize, err := calculateExportDataPVCSize(target.Status.TransferPartSize)
	if err != nil {
		return reconcileFailed("failed to calculate export data PVC size: %s/%s: %w",
			target.GetNamespace(), target.GetName(), err)
	}

	var pvc corev1.PersistentVolumeClaim
	pvc.SetName(MakeExportDataPVCName(target, largestCompletedPartNum+1))
	pvc.SetNamespace(r.managedCephClusterID)
	if _, err = ctrl.CreateOrUpdate(ctx, r.Client, &pvc, func() error {
		labels := pvc.GetLabels()
		if labels == nil {
			labels = make(map[string]string)
		}
		labels["app.kubernetes.io/name"] = labelAppNameValue
		labels["app.kubernetes.io/component"] = labelComponentExportData
		pvc.SetLabels(labels)

		if pvc.Spec.Resources.Requests == nil {
			pvc.Spec.Resources.Requests = map[corev1.ResourceName]resource.Quantity{}
		}
		pvc.Spec.Resources.Requests[corev1.ResourceStorage] = *pvcSize

		if !pvc.CreationTimestamp.IsZero() {
			return nil
		}
		pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
		pvc.Spec.StorageClassName = &r.primarySettings.ExportDataStorageClass

		return nil
	}); err != nil {
		return reconcileFailed("failed to create or update export data PVC: %w", err)
	}

	return nil
}

func MakeExportJobName(target *mantlev1.MantleBackup, index int) string {
	return fmt.Sprintf("%s%s-%d", MantleExportJobPrefix, string(target.GetUID()), index)
}

func MakeUploadJobName(target *mantlev1.MantleBackup, index int) string {
	return fmt.Sprintf("%s%s-%d", MantleUploadJobPrefix, string(target.GetUID()), index)
}

func MakeExportDataPVCName(target *mantlev1.MantleBackup, index int) string {
	return fmt.Sprintf("%s%s-%d", MantleExportDataPVCPrefix, string(target.GetUID()), index)
}

func MakeObjectNameOfExportedData(name, uid string, index int) string {
	return fmt.Sprintf("%s-%s-%d.bin", name, uid, index)
}

func MakeImportJobName(target *mantlev1.MantleBackup, index int) string {
	return fmt.Sprintf("%s%s-%d", MantleImportJobPrefix, string(target.GetUID()), index)
}

func MakeMiddleSnapshotName(backup *mantlev1.MantleBackup, offset int) string {
	return fmt.Sprintf("%s-offset-%d", backup.GetAnnotations()[annotRemoteUID], offset)
}

func MakeVerifyImageName(target *mantlev1.MantleBackup) string {
	return mantleVerifyImagePrefix + string(target.GetUID())
}

func MakeVerifyJobName(target *mantlev1.MantleBackup) string {
	return MantleVerifyJobPrefix + string(target.GetUID())
}

func MakeVerifyPVCName(target *mantlev1.MantleBackup) string {
	return mantleVerifyPVCPrefix + string(target.GetUID())
}

func MakeVerifyPVName(target *mantlev1.MantleBackup) string {
	return mantleVerifyPVPrefix + string(target.GetUID())
}

func MakeZeroOutJobName(target *mantlev1.MantleBackup) string {
	return MantleZeroOutJobPrefix + string(target.GetUID())
}

func MakeZeroOutPVCName(target *mantlev1.MantleBackup) string {
	return MantleZeroOutPVCPrefix + string(target.GetUID())
}

func MakeZeroOutPVName(target *mantlev1.MantleBackup) string {
	return MantleZeroOutPVPrefix + string(target.GetUID())
}

func ExtractPartNumFromComponentJobName(componentPrefix string, jobName string, backup *mantlev1.MantleBackup) (int, bool) {
	prefix := fmt.Sprintf("%s%s-", componentPrefix, string(backup.GetUID()))
	partNumString, ok := strings.CutPrefix(jobName, prefix)
	if !ok {
		return 0, false
	}
	partNum, err := strconv.Atoi(partNumString)
	if err != nil {
		return 0, false
	}

	return partNum, true
}

func ExtractPartNumFromExportJobName(jobName string, backup *mantlev1.MantleBackup) (int, bool) {
	return ExtractPartNumFromComponentJobName(MantleExportJobPrefix, jobName, backup)
}

func ExtractPartNumFromUploadJobName(jobName string, backup *mantlev1.MantleBackup) (int, bool) {
	return ExtractPartNumFromComponentJobName(MantleUploadJobPrefix, jobName, backup)
}

func ExtractPartNumFromImportJobName(jobName string, backup *mantlev1.MantleBackup) (int, bool) {
	return ExtractPartNumFromComponentJobName(MantleImportJobPrefix, jobName, backup)
}

func (r *MantleBackupReconciler) createOrUpdateExportJob(
	ctx context.Context,
	target *mantlev1.MantleBackup,
	sourceBackupNamePtr *string,
	largestCompletedPartNum int,
) *reconcileResult {
	sourceBackupName := ""
	if sourceBackupNamePtr != nil {
		sourceBackupName = *sourceBackupNamePtr
	}

	poolName, imageName, result := r.getPoolAndImageFromStatusPVManifest(target)
	if result.shouldReturn() {
		return result.wrapIfError("failed to get pool and image from .status.PVManifest")
	}

	partNum := largestCompletedPartNum + 1

	transferPartSize, ok := target.Status.TransferPartSize.AsInt64()
	if !ok {
		return reconcileFailed("failed to convert transferPartSize to int64: %d", transferPartSize)
	}

	script := os.Getenv(EnvExportJobScript)
	if script == "" {
		script = EmbedJobExportScript
	}

	var job batchv1.Job
	job.SetName(MakeExportJobName(target, partNum))
	job.SetNamespace(r.managedCephClusterID)
	if _, err := ctrl.CreateOrUpdate(ctx, r.Client, &job, func() error {
		labels := job.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels["app.kubernetes.io/name"] = labelAppNameValue
		labels["app.kubernetes.io/component"] = labelComponentExportJob
		job.SetLabels(labels)

		job.Spec.BackoffLimit = ptr.To(int32(65535))

		if !job.CreationTimestamp.IsZero() {
			return nil
		}
		job.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{
			FSGroup:      ptr.To(nonRootFSGroup),
			RunAsGroup:   ptr.To(nonRootGroupID),
			RunAsNonRoot: ptr.To(true),
			RunAsUser:    ptr.To(nonRootUserID),
		}

		job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyOnFailure

		job.Spec.Template.Spec.Containers = []corev1.Container{
			{
				Name:    "export",
				Command: []string{"/bin/bash", "-c", script},
				Env: []corev1.EnvVar{
					{
						Name: "ROOK_CEPH_USERNAME",
						ValueFrom: &corev1.EnvVarSource{
							SecretKeyRef: &corev1.SecretKeySelector{
								Key: "ceph-username",
								LocalObjectReference: corev1.LocalObjectReference{
									Name: "rook-ceph-mon",
								},
							},
						},
					},
					{
						Name:  "POOL_NAME",
						Value: poolName,
					},
					{
						Name:  "SRC_IMAGE_NAME",
						Value: imageName,
					},
					{
						Name:  "FROM_SNAP_NAME",
						Value: sourceBackupName,
					},
					{
						Name:  "SRC_SNAP_NAME",
						Value: target.GetName(),
					},
					{
						Name:  "PART_NUM",
						Value: strconv.Itoa(partNum),
					},
					{
						Name:  "TRANSFER_PART_SIZE_IN_BYTES",
						Value: strconv.FormatInt(transferPartSize, 10),
					},
					{
						Name:  "EXPORT_TARGET_MANTLE_BACKUP_UID",
						Value: string(target.GetUID()),
					},
				},
				Image:           r.podImage,
				ImagePullPolicy: corev1.PullIfNotPresent,
				VolumeMounts: []corev1.VolumeMount{
					{
						MountPath: "/etc/ceph",
						Name:      "ceph-config",
					},
					{
						MountPath: "/etc/rook",
						Name:      "mon-endpoint-volume",
					},
					{
						MountPath: "/var/lib/rook-ceph-mon",
						Name:      "ceph-admin-secret",
						ReadOnly:  true,
					},
					{
						MountPath: "/mantle",
						Name:      "volume-to-store",
					},
				},
			},
		}

		job.Spec.Template.Spec.Volumes = []corev1.Volume{
			{
				Name: "volume-to-store",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: MakeExportDataPVCName(target, partNum),
					},
				},
			},
			{
				Name: "ceph-admin-secret",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: "rook-ceph-mon",
						Optional:   ptr.To(false),
						Items: []corev1.KeyToPath{{
							Key:  "ceph-secret",
							Path: "secret.keyring",
						}},
					},
				},
			},
			{
				Name: "mon-endpoint-volume",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						Items: []corev1.KeyToPath{
							{
								Key:  "data",
								Path: "mon-endpoints",
							},
						},
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "rook-ceph-mon-endpoints",
						},
					},
				},
			},
			{
				Name: "ceph-config",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			},
		}

		return nil
	}); err != nil {
		return reconcileFailed("failed to create or update export Job: %w", err)
	}

	return nil
}

func (r *MantleBackupReconciler) createOrUpdateUploadJobs(
	ctx context.Context,
	target *mantlev1.MantleBackup,
	largestCompletedExportPartNum,
	largestCompletedUploadPartNum int,
) *reconcileResult {
	minPartNum, maxPartNum, result := r.getPartNumRangeOfExpectedRunningUploadJobs(
		ctx,
		target,
		largestCompletedExportPartNum,
		largestCompletedUploadPartNum,
	)
	if result.shouldReturn() {
		return result.wrapIfError("failed to get part num range of runnable upload jobs")
	}

	script := os.Getenv(EnvUploadJobScript)
	if script == "" {
		script = EmbedJobUploadScript
	}

	for partNum := minPartNum; partNum <= maxPartNum; partNum++ {
		var job batchv1.Job
		job.SetName(MakeUploadJobName(target, partNum))
		job.SetNamespace(r.managedCephClusterID)
		if _, err := ctrl.CreateOrUpdate(ctx, r.Client, &job, func() error {
			labels := job.GetLabels()
			if labels == nil {
				labels = map[string]string{}
			}
			labels["app.kubernetes.io/name"] = labelAppNameValue
			labels["app.kubernetes.io/component"] = labelComponentUploadJob
			job.SetLabels(labels)

			job.Spec.BackoffLimit = ptr.To(int32(65535))

			if !job.CreationTimestamp.IsZero() {
				return nil
			}
			job.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{
				FSGroup:      ptr.To(nonRootFSGroup),
				RunAsGroup:   ptr.To(nonRootGroupID),
				RunAsNonRoot: ptr.To(true),
				RunAsUser:    ptr.To(nonRootUserID),
			}

			job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyOnFailure

			job.Spec.Template.Spec.Containers = []corev1.Container{
				{
					Name:    "upload",
					Command: []string{"/bin/bash", "-c", script},
					Env: []corev1.EnvVar{
						{
							Name:  "OBJ_NAME",
							Value: MakeObjectNameOfExportedData(target.GetName(), string(target.GetUID()), partNum),
						},
						{
							Name:  "BUCKET_NAME",
							Value: r.objectStorageSettings.BucketName,
						},
						{
							Name:  "OBJECT_STORAGE_ENDPOINT",
							Value: r.objectStorageSettings.Endpoint,
						},
						{
							Name: "AWS_ACCESS_KEY_ID",
							ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: r.envSecret,
									},
									Key: "AWS_ACCESS_KEY_ID",
								},
							},
						},
						{
							Name: "AWS_SECRET_ACCESS_KEY",
							ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: r.envSecret,
									},
									Key: "AWS_SECRET_ACCESS_KEY",
								},
							},
						},
						{
							Name:  "HTTP_PROXY",
							Value: r.proxySettings.HttpProxy,
						},
						{
							Name:  "HTTPS_PROXY",
							Value: r.proxySettings.HttpsProxy,
						},
						{
							Name:  "NO_PROXY",
							Value: r.proxySettings.NoProxy,
						},
						{
							Name:  "PART_NUM",
							Value: strconv.Itoa(partNum),
						},
					},
					Image:           r.podImage,
					ImagePullPolicy: corev1.PullIfNotPresent,
					VolumeMounts: []corev1.VolumeMount{
						{
							MountPath: "/mantle",
							Name:      "volume-to-store",
						},
					},
				},
			}

			job.Spec.Template.Spec.Volumes = []corev1.Volume{
				{
					Name: "volume-to-store",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: MakeExportDataPVCName(target, partNum),
						},
					},
				},
			}

			if r.objectStorageSettings.CACertConfigMap != nil {
				container := job.Spec.Template.Spec.Containers[0]
				container.Env = append(
					container.Env,
					corev1.EnvVar{
						Name:  "CERT_FILE",
						Value: "/mantle_ca_cert/" + *r.objectStorageSettings.CACertKey,
					},
				)
				container.VolumeMounts = append(
					container.VolumeMounts,
					corev1.VolumeMount{
						MountPath: "/mantle_ca_cert",
						Name:      "ca-cert",
					},
				)
				job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes,
					corev1.Volume{
						Name: "ca-cert",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: *r.objectStorageSettings.CACertConfigMap,
								},
							},
						},
					},
				)
			}

			return nil
		}); err != nil {
			return reconcileFailed("failed to create or update upload Job: %w", err)
		}
	}

	return nil
}

func (r *MantleBackupReconciler) startImport(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	target *snapshotTarget,
) *reconcileResult {
	logger := log.FromContext(ctx)
	logger.Info("starting import reconciliation",
		"backupUID", string(backup.GetUID()),
		"pv", target.pv.GetName(),
		"pvc", fmt.Sprintf("%s/%s", target.pvc.GetNamespace(), target.pvc.GetName()),
		"pool", target.poolName,
		"image", target.imageName,
	)

	if !r.doesMantleBackupHaveSyncModeAnnot(backup) {
		// SetSynchronizing is not called yet or the cache is stale.
		// Note that we should not return a successful reconcileResult here because we can't proceed to secondaryCleanup.
		return reconcileRequeue()
	}

	if uploaded, result := r.isExportDataAlreadyUploaded(ctx, backup, 0); result.shouldReturn() {
		return result.wrapIfError("failed to check if export data part 0 is already uploaded")
	} else if !uploaded {
		logger.Info("waiting for the export data to be uploaded", "partNum", 0)

		return reconcileRequeue()
	}

	largestCompletedPartNum, result := r.handleCompletedImportJobs(ctx, backup)
	if result.shouldReturn() {
		return result.wrapIfError("failed to handle completed import jobs")
	}

	// Requeue if the PV is smaller than the PVC. (This may be the case if pvc-autoresizer is used.)
	if isPVSmallerThanPVC(target.pv, target.pvc) {
		return reconcileRequeue()
	}

	if result := r.updateStatusManifests(ctx, backup, target.pv, target.pvc); result.shouldReturn() {
		return result.wrapIfError("failed to update status manifests")
	}

	succeed, result := r.lockVolume(target.poolName, target.imageName, string(backup.GetUID()))
	if result.shouldReturn() {
		return result.wrapIfError("failed to lock the volume")
	}
	if !succeed {
		logger.Info("the volume is locked by another process", "uid", string(backup.GetUID()))

		return reconcileRequeue()
	}

	if result := r.reconcileZeroOutJob(ctx, backup, target); result.shouldReturn() {
		return result.wrapIfError("failed to reconcile zeroout Job")
	}

	if result := r.reconcileImportJob(ctx, backup, target, largestCompletedPartNum); result.shouldReturn() {
		return result.wrapIfError("failed to reconcile import Job")
	}

	if result := r.unlockVolume(target.poolName, target.imageName, string(backup.GetUID())); result.shouldReturn() {
		return result.wrapIfError("failed to unlock the volume")
	}

	if result := r.markSecondarySnapshotCaptured(ctx, backup, target); result.shouldReturn() {
		return result.wrapIfError("failed to mark the secondary as snapshot captured")
	}

	return nil
}

func (r *MantleBackupReconciler) doesMantleBackupHaveSyncModeAnnot(backup *mantlev1.MantleBackup) bool {
	annots := backup.GetAnnotations()
	syncMode, ok := annots[annotSyncMode]

	return ok && (syncMode == syncModeFull || syncMode == syncModeIncremental)
}

func (r *MantleBackupReconciler) prepareObjectStorageClient(ctx context.Context) *reconcileResult {
	if r.objectStorageClient != nil {
		return nil
	}

	var caPEMCerts []byte

	if r.objectStorageSettings.CACertConfigMap != nil {
		var cm corev1.ConfigMap
		if err := r.Get(
			ctx, types.NamespacedName{
				Name:      *r.objectStorageSettings.CACertConfigMap,
				Namespace: r.managedCephClusterID,
			},
			&cm,
		); err != nil {
			return reconcileFailed("failed to get ConfigMap for ca-cert-key: %w", err)
		}

		caPEMCertsString, ok := cm.Data[*r.objectStorageSettings.CACertKey]
		if !ok {
			return reconcileFailed("ca-cert-key not found in ConfigMap: %s", *r.objectStorageSettings.CACertConfigMap)
		}
		caPEMCerts = []byte(caPEMCertsString)
	}

	var envSecret corev1.Secret
	if err := r.Get(
		ctx,
		types.NamespacedName{Name: r.envSecret, Namespace: r.managedCephClusterID},
		&envSecret,
	); err != nil {
		return reconcileFailed("failed to get env-secret: %w", err)
	}
	accessKeyID, ok := envSecret.Data["AWS_ACCESS_KEY_ID"]
	if !ok {
		return reconcileFailed("failed to find AWS_ACCESS_KEY_ID in env-secret")
	}
	secretAccessKey, ok := envSecret.Data["AWS_SECRET_ACCESS_KEY"]
	if !ok {
		return reconcileFailed("failed to find AWS_SECRET_ACCESS_KEY in env-secret")
	}

	var err error
	r.objectStorageClient, err = objectstorage.NewS3Bucket(
		ctx,
		r.objectStorageSettings.BucketName,
		r.objectStorageSettings.Endpoint,
		string(accessKeyID),
		string(secretAccessKey),
		caPEMCerts,
	)
	if err != nil {
		return reconcileFailed("failed to create object storage client: %w", err)
	}

	return nil
}

func (r *MantleBackupReconciler) isExportDataAlreadyUploaded(
	ctx context.Context,
	target *mantlev1.MantleBackup,
	index int,
) (bool, *reconcileResult) {
	key := MakeObjectNameOfExportedData(target.GetName(), target.GetAnnotations()[annotRemoteUID], index)
	uploaded, err := r.objectStorageClient.Exists(ctx, key)
	if err != nil {
		return false, reconcileFailed("failed to check if an object exists in the object storage: %s: %w", key, err)
	}

	return uploaded, nil
}

func (r *MantleBackupReconciler) handleCompletedImportJobs(ctx context.Context, backup *mantlev1.MantleBackup) (int, *reconcileResult) {
	return r.handleCompletedJobsOfComponent(ctx, backup, labelComponentImportJob, MantleImportJobPrefix, nil)
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
) *reconcileResult {
	if backup.Status.PVManifest != "" || backup.Status.PVCManifest != "" {
		return nil
	}

	if err := updateStatus(ctx, r.Client, backup, func() error {
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
	}); err != nil {
		return reconcileFailed("failed to update status manifests: %w", err)
	}

	return nil
}

// lockVolume adds a lock to the specified RBD volume if the lock is not already held.
// It returns true if the lock is held by this caller, false if another lock is held.
func (r *MantleBackupReconciler) lockVolume(
	poolName, imageName, lockID string,
) (bool, *reconcileResult) {
	// Add a lock.
	if errAdd := r.ceph.RBDLockAdd(poolName, imageName, lockID); errAdd != nil {
		locks, errLs := r.ceph.RBDLockLs(poolName, imageName)
		if errLs != nil {
			return false, reconcileFailed("failed to add a lock and list locks on volume %s/%s: %w", poolName, imageName, errors.Join(errAdd, errLs))
		}

		switch len(locks) {
		case 0:
			// It may have been unlocked after the lock failed, but since other causes are also possible, an error is returned.
			return false, reconcileFailed("failed to add a lock to the volume %s/%s: %w", poolName, imageName, errAdd)

		case 1:
			if locks[0].LockID == lockID {
				// Already locked by this MB.
				return true, nil
			}
			// Locked by another process.
			return false, nil

		default:
			// Multiple locks found; unexpected state.
			return false, reconcileFailed("multiple locks found on volume %s/%s after failed lock attempt(%v)", poolName, imageName, locks)
		}
	}

	// Locked
	return true, nil
}

// unlockVolume removes the specified lock from the RBD volume if the lock is held.
// No action is taken if the lock is not found.
func (r *MantleBackupReconciler) unlockVolume(
	poolName, imageName, lockID string,
) *reconcileResult {
	// List up locks to check if the lock is held.
	locks, err := r.ceph.RBDLockLs(poolName, imageName)
	if err != nil {
		return reconcileFailed("failed to list locks of the volume %s/%s: %w", poolName, imageName, err)
	}

	if len(locks) >= 2 {
		return reconcileFailed("multiple locks found on volume %s/%s when unlocking (%v)", poolName, imageName, locks)
	}

	for _, lock := range locks {
		if lock.LockID == lockID {
			// Unlock
			if err := r.ceph.RBDLockRm(poolName, imageName, lock); err != nil {
				return reconcileFailed("failed to remove the lock from the volume %s/%s: %w", poolName, imageName, err)
			}

			return nil
		}
	}

	// Already unlocked.
	return nil
}

func (r *MantleBackupReconciler) reconcileZeroOutJob(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	snapshotTarget *snapshotTarget,
) *reconcileResult {
	if backup.GetAnnotations()[annotSyncMode] != syncModeFull {
		return nil
	}

	// To run the zeroout job, the backup target volume must be accessed as a block device.
	// In most cases, the backup target volume is accessed via filesystem type PV, and it
	// cannot be accesses as a block device. Therefore, we use ceph-csi static PVC/PV
	// to create a separate PVC/PV that accesses the same underlying RBD image as
	// a block device.
	// ref. https://github.com/ceph/ceph-csi/blob/8a82b6e76f2c620fceec2d49659d00c97c6ddbbe/docs/static-pvc.md
	if result := r.createStaticPVIfNotExists(
		ctx,
		snapshotTarget.pv,
		snapshotTarget.pv.Spec.CSI.VolumeAttributes["imageName"],
		snapshotTarget.pv.Spec.Capacity,
		MakeZeroOutPVName(backup),
		MakeZeroOutPVCName(backup),
		labelComponentZeroOutVolume,
		false,
	); result.shouldReturn() {
		return result.wrapIfError("failed to create a static PV for zeroout")
	}

	if result := r.createStaticPVCIfNotExists(
		ctx,
		MakeZeroOutPVCName(backup),
		MakeZeroOutPVName(backup),
		labelComponentZeroOutVolume,
		snapshotTarget.pvc.Spec.Resources,
	); result.shouldReturn() {
		return result.wrapIfError("failed to create a static PVC for zeroout")
	}

	if result := r.createOrUpdateZeroOutJob(ctx, backup); result.shouldReturn() {
		return result.wrapIfError("failed to create or update zeroout Job")
	}

	completed, result := r.hasZeroOutJobCompleted(ctx, backup)
	if result.shouldReturn() {
		return result.wrapIfError("failed to check if the zeroout Job has completed")
	}
	if completed {
		return nil
	}

	return reconcileRequeue()
}

// createStaticPVIfNotExists creates a static PersistentVolume (PV) if it does not exist.
// It copies relevant CSI and metadata fields from the basePV, sets the provided volume handle,
// capacity, and component label, and ensures the PV is configured for static provisioning.
//
// Parameters:
//
//	ctx           - context for the API calls
//	basePV        - source PV to copy CSI and metadata fields from
//	volume        - volume handle to assign to the new PV
//	capacity      - resource capacity for the PV
//	newPvName     - name for the new PV
//	componentName - label value for the component
//	addFlatten    - whether to add deep-flatten feature to the volume attributes
func (r *MantleBackupReconciler) createStaticPVIfNotExists(
	ctx context.Context,
	basePV *corev1.PersistentVolume,
	volume string,
	capacity corev1.ResourceList,
	newPvName, pvcName, componentName string,
	addFlatten bool,
) *reconcileResult {
	if basePV.Spec.CSI == nil {
		return reconcileFailed("PV is not a CSI volume")
	}
	feature := basePV.Spec.CSI.VolumeAttributes["imageFeatures"]
	if addFlatten {
		if len(feature) > 0 {
			feature = feature + ",deep-flatten"
		} else {
			feature = "deep-flatten"
		}
	}

	err := r.Create(ctx, &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: newPvName,
			Labels: map[string]string{
				"app.kubernetes.io/name":      labelAppNameValue,
				"app.kubernetes.io/component": componentName,
			},
		},
		Spec: corev1.PersistentVolumeSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Capacity:    capacity,
			// Set ClaimRef to bind only to the specific PVC.
			ClaimRef: &corev1.ObjectReference{
				Namespace: r.managedCephClusterID,
				Name:      pvcName,
			},
			PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
			// No StorageClass to indicate static provisioning.
			StorageClassName: "",
			VolumeMode:       ptr.To(corev1.PersistentVolumeBlock),

			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:             basePV.Spec.CSI.Driver,
					NodeStageSecretRef: basePV.Spec.CSI.NodeStageSecretRef,
					VolumeAttributes: map[string]string{
						"clusterID":     basePV.Spec.CSI.VolumeAttributes["clusterID"],
						"imageFeatures": feature,
						"imageFormat":   basePV.Spec.CSI.VolumeAttributes["imageFormat"],
						"pool":          basePV.Spec.CSI.VolumeAttributes["pool"],
						"staticVolume":  "true",
					},
					VolumeHandle: volume,
				},
			},
		},
	})

	if err != nil {
		// Ignore AlreadyExists error
		if aerrors.IsAlreadyExists(err) {
			return nil
		}

		return reconcileFailed("failed to create PV %s: %w", newPvName, err)
	}

	return nil
}

// createStaticPVCIfNotExists creates a PersistentVolumeClaim (PVC) for binding to a static PersistentVolume (PV) if it does not
// exist. This function configures the PVC to reference the specified static PV, sets labels,
// access modes, storage class, and resource requirements for static volume provisioning.
//
// Parameters:
//
//	ctx           - context for the API calls
//	pvcName       - name for the new or updated PVC
//	pvName        - name of the static PV to bind to
//	componentName - label value for the component
//	resources     - resource requirements for the PVC
func (r *MantleBackupReconciler) createStaticPVCIfNotExists(
	ctx context.Context,
	pvcName, pvName, componentName string,
	resources corev1.VolumeResourceRequirements,
) *reconcileResult {
	err := r.Create(ctx, &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: r.managedCephClusterID,
			Name:      pvcName,
			Labels: map[string]string{
				"app.kubernetes.io/name":      labelAppNameValue,
				"app.kubernetes.io/component": componentName,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   resources,
			// No StorageClass to indicate static provisioning.
			// and ensure this PVC statically binds to the specific PV.
			StorageClassName: ptr.To(""),
			VolumeName:       pvName,
			VolumeMode:       ptr.To(corev1.PersistentVolumeBlock),
		},
	})

	if err != nil {
		// Ignore AlreadyExists error
		if aerrors.IsAlreadyExists(err) {
			return nil
		}

		return reconcileFailed("failed to create PVC %s: %w", pvcName, err)
	}

	return nil
}

// createOrUpdateZeroOutJob creates or updates a Job that runs `blkdiscard -z`
// on the backup target volume before a full backup. This achieves the following:
//   - Zero-fills the volume to erase any stale data that existed before the backup.
//   - Releases the storage space that was allocated for that stale data.
//   - Eliminates the risk of stale data causing issues in subsequent processing.
//
// The reason `blkdiscard -z` is used instead of plain `blkdiscard`:
//   - `blkdiscard` does not guarantee that existing data is actually erased.
//   - `blkdiscard -z`, like `blkdiscard`, releases all regions, but also zero-fills them.
//
// ref. https://www.spinics.net/lists/ceph-devel/msg61800.html
func (r *MantleBackupReconciler) createOrUpdateZeroOutJob(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
) *reconcileResult {
	var job batchv1.Job
	job.SetName(MakeZeroOutJobName(backup))
	job.SetNamespace(r.managedCephClusterID)
	if _, err := ctrl.CreateOrUpdate(ctx, r.Client, &job, func() error {
		labels := job.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels["app.kubernetes.io/name"] = labelAppNameValue
		labels["app.kubernetes.io/component"] = labelComponentZeroOutJob
		job.SetLabels(labels)

		job.Spec.BackoffLimit = ptr.To(int32(65535))

		if !job.CreationTimestamp.IsZero() {
			return nil
		}
		job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyOnFailure

		job.Spec.Template.Spec.Containers = []corev1.Container{
			{
				Name:  "zeroout",
				Image: r.podImage,
				Command: []string{
					"/bin/bash",
					"-c",
					`
set -e
blkdiscard -z /dev/zeroout-rbd
`,
				},
				SecurityContext: &corev1.SecurityContext{
					Privileged: ptr.To(true),
					RunAsGroup: ptr.To(int64(0)),
					RunAsUser:  ptr.To(int64(0)),
				},
				VolumeDevices: []corev1.VolumeDevice{
					{
						Name:       "zeroout-rbd",
						DevicePath: "/dev/zeroout-rbd",
					},
				},
				ImagePullPolicy: corev1.PullIfNotPresent,
			},
		}

		job.Spec.Template.Spec.Volumes = []corev1.Volume{
			{
				Name: "zeroout-rbd",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: MakeZeroOutPVCName(backup),
					},
				},
			},
		}

		return nil
	}); err != nil {
		return reconcileFailed("failed to create or update zeroout Job: %w", err)
	}

	return nil
}

func (r *MantleBackupReconciler) createOrUpdateVerifyJob(ctx context.Context, jobName, pvcName string) *reconcileResult {
	var job batchv1.Job
	job.SetName(jobName)
	job.SetNamespace(r.managedCephClusterID)
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, &job, func() error {
		labels := job.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels["app.kubernetes.io/name"] = labelAppNameValue
		labels["app.kubernetes.io/component"] = labelComponentVerifyJob
		job.SetLabels(labels)

		job.Spec.BackoffLimit = ptr.To(int32(65535))

		if !job.CreationTimestamp.IsZero() {
			return nil
		}
		/*
			According to the e2fsck man page:

			> The exit code returned by e2fsck is the sum of the following conditions:
			>      0    - No errors
			>      1    - File system errors corrected
			>      2    - File system errors corrected, system should
			>             be rebooted
			>      4    - File system errors left uncorrected
			>      8    - Operational error
			>      16   - Usage or syntax error
			>      32   - E2fsck canceled by user request
			>      128  - Shared library error

			We should retry e2fsck when it's canceled by signals like SIGTERM or
			SIGINT, so the expected failure exit codes should NOT include
			numbers that have bit 5 (value 32) set. In addition, when e2fsck is
			killed by SIGKILL, it will return a code larger than 128.  We should
			retry in that case as well. To sum up, we should set the expected
			failure exit codes to [1,32).
		*/
		expectedFailureExitCodes := []int32{}
		for i := 1; i < 32; i++ {
			expectedFailureExitCodes = append(expectedFailureExitCodes, int32(i))
		}
		job.Spec.PodFailurePolicy = &batchv1.PodFailurePolicy{
			Rules: []batchv1.PodFailurePolicyRule{
				{
					Action: batchv1.PodFailurePolicyActionFailJob,
					OnExitCodes: &batchv1.PodFailurePolicyOnExitCodesRequirement{
						ContainerName: ptr.To("verify"),
						Operator:      batchv1.PodFailurePolicyOnExitCodesOpIn,
						Values:        expectedFailureExitCodes,
					},
				},
			},
		}

		job.Spec.Template.Spec.Containers = []corev1.Container{
			{
				Name:  "verify",
				Image: r.podImage,
				Command: []string{
					"/bin/bash",
					"-c",
					// The first e2fsck with -E journal_only is to replay the
					// journal and it may return non zero if there are errors
					// that need to be fixed.
					`
set -eux -o pipefail
/usr/sbin/e2fsck -y -E journal_only /dev/verify-rbd || true
/usr/sbin/e2fsck -fnv /dev/verify-rbd
`,
				},
				SecurityContext: &corev1.SecurityContext{
					Privileged: ptr.To(true),
					RunAsGroup: ptr.To(int64(0)),
					RunAsUser:  ptr.To(int64(0)),
				},
				VolumeDevices: []corev1.VolumeDevice{
					{
						Name:       "verify-rbd",
						DevicePath: "/dev/verify-rbd",
					},
				},
				ImagePullPolicy: corev1.PullIfNotPresent,
			},
		}

		job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever

		job.Spec.Template.Spec.Volumes = []corev1.Volume{
			{
				Name: "verify-rbd",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: pvcName,
					},
				},
			},
		}

		return nil
	})

	if err != nil {
		return reconcileFailed("failed to create or update verify job %s: %w", jobName, err)
	}

	return nil
}

func (r *MantleBackupReconciler) hasZeroOutJobCompleted(ctx context.Context, backup *mantlev1.MantleBackup) (bool, *reconcileResult) {
	var job batchv1.Job
	if err := r.Get(
		ctx,
		types.NamespacedName{Name: MakeZeroOutJobName(backup), Namespace: r.managedCephClusterID},
		&job,
	); err != nil {
		if aerrors.IsNotFound(err) {
			return false, nil // The cache must be stale. Let's just requeue.
		}

		return false, reconcileFailed("failed to get zeroout Job: %w", err)
	}

	if IsJobConditionTrue(job.Status.Conditions, batchv1.JobComplete) {
		return true, nil
	}

	return false, nil
}

// reconcileImportJob ensures that the import Job for the next part is created if the previous parts have been imported.
// It checks if all import Jobs are completed and whether the export data for the next part is already uploaded.
func (r *MantleBackupReconciler) reconcileImportJob(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	snapshotTarget *snapshotTarget,
	largestCompletedPartNum int,
) *reconcileResult {
	logger := log.FromContext(ctx)
	logger.Info("reconciling import job",
		"backupUID", string(backup.GetUID()),
		"pv", snapshotTarget.pv.GetName(),
		"pvc", fmt.Sprintf("%s/%s", snapshotTarget.pvc.GetNamespace(), snapshotTarget.pvc.GetName()),
		"pool", snapshotTarget.poolName,
		"image", snapshotTarget.imageName,
		"largestCompletedPartNum", largestCompletedPartNum,
	)

	partNum := largestCompletedPartNum + 1

	// Check that all import Jobs are completed
	finalPartNum, result := r.getNumberOfParts(backup)
	if result.shouldReturn() {
		return result.wrapIfError("failed to calculate num of export data parts")
	}
	if partNum == finalPartNum {
		return nil
	}

	// Check that the export data is already uploaded.
	uploaded, result := r.isExportDataAlreadyUploaded(ctx, backup, partNum)
	if result.shouldReturn() {
		return result.wrapIfError("failed to check if part of export data is not already uploaded: %d", partNum)
	}
	if !uploaded {
		logger.Info("export data for the next part is not yet uploaded", "partNum", partNum)

		return reconcileRequeue()
	}

	// check if a new import Job can be created
	ok, result := r.canNewImportJobBeCreated(ctx)
	if result.shouldReturn() {
		return result.wrapIfError("failed to check if a new import Job can be created")
	}
	if !ok {
		return reconcileRequeue()
	}

	// create or update an import Job
	if result := r.createOrUpdateImportJob(ctx, backup, snapshotTarget, partNum); result.shouldReturn() {
		return result.wrapIfError("failed to create or update import Job")
	}

	return reconcileRequeue()
}

func (r *MantleBackupReconciler) createOrUpdateImportJob(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	snapshotTarget *snapshotTarget,
	partNum int,
) *reconcileResult {
	if backup.Status.TransferPartSize == nil {
		return reconcileFailed("status.transferPartSize is nil")
	}
	transferPartSize, ok := backup.Status.TransferPartSize.AsInt64()
	if !ok {
		return reconcileFailed("status.transferPartSize can't be converted to int64")
	}

	script := os.Getenv(EnvImportJobScript)
	if script == "" {
		script = EmbedJobImportScript
	}

	var job batchv1.Job

	job.SetName(MakeImportJobName(backup, partNum))
	job.SetNamespace(r.managedCephClusterID)

	if _, err := ctrl.CreateOrUpdate(ctx, r.Client, &job, func() error {
		labels := job.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels["app.kubernetes.io/name"] = labelAppNameValue
		labels["app.kubernetes.io/component"] = labelComponentImportJob
		job.SetLabels(labels)

		job.Spec.BackoffLimit = ptr.To(int32(65535))

		if !job.CreationTimestamp.IsZero() {
			return nil
		}
		job.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{
			RunAsGroup:   ptr.To(nonRootGroupID),
			RunAsNonRoot: ptr.To(true),
			RunAsUser:    ptr.To(nonRootUserID),
		}

		job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyOnFailure

		sourceBackupName := backup.GetAnnotations()[annotDiffFrom]
		if partNum != 0 {
			sourceBackupName = MakeMiddleSnapshotName(backup, partNum*int(transferPartSize))
		}

		container := corev1.Container{
			Name:    "import",
			Command: []string{"/bin/bash", "-c", script},
			Env: []corev1.EnvVar{
				{
					Name: "ROOK_CEPH_USERNAME",
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							Key: "ceph-username",
							LocalObjectReference: corev1.LocalObjectReference{
								Name: "rook-ceph-mon",
							},
						},
					},
				},
				{
					Name:  "POOL_NAME",
					Value: snapshotTarget.pv.Spec.CSI.VolumeAttributes["pool"],
				},
				{
					Name:  "DST_IMAGE_NAME",
					Value: snapshotTarget.imageName,
				},
				{
					Name:  "FROM_SNAP_NAME",
					Value: sourceBackupName,
				},
				{
					Name:  "OBJ_NAME",
					Value: MakeObjectNameOfExportedData(backup.GetName(), backup.GetAnnotations()[annotRemoteUID], partNum),
				},
				{
					Name:  "BUCKET_NAME",
					Value: r.objectStorageSettings.BucketName,
				},
				{
					Name:  "OBJECT_STORAGE_ENDPOINT",
					Value: r.objectStorageSettings.Endpoint,
				},
				{
					Name: "AWS_ACCESS_KEY_ID",
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: r.envSecret,
							},
							Key: "AWS_ACCESS_KEY_ID",
						},
					},
				},
				{
					Name: "AWS_SECRET_ACCESS_KEY",
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: r.envSecret,
							},
							Key: "AWS_SECRET_ACCESS_KEY",
						},
					},
				},
				{
					Name:  "PART_NUM",
					Value: strconv.Itoa(partNum),
				},
			},
			Image:           r.podImage,
			ImagePullPolicy: corev1.PullIfNotPresent,
			VolumeMounts: []corev1.VolumeMount{
				{
					MountPath: "/etc/ceph",
					Name:      "ceph-config",
				},
				{
					MountPath: "/etc/rook",
					Name:      "mon-endpoint-volume",
				},
				{
					MountPath: "/var/lib/rook-ceph-mon",
					Name:      "ceph-admin-secret",
					ReadOnly:  true,
				},
			},
		}

		if r.objectStorageSettings.CACertConfigMap != nil {
			container.Env = append(
				container.Env,
				corev1.EnvVar{
					Name:  "CERT_FILE",
					Value: filepath.Join("/mantle_ca_cert", *r.objectStorageSettings.CACertKey),
				},
			)
			container.VolumeMounts = append(
				container.VolumeMounts,
				corev1.VolumeMount{
					MountPath: "/mantle_ca_cert",
					Name:      "ca-cert",
				},
			)
		}

		job.Spec.Template.Spec.Containers = []corev1.Container{container}

		job.Spec.Template.Spec.Volumes = []corev1.Volume{
			{
				Name: "ceph-admin-secret",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: "rook-ceph-mon",
						Optional:   ptr.To(false),
						Items: []corev1.KeyToPath{{
							Key:  "ceph-secret",
							Path: "secret.keyring",
						}},
					},
				},
			},
			{
				Name: "mon-endpoint-volume",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						Items: []corev1.KeyToPath{
							{
								Key:  "data",
								Path: "mon-endpoints",
							},
						},
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "rook-ceph-mon-endpoints",
						},
					},
				},
			},
			{
				Name: "ceph-config",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			},
		}

		if r.objectStorageSettings.CACertConfigMap != nil {
			job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
				Name: "ca-cert",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: *r.objectStorageSettings.CACertConfigMap,
						},
					},
				},
			})
		}

		return nil
	}); err != nil {
		return reconcileFailed("failed to create or update import Job: %w", err)
	}

	return nil
}

// markSecondarySnapshotCaptured marks the MantleBackup SnapshotCaptured as True.
func (r *MantleBackupReconciler) markSecondarySnapshotCaptured(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	snapshotTarget *snapshotTarget,
) *reconcileResult {
	// Find the RBD snapshot created for the imported backup.
	snapshot, err := ceph.FindRBDSnapshot(
		r.ceph,
		snapshotTarget.poolName,
		snapshotTarget.imageName,
		backup.GetName(),
	)
	if err != nil {
		return reconcileFailed("failed to find imported RBD snapshot: %w", err)
	}

	// Update the status of the MantleBackup to set True to the SnapshotCaptured condition.
	if err := updateStatus(ctx, r.Client, backup, func() error {
		backup.Status.SnapID = &snapshot.Id
		meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
			Type:   mantlev1.BackupConditionSnapshotCaptured,
			Status: metav1.ConditionTrue,
			Reason: mantlev1.ConditionReasonSnapshotCapturedNoProblem,
		})

		return nil
	}); err != nil {
		return reconcileFailed("failed to update MantleBackup status: %w", err)
	}

	return nil
}

func (r *MantleBackupReconciler) primaryCleanup(
	ctx context.Context,
	target *mantlev1.MantleBackup,
) *reconcileResult {
	diffFrom, ok := target.GetAnnotations()[annotDiffFrom]
	if ok {
		var source mantlev1.MantleBackup
		if err := r.Get(
			ctx,
			types.NamespacedName{Name: diffFrom, Namespace: target.GetNamespace()},
			&source,
		); err != nil {
			return reconcileFailed("failed to get source MantleBackup: %w", err)
		}
		delete(source.GetAnnotations(), annotDiffTo)
		if err := r.Update(ctx, &source); err != nil {
			return reconcileFailed("failed to update source MantleBackup: %w", err)
		}
	}

	if result := r.deleteAllExportJobs(ctx, target); result.shouldReturn() {
		return result.wrapIfError("failed to delete export Jobs")
	}

	if result := r.deleteAllUploadJobs(ctx, target); result.shouldReturn() {
		return result.wrapIfError("failed to delete upload Jobs")
	}

	if result := r.deleteAllExportDataPVCs(ctx, target); result.shouldReturn() {
		return result.wrapIfError("failed to delete export data PVCs")
	}

	if result := r.deleteVerifyJob(ctx, target); result.shouldReturn() {
		return result.wrapIfError("failed to delete verify Job")
	}
	if result := r.deleteVerifyPVC(ctx, target); result.shouldReturn() {
		return result.wrapIfError("failed to delete verify PVC")
	}
	if result := r.deleteVerifyPV(ctx, target); result.shouldReturn() {
		return result.wrapIfError("failed to delete verify PV")
	}
	if result := r.deleteVerifyRBDImage(target); result.shouldReturn() {
		return result.wrapIfError("failed to delete verify RBD image")
	}

	delete(target.GetAnnotations(), annotDiffFrom)
	delete(target.GetAnnotations(), annotSyncMode)
	if err := r.Update(ctx, target); err != nil {
		return reconcileFailed("failed to delete annotations of diff-from and sync-mode: %w", err)
	}

	return nil
}

func (r *MantleBackupReconciler) getNumberOfPartsForResourceDeletion(backup *mantlev1.MantleBackup) (int, *reconcileResult) {
	if backup.Status.SnapSize == nil || backup.Status.TransferPartSize == nil {
		return 0, nil
	}

	return r.getNumberOfParts(backup)
}

func (r *MantleBackupReconciler) deleteAllJobsOfComponent(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	makeJobName func(*mantlev1.MantleBackup, int) string,
) *reconcileResult {
	numParts, result := r.getNumberOfPartsForResourceDeletion(backup)
	if result.shouldReturn() {
		return result.wrapIfError("failed to get the number of the parts of the exported data")
	}

	for partNum := range numParts {
		var job batchv1.Job
		job.SetName(makeJobName(backup, partNum))
		job.SetNamespace(r.managedCephClusterID)
		if err := r.Delete(ctx, &job, &client.DeleteOptions{
			PropagationPolicy: ptr.To(metav1.DeletePropagationBackground),
		}); err != nil && !aerrors.IsNotFound(err) {
			return reconcileFailed("failed to delete Job: %s/%s: %w", job.GetNamespace(), job.GetName(), err)
		}
	}

	return nil
}

func (r *MantleBackupReconciler) deleteAllExportJobs(ctx context.Context, backup *mantlev1.MantleBackup) *reconcileResult {
	return r.deleteAllJobsOfComponent(ctx, backup, MakeExportJobName)
}

func (r *MantleBackupReconciler) deleteAllUploadJobs(ctx context.Context, backup *mantlev1.MantleBackup) *reconcileResult {
	return r.deleteAllJobsOfComponent(ctx, backup, MakeUploadJobName)
}

func (r *MantleBackupReconciler) deleteAllExportDataPVCs(ctx context.Context, backup *mantlev1.MantleBackup) *reconcileResult {
	numParts, result := r.getNumberOfPartsForResourceDeletion(backup)
	if result.shouldReturn() {
		return result.wrapIfError("failed to get the number of the parts of the exported data")
	}

	for partNum := range numParts {
		pvc := corev1.PersistentVolumeClaim{}
		pvc.SetName(MakeExportDataPVCName(backup, partNum))
		pvc.SetNamespace(r.managedCephClusterID)
		if err := r.Delete(ctx, &pvc, &client.DeleteOptions{
			PropagationPolicy: ptr.To(metav1.DeletePropagationBackground),
		}); err != nil && !aerrors.IsNotFound(err) {
			return reconcileFailed("failed to delete PVC: %s/%s: %w", pvc.GetNamespace(), pvc.GetName(), err)
		}
	}

	return nil
}

func (r *MantleBackupReconciler) deleteVerifyJob(ctx context.Context, backup *mantlev1.MantleBackup) *reconcileResult {
	var job batchv1.Job
	job.SetName(MakeVerifyJobName(backup))
	job.SetNamespace(r.managedCephClusterID)
	if err := r.Delete(ctx, &job, &client.DeleteOptions{
		PropagationPolicy: ptr.To(metav1.DeletePropagationBackground),
	}); err != nil && !aerrors.IsNotFound(err) {
		return reconcileFailed("failed to delete verify Job: %s/%s: %w", job.GetNamespace(), job.GetName(), err)
	}

	return nil
}

func (r *MantleBackupReconciler) deleteVerifyPV(ctx context.Context, backup *mantlev1.MantleBackup) *reconcileResult {
	var pv corev1.PersistentVolume
	pv.SetName(MakeVerifyPVName(backup))
	if err := r.Delete(ctx, &pv); err != nil && !aerrors.IsNotFound(err) {
		return reconcileFailed("failed to delete verify PV: %w", err)
	}

	return nil
}

func (r *MantleBackupReconciler) deleteVerifyPVC(ctx context.Context, backup *mantlev1.MantleBackup) *reconcileResult {
	var pvc corev1.PersistentVolumeClaim
	pvc.SetName(MakeVerifyPVCName(backup))
	pvc.SetNamespace(r.managedCephClusterID)
	if err := r.Delete(ctx, &pvc); err != nil && !aerrors.IsNotFound(err) {
		return reconcileFailed("failed to delete verify PVC: %w", err)
	}

	return nil
}

func (r *MantleBackupReconciler) deleteVerifyRBDImage(backup *mantlev1.MantleBackup) *reconcileResult {
	if backup.Status.PVManifest == "" {
		// The RBD image for verification is created after the PV manifest is stored in the status.
		// Thus, when the PV manifest is missing, the RBD image hasn't been created, so we can safely return nil.
		return nil
	}
	var pv corev1.PersistentVolume
	if err := json.Unmarshal([]byte(backup.Status.PVManifest), &pv); err != nil {
		return reconcileFailed("failed to unmarshal PV manifest: %w", err)
	}
	pool := pv.Spec.CSI.VolumeAttributes["pool"]
	if pool == "" {
		return reconcileFailed("pool name is missing in the PV manifest")
	}
	image := MakeVerifyImageName(backup)

	if err := deleteRBDImageAsynchronously(r.ceph, pool, image); err != nil {
		return reconcileFailed("failed to delete verify RBD image: %w", err)
	}

	return nil
}

func (r *MantleBackupReconciler) secondaryCleanup(
	ctx context.Context,
	target *mantlev1.MantleBackup,
	deleteExportData bool,
) *reconcileResult {
	logger := log.FromContext(ctx)
	logger.Info("starting cleanup in secondary", "backupUID", string(target.GetUID()), "deleteExportData", deleteExportData)

	diffFrom, ok := target.GetAnnotations()[annotDiffFrom]
	if ok {
		var source mantlev1.MantleBackup
		if err := r.Get(
			ctx,
			types.NamespacedName{Name: diffFrom, Namespace: target.GetNamespace()},
			&source,
		); err != nil {
			return reconcileFailed("failed to get source MantleBackup: %w", err)
		}
		delete(source.GetAnnotations(), annotDiffTo)
		if err := r.Update(ctx, &source); err != nil {
			return reconcileFailed("failed to update source MantleBackup: %w", err)
		}
	}

	if result := r.deleteAllImportJobs(ctx, target); result.shouldReturn() {
		return result.wrapIfError("failed to delete import Jobs")
	}

	var zeroOutJob batchv1.Job
	zeroOutJob.SetName(MakeZeroOutJobName(target))
	zeroOutJob.SetNamespace(r.managedCephClusterID)
	if err := r.Delete(ctx, &zeroOutJob, &client.DeleteOptions{
		PropagationPolicy: ptr.To(metav1.DeletePropagationBackground),
	}); err != nil && !aerrors.IsNotFound(err) {
		return reconcileFailed("failed to delete zeroout Job: %w", err)
	}

	var zeroOutPVC corev1.PersistentVolumeClaim
	zeroOutPVC.SetName(MakeZeroOutPVCName(target))
	zeroOutPVC.SetNamespace(r.managedCephClusterID)
	if err := r.Delete(ctx, &zeroOutPVC); err != nil && !aerrors.IsNotFound(err) {
		return reconcileFailed("failed to delete zeroout PVC: %w", err)
	}

	var zeroOutPV corev1.PersistentVolume
	zeroOutPV.SetName(MakeZeroOutPVName(target))
	if err := r.Delete(ctx, &zeroOutPV); err != nil && !aerrors.IsNotFound(err) {
		return reconcileFailed("failed to delete zeroout PV: %w", err)
	}

	if result := r.deleteVerifyJob(ctx, target); result.shouldReturn() {
		return result.wrapIfError("failed to delete verify Job")
	}
	if result := r.deleteVerifyPVC(ctx, target); result.shouldReturn() {
		return result.wrapIfError("failed to delete verify PVC")
	}
	if result := r.deleteVerifyPV(ctx, target); result.shouldReturn() {
		return result.wrapIfError("failed to delete verify PV")
	}
	if result := r.deleteVerifyRBDImage(target); result.shouldReturn() {
		return result.wrapIfError("failed to delete verify RBD image")
	}

	if deleteExportData {
		if result := r.deleteAllExportedData(ctx, target); result.shouldReturn() {
			return result.wrapIfError("failed to delete exported data in the object storage")
		}
	}

	delete(target.GetAnnotations(), annotDiffFrom)
	delete(target.GetAnnotations(), annotSyncMode)
	if err := r.Update(ctx, target); err != nil {
		return reconcileFailed("failed to update target MantleBackup: %w", err)
	}

	if result := r.deleteMiddleSnapshots(target); result.shouldReturn() {
		return result.wrapIfError("failed to delete middle snapshots")
	}

	return nil
}

func (r *MantleBackupReconciler) deleteAllImportJobs(ctx context.Context, backup *mantlev1.MantleBackup) *reconcileResult {
	return r.deleteAllJobsOfComponent(ctx, backup, MakeImportJobName)
}

func (r *MantleBackupReconciler) deleteAllExportedData(ctx context.Context, backup *mantlev1.MantleBackup) *reconcileResult {
	numParts, result := r.getNumberOfPartsForResourceDeletion(backup)
	if result.shouldReturn() {
		return result.wrapIfError("failed to get the number of the parts of the exported data")
	}

	for partNum := range numParts {
		key := MakeObjectNameOfExportedData(backup.GetName(), backup.GetAnnotations()[annotRemoteUID], partNum)
		if err := r.objectStorageClient.Delete(ctx, key); err != nil {
			return reconcileFailed("failed to delete exported data in the object storage: %s: %w", key, err)
		}
	}

	return nil
}

func (r *MantleBackupReconciler) deleteMiddleSnapshots(backup *mantlev1.MantleBackup) *reconcileResult {
	// Check that middle snapshots can exist
	_, ok := backup.GetAnnotations()[annotRemoteUID]
	if !ok {
		return nil
	}
	if backup.Status.PVManifest == "" || backup.Status.TransferPartSize == nil || backup.Status.SnapSize == nil {
		return nil
	}

	numParts, result := r.getNumberOfPartsForResourceDeletion(backup)
	if result.shouldReturn() {
		return result.wrapIfError("failed to get number of parts: %s/%s", backup.GetNamespace(), backup.GetName())
	}

	transferPartSize, ok := backup.Status.TransferPartSize.AsInt64()
	if !ok {
		return reconcileFailed("failed to get transferPartSize as int64: %s/%s", backup.GetNamespace(), backup.GetName())
	}

	pool, image, result := r.getPoolAndImageFromStatusPVManifest(backup)
	if result.shouldReturn() {
		return result.wrapIfError("failed to get pool and image from status.PVManifest")
	}

	imageNames, err := r.ceph.RBDLs(pool)
	if err != nil {
		return reconcileFailed("failed to list RBD images: %w", err)
	}

	var imageID string
	var snaps []ceph.RBDSnapshot
	if slices.Contains(imageNames, image) {
		info, err := r.ceph.RBDInfo(pool, image)
		if err != nil {
			return reconcileFailed("failed to get RBD image info: %s: %s: %w", pool, image, err)
		}
		imageID = info.ID
		snaps, err = r.ceph.RBDSnapLs(pool, image)
		if err != nil {
			return reconcileFailed("failed to list snapshots: %s: %s: %w", pool, image, err)
		}
	} else {
		trashList, err := r.ceph.RBDTrashLs(pool)
		if err != nil {
			return reconcileFailed("failed to list RBD trash: %w", err)
		}
		for _, trash := range trashList {
			if trash.Name == image {
				imageID = trash.ID

				break
			}
		}
		if imageID == "" {
			// Image not found in active images or trash; snapshots are already gone.
			return nil
		}
		snaps, err = r.ceph.RBDSnapLsByID(pool, imageID)
		if err != nil {
			return reconcileFailed("failed to list snapshots by ID: %s: %s: %w", pool, imageID, err)
		}
	}

	for i := range numParts {
		snapIndex := slices.IndexFunc(snaps, func(snap ceph.RBDSnapshot) bool {
			return snap.Name == MakeMiddleSnapshotName(backup, i*int(transferPartSize))
		})
		if snapIndex == -1 {
			continue
		}
		if err := r.ceph.RBDSnapRm(pool, imageID, snaps[snapIndex].Name); err != nil {
			return reconcileFailed("failed to remove snapshot: %s: %s: %w", pool, image, err)
		}
	}

	return nil
}

func (r *MantleBackupReconciler) updateMantleBackupCondition(
	ctx context.Context,
	target *mantlev1.MantleBackup,
	conditionType string,
	status metav1.ConditionStatus,
	reason string,
) *reconcileResult {
	// Update the status of the MantleBackup. Use Patch here because Update() is
	// likely to fail due to "the object has been modified" error.
	willBeApplied := target.DeepCopy()
	meta.SetStatusCondition(&willBeApplied.Status.Conditions, metav1.Condition{
		Type:   conditionType,
		Status: status,
		Reason: reason,
	})
	if err := r.Client.Status().Patch(ctx, willBeApplied, client.MergeFrom(target)); err != nil {
		return reconcileFailed("failed to update MantleBackup condition by patch: %w", err)
	}

	return nil
}
