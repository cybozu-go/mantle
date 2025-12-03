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
	mantleVerifyJobPrefix     = "mantle-verify-"
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

// MantleBackupReconciler reconciles a MantleBackup object
type MantleBackupReconciler struct {
	client.Client
	Scheme                 *runtime.Scheme
	ceph                   ceph.CephCmd
	managedCephClusterID   string
	role                   string
	primarySettings        *PrimarySettings // This should be non-nil if and only if role equals 'primary'.
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
		expireQueueCh:          make(chan event.GenericEvent),
		podImage:               podImage,
		envSecret:              envSecret,
		objectStorageSettings:  objectStorageSettings,
		proxySettings:          proxySettings,
		backupTransferPartSize: backupTransferPartSize,
	}
}

func (r *MantleBackupReconciler) removeRBDSnapshot(ctx context.Context, poolName, imageName, snapshotName string) error {
	logger := log.FromContext(ctx)
	rmErr := r.ceph.RBDSnapRm(poolName, imageName, snapshotName)
	if rmErr != nil {
		_, findErr := ceph.FindRBDSnapshot(r.ceph, poolName, imageName, snapshotName)
		if findErr == nil || !errors.Is(findErr, ceph.ErrSnapshotNotFound) {
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
		return nil, errors.Join(createErr, findErr)
	}
	return snap, nil
}

func (r *MantleBackupReconciler) checkPVCInManagedCluster(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	logger := log.FromContext(ctx)
	clusterID, err := getCephClusterIDFromPVC(ctx, r.Client, pvc)
	if err != nil {
		logger.Error(err, "failed to get clusterID from PVC", "namespace", pvc.Namespace, "name", pvc.Name)
		return err
	}
	if clusterID != r.managedCephClusterID {
		logger.Info("clusterID not matched", "pvc", pvc.Name, "clusterID", clusterID, "managedCephClusterID", r.managedCephClusterID)
		return errSkipProcessing
	}

	return nil
}

func (r *MantleBackupReconciler) isPVCBound(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (bool, error) {
	logger := log.FromContext(ctx)
	if pvc.Status.Phase != corev1.ClaimBound {
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
		return nil, ctrl.Result{}, fmt.Errorf("failed to get PVC: %w", err)
	}

	// Return an error if the PVC has been re-created after the first call.
	if uid, ok := backup.GetLabels()[labelLocalBackupTargetPVCUID]; ok && uid != string(pvc.GetUID()) {
		return nil, ctrl.Result{}, fmt.Errorf("PVC UID does not match the backup target")
	}

	if err := r.checkPVCInManagedCluster(ctx, &pvc); err != nil {
		return nil, ctrl.Result{}, err
	}

	ok, err := r.isPVCBound(ctx, &pvc)
	if err != nil {
		return nil, ctrl.Result{}, err
	}
	if !ok {
		logger.Info("waiting for PVC bound.")
		return nil, requeueReconciliation(), nil
	}

	pvName := pvc.Spec.VolumeName
	var pv corev1.PersistentVolume
	err = r.Get(ctx, types.NamespacedName{Name: pvName}, &pv)
	if err != nil {
		logger.Error(err, "failed to get PV", "name", pvName)
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

	target, result, err := r.getSnapshotTarget(ctx, backup)
	notFound := aerrors.IsNotFound(err)
	switch {
	case errors.Is(err, errSkipProcessing):
		return ctrl.Result{}, nil
	case err != nil && !notFound:
		return ctrl.Result{}, err
	}
	if !result.IsZero() {
		return result, nil
	}
	if !backup.DeletionTimestamp.IsZero() {
		return r.finalizeStandalone(ctx, backup, target, notFound)
	}
	// Only the NotFound error reaches this point, so return it as is.
	if notFound {
		return ctrl.Result{}, err
	}
	if backup.Labels == nil {
		backup.Labels = make(map[string]string)
	}

	if _, ok := backup.Labels[labelLocalBackupTargetPVCUID]; !ok {
		backup.Labels[labelLocalBackupTargetPVCUID] = string(target.pvc.GetUID())

		if err := r.Update(ctx, backup); err != nil {
			logger.Error(err, "failed to add label", "label", labelLocalBackupTargetPVCUID)
			return ctrl.Result{}, err
		}
	}

	if !controllerutil.ContainsFinalizer(backup, MantleBackupFinalizerName) {
		controllerutil.AddFinalizer(backup, MantleBackupFinalizerName)

		if err := r.Update(ctx, backup); err != nil {
			logger.Error(err, "failed to add finalizer", "finalizer", MantleBackupFinalizerName)
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	if err := r.expire(ctx, backup); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.provisionRBDSnapshot(ctx, backup, target); err != nil {
		return ctrl.Result{}, err
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

	if err := r.prepareObjectStorageClient(ctx); err != nil {
		return ctrl.Result{}, err
	}

	if !isCreatedWhenMantleControllerWasSecondary(backup) {
		logger.Info(
			"skipping to reconcile the MantleBackup created by a different mantle-controller to prevent accidental data loss",
		)
		return ctrl.Result{}, nil
	}

	target, result, err := r.getSnapshotTarget(ctx, backup)
	notFound := aerrors.IsNotFound(err)
	switch {
	case errors.Is(err, errSkipProcessing):
		return ctrl.Result{}, nil
	case err != nil && !notFound:
		return ctrl.Result{}, err
	}
	if !result.IsZero() {
		return result, nil
	}
	if !backup.DeletionTimestamp.IsZero() {
		return r.finalizeSecondary(ctx, backup, target, notFound)
	}
	// Only the NotFound error reaches this point, so return it as is.
	if notFound {
		return ctrl.Result{}, err
	}

	if err := r.expire(ctx, backup); err != nil {
		return ctrl.Result{}, err
	}

	if backup.IsReady() {
		return r.secondaryCleanup(ctx, backup, true)
	}

	return r.startImport(ctx, backup, target)
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
) (ctrl.Result, error) {
	// Skip replication if SyncedToRemote condition is true.
	if backup.IsSynced() {
		return ctrl.Result{}, nil
	}

	result, err := r.replicateManifests(ctx, backup)
	if err != nil || result != (ctrl.Result{}) {
		return result, err
	}
	prepareResult, err := r.prepareForDataSynchronization(ctx, backup, r.primarySettings.Client)
	if err != nil {
		return ctrl.Result{}, err
	}

	if prepareResult.isSecondaryMantleBackupReadyToUse {
		return r.primaryCleanup(ctx, backup)
	}
	return r.startExportAndUpload(ctx, backup, prepareResult)
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
	if err := r.List(ctx, &backupList, &client.ListOptions{
		LabelSelector: labels.SelectorFromSet(map[string]string{labelLocalBackupTargetPVCUID: string(pvc.GetUID())}),
	}); err != nil {
		return ctrl.Result{}, err
	}
	for _, backup1 := range backupList.Items {
		if backup1.Status.SnapID == nil ||
			*backup1.Status.SnapID < *backup.Status.SnapID &&
				backup1.DeletionTimestamp.IsZero() &&
				!backup1.IsSynced() {
			return requeueReconciliation(), nil
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
	capacity, err := resource.ParseQuantity(fmt.Sprintf("%d", *backup.Status.SnapSize))
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to parse quantity: %w", err)
	}
	pvcSent.Spec.Resources = corev1.VolumeResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceStorage: capacity,
		},
	}
	pvcSentJson, err := json.Marshal(pvcSent)
	if err != nil {
		return ctrl.Result{}, err
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
	backupSent.Status.SnapSize = backup.Status.SnapSize
	backupSent.Status.TransferPartSize = backup.Status.TransferPartSize
	backupSentJson, err := json.Marshal(backupSent)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Call CreateMantleBackup.
	if _, err := client.CreateMantleBackup(
		ctx,
		&proto.CreateMantleBackupRequest{
			MantleBackup: backupSentJson,
		},
	); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

//nolint:unused // TODO: Delete this line after starting use.
func (r *MantleBackupReconciler) verify(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
) error {
	var storedPVC corev1.PersistentVolumeClaim
	if err := json.Unmarshal([]byte(backup.Status.PVCManifest), &storedPVC); err != nil {
		return fmt.Errorf("failed to unmarshal PVC manifest: %w", err)
	}

	var storedPV corev1.PersistentVolume
	if err := json.Unmarshal([]byte(backup.Status.PVManifest), &storedPV); err != nil {
		return fmt.Errorf("failed to unmarshal PV manifest: %w", err)
	}

	// create a clone by the snapshot bound to the MB
	if err := createCloneByPV(ctx, r.ceph, &storedPV, backup.Name, makeVerifyImageName(backup)); err != nil {
		return fmt.Errorf("failed to create a clone by the snapshot: %w", err)
	}

	// create a static PV with the clone
	if err := r.createOrUpdateStaticPV(
		ctx,
		&storedPV,
		makeVerifyImageName(backup),
		corev1.ResourceList{
			corev1.ResourceStorage: *resource.NewQuantity(*backup.Status.SnapSize, resource.BinarySI),
		},
		makeVerifyPVName(backup),
		labelComponentVerifyVolume,
		true,
	); err != nil {
		return fmt.Errorf("failed to create a static PV with the clone: %w", err)
	}

	// create a PVC with the PV
	if err := r.createOrUpdateStaticPVC(
		ctx,
		makeVerifyPVCName(backup),
		makeVerifyPVName(backup),
		labelComponentVerifyVolume,
		storedPVC.Spec.Resources,
	); err != nil {
		return fmt.Errorf("failed to create a PVC with the PV: %w", err)
	}

	// create a Job to execute e2fsck on the PVC
	if err := r.createOrUpdateVerifyJob(
		ctx,
		makeVerifyJobName(backup),
		makeVerifyPVCName(backup),
	); err != nil {
		return fmt.Errorf("failed to create a Job to execute e2fsck on the PVC: %w", err)
	}

	// wait for the Job to complete
	jobFinished, jobSucceeded, err := r.checkJobStatus(ctx, makeVerifyJobName(backup))
	if err != nil {
		return fmt.Errorf("failed to check the Job status: %w", err)
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
	if err := r.updateMantleBackupCondition(
		ctx, backup,
		mantlev1.BackupConditionVerified,
		condition,
		reason,
	); err != nil {
		return fmt.Errorf("failed to update MantleBackup condition: %w", err)
	}

	return nil
}

// checkJobStatus checks the status of the Job with the given name.
// It returns (is job finished, is job succeeded, error).
//
//nolint:unused // TODO: Delete this line after starting use `verify`.
func (r *MantleBackupReconciler) checkJobStatus(ctx context.Context, jobName string) (bool, bool, error) {
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
		return false, false, fmt.Errorf("failed to get Job for checking job status: %w", err)
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
) error {
	logger := log.FromContext(ctx)

	// Attach local-backup-target-pvc-uid label before trying to create a RBD
	// snapshot corresponding to the given MantleBackup, so that we can make
	// sure that every MantleBackup that has a RBD snapshot is labelled with
	// local-backup-target-pvc-uid.
	key := client.ObjectKeyFromObject(backup)
	if err := r.Get(ctx, key, backup); err != nil {
		return err
	}
	if backup.Labels == nil {
		backup.Labels = map[string]string{}
	}
	backup.Labels[labelLocalBackupTargetPVCUID] = string(target.pvc.GetUID())
	if err := r.Update(ctx, backup); err != nil {
		return err
	}

	// If the given MantleBackup is not ready to use, create a new RBD snapshot and update its status.
	if backup.IsReady() {
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
		backup.Status.SnapSize = &snapshot.Size

		meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
			Type: mantlev1.BackupConditionReadyToUse, Status: metav1.ConditionTrue, Reason: mantlev1.ConditionReasonReadyToUseNoProblem})
		return nil
	}); err != nil {
		logger.Error(err, "failed to update MantleBackup status", "status", backup.Status)
		return err
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
	target *snapshotTarget,
	targetPVCNotFound bool,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	if _, ok := backup.GetAnnotations()[annotDiffTo]; ok {
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(backup, MantleBackupFinalizerName) {
		return ctrl.Result{}, nil
	}

	// primaryClean() is called in finalizeStandalone() to delete resources for
	// exported and uploaded snapshots in both standalone and primary Mantle.
	result, err := r.primaryCleanup(ctx, backup)
	if err != nil || result != (ctrl.Result{}) {
		return result, err
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
	logger.Info("succeeded to delete a backup")

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
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(backup, MantleBackupFinalizerName) {
		return ctrl.Result{}, nil
	}

	result, err := r.secondaryCleanup(ctx, backup, false)
	if err != nil || result != (ctrl.Result{}) {
		return result, err
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

	secondaryBackup, ok := secondaryBackupMap[backup.GetName()]
	if !ok {
		return nil, fmt.Errorf("secondary MantleBackup not found: %s, %s",
			backup.GetName(), backup.GetNamespace())
	}
	isSecondaryMantleBackupReadyToUse := secondaryBackup.IsReady()

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
			err = r.Get(ctx, types.NamespacedName{
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
	err = r.List(ctx, &primaryBackupList, &client.ListOptions{
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
		if !primaryBackup.IsReady() || !secondaryBackup.IsReady() {
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
) (ctrl.Result, error) {
	sourceBackup := prepareResult.diffFrom
	var sourceBackupName *string
	if sourceBackup != nil {
		s := sourceBackup.GetName()
		sourceBackupName = &s
	}

	if err := r.annotateExportTargetMantleBackup(
		ctx, targetBackup, prepareResult.isIncremental, sourceBackupName,
	); err != nil {
		return ctrl.Result{}, err
	}

	if prepareResult.isIncremental {
		if err := r.annotateExportSourceMantleBackup(ctx, sourceBackup, targetBackup); err != nil {
			return ctrl.Result{}, err
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
		return ctrl.Result{}, err
	}

	largestCompletedExportPartNum, err := r.startExport(ctx, targetBackup, sourceBackupName)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to export: %w", err)
	}

	if err := r.startUpload(ctx, targetBackup, largestCompletedExportPartNum); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to upload MantleBackup: %w", err)
	}

	return requeueReconciliation(), nil
}

func (r *MantleBackupReconciler) annotateExportTargetMantleBackup(
	ctx context.Context,
	target *mantlev1.MantleBackup,
	incremental bool,
	sourceName *string,
) error {
	key := client.ObjectKeyFromObject(target)
	if err := r.Get(ctx, key, target); err != nil {
		return err
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

	return r.Update(ctx, target)
}

func (r *MantleBackupReconciler) annotateExportSourceMantleBackup(
	ctx context.Context,
	source *mantlev1.MantleBackup,
	target *mantlev1.MantleBackup,
) error {
	key := client.ObjectKeyFromObject(source)
	if err := r.Get(ctx, key, source); err != nil {
		return err
	}

	annot := source.GetAnnotations()
	if annot == nil {
		annot = map[string]string{}
	}
	annot[annotDiffTo] = target.GetName()
	source.SetAnnotations(annot)

	return r.Update(ctx, source)
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
) (int, error) {
	largestCompletedPartNum, err := r.handleCompletedExportJobs(ctx, targetBackup)
	if err != nil {
		return -1, fmt.Errorf("failed to handle completed export jobs: %w", err)
	}

	if ok, err := r.canNewExportJobBeCreated(ctx); err != nil {
		return -1, fmt.Errorf("failed to check if a new export Job can be created: %w", err)
	} else if !ok {
		// skip to create an export Job
		return largestCompletedPartNum, nil
	}

	if err := r.addStatusTransferPartSizeIfEmpty(ctx, targetBackup); err != nil {
		return -1, fmt.Errorf("failed to patch .status.transferPartSize: %w", err)
	}

	if ok, err := r.haveAllExportJobsCompleted(targetBackup, largestCompletedPartNum); err != nil {
		return -1, fmt.Errorf("failed to check if all export Jobs are completed: %w", err)
	} else if ok {
		return largestCompletedPartNum, nil
	}

	if err := r.createOrUpdateExportDataPVC(ctx, targetBackup, largestCompletedPartNum); err != nil {
		return -1, fmt.Errorf("failed to create or update export data PVC: %w", err)
	}

	if err := r.createOrUpdateExportJob(ctx, targetBackup, sourceBackupName, largestCompletedPartNum); err != nil {
		return -1, fmt.Errorf("failed to create or update export Job: %w", err)
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
) (int, error) {
	// List all the Jobs
	var jobList batchv1.JobList
	if err := r.List(ctx, &jobList, &client.ListOptions{
		Namespace: r.managedCephClusterID,
		LabelSelector: labels.SelectorFromSet(map[string]string{
			"app.kubernetes.io/name":      labelAppNameValue,
			"app.kubernetes.io/component": componentLabel,
		}),
	}); err != nil {
		return -1, fmt.Errorf("failed to list Jobs: %w", err)
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
			return -1, fmt.Errorf("failed to delete Job: %s: %w", job.job.GetName(), err)
		}

		if hookPostJobDeletion != nil {
			if err := (*hookPostJobDeletion)(job.partNum); err != nil {
				return -1, fmt.Errorf("hookPostJobDeletion failed: %w", err)
			}
		}
	}

	return largestPartNum, nil
}

func IsPartNextToLargestCompletedPart(largestCompletedPartNum *int, partNum int) bool {
	return (largestCompletedPartNum == nil && partNum != 0) ||
		(largestCompletedPartNum != nil && partNum != *largestCompletedPartNum+1)
}

func (r *MantleBackupReconciler) handleCompletedExportJobs(ctx context.Context, backup *mantlev1.MantleBackup) (int, error) {
	return r.handleCompletedJobsOfComponent(ctx, backup, labelComponentExportJob, MantleExportJobPrefix, nil)
}

func (r *MantleBackupReconciler) canNewExportJobBeCreated(ctx context.Context) (bool, error) {
	if r.primarySettings.MaxExportJobs == 0 {
		return true, nil
	}

	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, &client.ListOptions{
		Namespace: r.managedCephClusterID,
		LabelSelector: labels.SelectorFromSet(map[string]string{
			"app.kubernetes.io/name":      labelAppNameValue,
			"app.kubernetes.io/component": labelComponentExportJob,
		}),
	}); err != nil {
		return false, fmt.Errorf("failed to list export Jobs: %w", err)
	}

	// exclude completed jobs
	count := 0
	for _, job := range jobs.Items {
		if !IsJobConditionTrue(job.Status.Conditions, batchv1.JobComplete) {
			count++
		}
	}

	if count >= r.primarySettings.MaxExportJobs {
		return false, nil
	}

	return true, nil
}

func (r *MantleBackupReconciler) getPartNumRangeOfExpectedRunningUploadJobs(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	exportedPartNum,
	uploadedPartNum int,
) (int, int, error) {
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
		return 0, 0, fmt.Errorf("failed to list upload Jobs: %w", err)
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

func (r *MantleBackupReconciler) addStatusTransferPartSizeIfEmpty(ctx context.Context, backup *mantlev1.MantleBackup) error {
	if backup.Status.TransferPartSize != nil {
		return nil
	}

	// Use PATCH here in order not to update backup with stale values.
	oldBackup := backup.DeepCopy()
	backup.Status.TransferPartSize = &r.backupTransferPartSize
	if err := r.Client.Status().Patch(ctx, backup, client.MergeFrom(oldBackup)); err != nil {
		return fmt.Errorf("failed to patch .status.transferPartSize: %s: %w", r.backupTransferPartSize.String(), err)
	}

	return nil
}

func (r *MantleBackupReconciler) getPoolAndImageFromStatusPVManifest(backup *mantlev1.MantleBackup) (string, string, error) {
	var pv corev1.PersistentVolume
	if err := json.Unmarshal([]byte(backup.Status.PVManifest), &pv); err != nil {
		return "", "", fmt.Errorf("failed to unmarshal status.PVManifest: %w", err)
	}
	return pv.Spec.CSI.VolumeAttributes["pool"], pv.Spec.CSI.VolumeAttributes["imageName"], nil
}

func (r *MantleBackupReconciler) getNumberOfParts(backup *mantlev1.MantleBackup) (int, error) {
	if backup.Status.SnapSize == nil {
		return 0, fmt.Errorf("failed to get status.snapSize: %s/%s", backup.GetNamespace(), backup.GetName())
	}
	if backup.Status.TransferPartSize == nil {
		return 0, fmt.Errorf("failed to get status.transferPartSize: %s/%s", backup.GetNamespace(), backup.GetName())
	}

	transferPartSize, ok := backup.Status.TransferPartSize.AsInt64()
	if !ok {
		return 0, fmt.Errorf("failed to convert transferPartSize to int64: %s/%s: %s",
			backup.GetNamespace(), backup.GetName(), backup.Status.TransferPartSize.String())
	}

	numParts := *backup.Status.SnapSize / transferPartSize
	if *backup.Status.SnapSize%transferPartSize != 0 {
		numParts++
	}
	return int(numParts), nil
}

func (r *MantleBackupReconciler) haveAllExportJobsCompleted(backup *mantlev1.MantleBackup, largestCompletedPartNum int) (bool, error) {
	limit, err := r.getNumberOfParts(backup)
	if err != nil {
		return false, fmt.Errorf("failed to get the number of the parts of the exported data: %w", err)
	}
	return largestCompletedPartNum+1 == limit, nil
}

func (r *MantleBackupReconciler) startUpload(ctx context.Context, targetBackup *mantlev1.MantleBackup, largestCompletedExportPartNum int) error {
	largestCompletedUploadPartNum, err := r.handleCompletedUploadJobs(ctx, targetBackup)
	if err != nil {
		return fmt.Errorf("failed to handle completed upload jobs: %w", err)
	}

	if err := r.createOrUpdateUploadJobs(
		ctx,
		targetBackup,
		largestCompletedExportPartNum,
		largestCompletedUploadPartNum,
	); err != nil {
		return fmt.Errorf("failed to create or update upload jobs: %w", err)
	}

	return nil
}

func (r *MantleBackupReconciler) handleCompletedUploadJobs(ctx context.Context, backup *mantlev1.MantleBackup) (int, error) {
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

	if pvcSizeI64 < 512*1024*1024 {
		pvcSizeI64 = 512 * 1024 * 1024
	}
	pvcSizeI64 = int64(float64(pvcSizeI64) * 1.2)

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
) error {
	var targetPVC corev1.PersistentVolumeClaim
	if err := json.Unmarshal([]byte(target.Status.PVCManifest), &targetPVC); err != nil {
		return err
	}

	pvcSize, err := calculateExportDataPVCSize(target.Status.TransferPartSize)
	if err != nil {
		return fmt.Errorf("failed to calculate export data PVC size: %s/%s: %w",
			target.GetNamespace(), target.GetName(), err)
	}

	var pvc corev1.PersistentVolumeClaim
	pvc.SetName(MakeExportDataPVCName(target, largestCompletedPartNum+1))
	pvc.SetNamespace(r.managedCephClusterID)
	_, err = ctrl.CreateOrUpdate(ctx, r.Client, &pvc, func() error {
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

		pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
		pvc.Spec.StorageClassName = &r.primarySettings.ExportDataStorageClass

		return nil
	})

	return err
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

//nolint:unused // TODO: Delete this line after starting use.
func makeVerifyImageName(target *mantlev1.MantleBackup) string {
	return mantleVerifyImagePrefix + string(target.GetUID())
}

//nolint:unused // TODO: Delete this line after starting use.
func makeVerifyJobName(target *mantlev1.MantleBackup) string {
	return mantleVerifyJobPrefix + string(target.GetUID())
}

//nolint:unused // TODO: Delete this line after starting use.
func makeVerifyPVCName(target *mantlev1.MantleBackup) string {
	return mantleVerifyPVCPrefix + string(target.GetUID())
}

//nolint:unused // TODO: Delete this line after starting use.
func makeVerifyPVName(target *mantlev1.MantleBackup) string {
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
) error {
	sourceBackupName := ""
	if sourceBackupNamePtr != nil {
		sourceBackupName = *sourceBackupNamePtr
	}

	poolName, imageName, err := r.getPoolAndImageFromStatusPVManifest(target)
	if err != nil {
		return fmt.Errorf("failed to get pool and image from .status.PVManifest: %w", err)
	}

	partNum := largestCompletedPartNum + 1

	transferPartSize, ok := target.Status.TransferPartSize.AsInt64()
	if !ok {
		return fmt.Errorf("failed to convert transferPartSize to int64: %d", transferPartSize)
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
		return err
	}

	return nil
}

func (r *MantleBackupReconciler) createOrUpdateUploadJobs(
	ctx context.Context,
	target *mantlev1.MantleBackup,
	largestCompletedExportPartNum,
	largestCompletedUploadPartNum int,
) error {
	minPartNum, maxPartNum, err := r.getPartNumRangeOfExpectedRunningUploadJobs(
		ctx,
		target,
		largestCompletedExportPartNum,
		largestCompletedUploadPartNum,
	)
	if err != nil {
		return fmt.Errorf("failed to get part num range of runnable upload jobs: %w", err)
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
						Value: fmt.Sprintf("/mantle_ca_cert/%s", *r.objectStorageSettings.CACertKey),
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
			return err
		}
	}

	return nil
}

func (r *MantleBackupReconciler) startImport(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	target *snapshotTarget,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if !r.doesMantleBackupHaveSyncModeAnnot(backup) {
		// SetSynchronizing is not called yet or the cache is stale.
		return ctrl.Result{}, nil
	}

	if uploaded, err := r.isExportDataAlreadyUploaded(ctx, backup, 0); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to check if export data part 0 is already uploaded: %w", err)
	} else if !uploaded {
		return requeueReconciliation(), nil
	}

	largestCompletedPartNum, err := r.handleCompletedImportJobs(ctx, backup)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to handle completed import jobs: %w", err)
	}

	// Requeue if the PV is smaller than the PVC. (This may be the case if pvc-autoresizer is used.)
	if isPVSmallerThanPVC(target.pv, target.pvc) {
		return requeueReconciliation(), nil
	}

	if err := r.updateStatusManifests(ctx, backup, target.pv, target.pvc); err != nil {
		return ctrl.Result{}, err
	}

	succeed, err := r.lockVolume(target.poolName, target.imageName, string(backup.GetUID()))
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to lock the volume: %w", err)
	}
	if !succeed {
		logger.Info("the volume is locked by another process", "uid", string(backup.GetUID()))
		return requeueReconciliation(), nil
	}

	if result, err := r.reconcileZeroOutJob(ctx, backup, target); err != nil || !result.IsZero() {
		return result, err
	}

	if result, err := r.reconcileImportJob(ctx, backup, target, largestCompletedPartNum); err != nil || !result.IsZero() {
		return result, err
	}

	if err := r.unlockVolume(target.poolName, target.imageName, string(backup.GetUID())); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to unlock the volume: %w", err)
	}

	return ctrl.Result{}, nil
}

func (r *MantleBackupReconciler) doesMantleBackupHaveSyncModeAnnot(backup *mantlev1.MantleBackup) bool {
	annots := backup.GetAnnotations()
	syncMode, ok := annots[annotSyncMode]
	return ok && (syncMode == syncModeFull || syncMode == syncModeIncremental)
}

func (r *MantleBackupReconciler) prepareObjectStorageClient(ctx context.Context) error {
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
			return err
		}

		caPEMCertsString, ok := cm.Data[*r.objectStorageSettings.CACertKey]
		if !ok {
			return fmt.Errorf("ca-cert-key not found in ConfigMap: %s", *r.objectStorageSettings.CACertConfigMap)
		}
		caPEMCerts = []byte(caPEMCertsString)
	}

	var envSecret corev1.Secret
	if err := r.Get(
		ctx,
		types.NamespacedName{Name: r.envSecret, Namespace: r.managedCephClusterID},
		&envSecret,
	); err != nil {
		return err
	}
	accessKeyID, ok := envSecret.Data["AWS_ACCESS_KEY_ID"]
	if !ok {
		return errors.New("failed to find AWS_ACCESS_KEY_ID in env-secret")
	}
	secretAccessKey, ok := envSecret.Data["AWS_SECRET_ACCESS_KEY"]
	if !ok {
		return errors.New("failed to find AWS_SECRET_ACCESS_KEY in env-secret")
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
		return err
	}

	return nil
}

func (r *MantleBackupReconciler) isExportDataAlreadyUploaded(
	ctx context.Context,
	target *mantlev1.MantleBackup,
	index int,
) (bool, error) {
	key := MakeObjectNameOfExportedData(target.GetName(), target.GetAnnotations()[annotRemoteUID], index)
	uploaded, err := r.objectStorageClient.Exists(ctx, key)
	if err != nil {
		return false, fmt.Errorf("failed to check if an object exists in the object storage: %s: %w", key, err)
	}
	return uploaded, nil
}

func (r *MantleBackupReconciler) handleCompletedImportJobs(ctx context.Context, backup *mantlev1.MantleBackup) (int, error) {
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

// lockVolume adds a lock to the specified RBD volume if the lock is not already held.
// It returns true if the lock is held by this caller, false if another lock is held or an error occurs.
func (r *MantleBackupReconciler) lockVolume(
	poolName, imageName, lockID string,
) (bool, error) {
	// Add a lock.
	if errAdd := r.ceph.RBDLockAdd(poolName, imageName, lockID); errAdd != nil {
		locks, errLs := r.ceph.RBDLockLs(poolName, imageName)
		if errLs != nil {
			return false, fmt.Errorf("failed to add a lock and list locks on volume %s/%s: %w", poolName, imageName, errors.Join(errAdd, errLs))
		}

		switch len(locks) {
		case 0:
			// It may have been unlocked after the lock failed, but since other causes are also possible, an error is returned.
			return false, fmt.Errorf("failed to add a lock to the volume %s/%s: %w", poolName, imageName, errAdd)

		case 1:
			if locks[0].LockID == lockID {
				// Already locked by this MB.
				return true, nil
			}
			// Locked by another process.
			return false, nil

		default:
			// Multiple locks found; unexpected state.
			return false, fmt.Errorf("multiple locks found on volume %s/%s after failed lock attempt(%v)", poolName, imageName, locks)
		}
	}

	// Locked
	return true, nil
}

// unlockVolume removes the specified lock from the RBD volume if the lock is held.
// No action is taken if the lock is not found.
func (r *MantleBackupReconciler) unlockVolume(
	poolName, imageName, lockID string,
) error {
	// List up locks to check if the lock is held.
	locks, err := r.ceph.RBDLockLs(poolName, imageName)
	if err != nil {
		return fmt.Errorf("failed to list locks of the volume %s/%s: %w", poolName, imageName, err)
	}

	if len(locks) >= 2 {
		return fmt.Errorf("multiple locks found on volume %s/%s when unlocking (%v)", poolName, imageName, locks)
	}

	for _, lock := range locks {
		if lock.LockID == lockID {
			// Unlock
			if err := r.ceph.RBDLockRm(poolName, imageName, lock); err != nil {
				return fmt.Errorf("failed to remove the lock from the volume %s/%s: %w", poolName, imageName, err)
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
) (ctrl.Result, error) {
	if backup.GetAnnotations()[annotSyncMode] != syncModeFull {
		return ctrl.Result{}, nil
	}

	if err := r.createOrUpdateStaticPV(
		ctx,
		snapshotTarget.pv,
		snapshotTarget.pv.Spec.CSI.VolumeAttributes["imageName"],
		snapshotTarget.pv.Spec.Capacity,
		MakeZeroOutPVName(backup),
		labelComponentZeroOutVolume,
		false,
	); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.createOrUpdateStaticPVC(
		ctx,
		MakeZeroOutPVCName(backup),
		MakeZeroOutPVName(backup),
		labelComponentZeroOutVolume,
		snapshotTarget.pvc.Spec.Resources,
	); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.createOrUpdateZeroOutJob(ctx, backup); err != nil {
		return ctrl.Result{}, err
	}

	completed, err := r.hasZeroOutJobCompleted(ctx, backup)
	if err != nil {
		return ctrl.Result{}, err
	}
	if completed {
		return ctrl.Result{}, nil
	}
	return requeueReconciliation(), nil
}

// createOrUpdateStaticPV creates or updates a static PersistentVolume (PV) resource.
// It copies relevant CSI and metadata fields from the basePV, sets the provided volume handle,
// capacity, and component label, and ensures the PV is configured for static provisioning.
//
// Parameters:
//
//	ctx           - context for the API calls
//	basePV        - source PV to copy CSI and metadata fields from
//	volume        - volume handle to assign to the new PV
//	capacity      - resource capacity for the PV
//	newPvName     - name for the new or updated PV
//	componentName - label value for the component
//	addFlatten    - whether to add deep-flatten feature to the volume attributes
//
// Returns an error if creation or update fails.
func (r *MantleBackupReconciler) createOrUpdateStaticPV(
	ctx context.Context,
	basePV *corev1.PersistentVolume,
	volume string,
	capacity corev1.ResourceList,
	newPvName, componentName string,
	addFlatten bool,
) error {
	var pv corev1.PersistentVolume
	pv.SetName(newPvName)
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, &pv, func() error {
		labels := pv.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels["app.kubernetes.io/name"] = labelAppNameValue
		labels["app.kubernetes.io/component"] = componentName
		pv.SetLabels(labels)

		pv.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
		pv.Spec.Capacity = capacity
		pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
		pv.Spec.StorageClassName = ""
		pv.Spec.VolumeMode = ptr.To(corev1.PersistentVolumeBlock)

		if pv.Spec.CSI == nil {
			pv.Spec.CSI = &corev1.CSIPersistentVolumeSource{}
		}
		pv.Spec.CSI.Driver = basePV.Spec.CSI.Driver
		pv.Spec.CSI.ControllerExpandSecretRef = basePV.Spec.CSI.ControllerExpandSecretRef
		pv.Spec.CSI.NodeStageSecretRef = basePV.Spec.CSI.NodeStageSecretRef
		pv.Spec.CSI.VolumeHandle = volume

		if pv.Spec.CSI.VolumeAttributes == nil {
			pv.Spec.CSI.VolumeAttributes = map[string]string{}
		}
		feature := basePV.Spec.CSI.VolumeAttributes["imageFeatures"]
		if addFlatten {
			if len(feature) > 0 {
				feature = feature + ",deep-flatten"
			} else {
				feature = "deep-flatten"
			}
		}
		pv.Spec.CSI.VolumeAttributes["clusterID"] = basePV.Spec.CSI.VolumeAttributes["clusterID"]
		pv.Spec.CSI.VolumeAttributes["imageFeatures"] = feature
		pv.Spec.CSI.VolumeAttributes["imageFormat"] = basePV.Spec.CSI.VolumeAttributes["imageFormat"]
		pv.Spec.CSI.VolumeAttributes["pool"] = basePV.Spec.CSI.VolumeAttributes["pool"]
		pv.Spec.CSI.VolumeAttributes["staticVolume"] = "true"

		return nil
	})
	return err
}

// createOrUpdateStaticPVC creates or updates a PersistentVolumeClaim (PVC) for binding to a static PersistentVolume (PV).
// This function configures the PVC to reference the specified static PV, sets labels, access modes, storage class,
// and resource requirements for static volume provisioning.
//
// Parameters:
//
//	ctx           - context for the API calls
//	pvcName       - name for the new or updated PVC
//	pvName        - name of the static PV to bind to
//	componentName - label value for the component
//	resources     - resource requirements for the PVC
//
// Returns an error if creation or update fails.
func (r *MantleBackupReconciler) createOrUpdateStaticPVC(
	ctx context.Context,
	pvcName, pvName, componentName string,
	resources corev1.VolumeResourceRequirements,
) error {
	var pvc corev1.PersistentVolumeClaim
	pvc.SetName(pvcName)
	pvc.SetNamespace(r.managedCephClusterID)
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, &pvc, func() error {
		labels := pvc.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels["app.kubernetes.io/name"] = labelAppNameValue
		labels["app.kubernetes.io/component"] = componentName
		pvc.SetLabels(labels)

		storageClassName := ""
		pvc.Spec.StorageClassName = &storageClassName
		pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
		pvc.Spec.Resources = resources
		pvc.Spec.VolumeName = pvName

		volumeMode := corev1.PersistentVolumeBlock
		pvc.Spec.VolumeMode = &volumeMode

		return nil
	})
	return err
}

func (r *MantleBackupReconciler) createOrUpdateZeroOutJob(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
) error {
	var job batchv1.Job
	job.SetName(MakeZeroOutJobName(backup))
	job.SetNamespace(r.managedCephClusterID)
	_, err := ctrl.CreateOrUpdate(ctx, r.Client, &job, func() error {
		labels := job.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels["app.kubernetes.io/name"] = labelAppNameValue
		labels["app.kubernetes.io/component"] = labelComponentZeroOutJob
		job.SetLabels(labels)

		job.Spec.BackoffLimit = ptr.To(int32(65535))

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
	})
	return err
}

//nolint:unused // TODO: Delete this line after starting use `verify`.
func (r *MantleBackupReconciler) createOrUpdateVerifyJob(ctx context.Context, jobName, pvcName string) error {
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
		job.Spec.PodFailurePolicy = &batchv1.PodFailurePolicy{
			Rules: []batchv1.PodFailurePolicyRule{
				{
					Action: batchv1.PodFailurePolicyActionFailJob,
					OnExitCodes: &batchv1.PodFailurePolicyOnExitCodesRequirement{
						ContainerName: ptr.To("verify"),
						Operator:      batchv1.PodFailurePolicyOnExitCodesOpIn,
						Values:        []int32{1, 2, 3, 4, 5, 6, 7}, // File system errors
					},
				},
			},
		}

		job.Spec.Template.Spec.Containers = []corev1.Container{
			{
				Name:  "verify",
				Image: r.podImage,
				Command: []string{
					"/usr/sbin/e2fsck",
					"-fn",
					"/dev/verify-rbd",
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
	return err
}

func (r *MantleBackupReconciler) hasZeroOutJobCompleted(ctx context.Context, backup *mantlev1.MantleBackup) (bool, error) {
	var job batchv1.Job
	if err := r.Get(
		ctx,
		types.NamespacedName{Name: MakeZeroOutJobName(backup), Namespace: r.managedCephClusterID},
		&job,
	); err != nil {
		if aerrors.IsNotFound(err) {
			return false, nil // The cache must be stale. Let's just requeue.
		}
		return false, err
	}

	if IsJobConditionTrue(job.Status.Conditions, batchv1.JobComplete) {
		return true, nil
	}
	return false, nil
}

func (r *MantleBackupReconciler) reconcileImportJob(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	snapshotTarget *snapshotTarget,
	largestCompletedPartNum int,
) (ctrl.Result, error) {
	partNum := largestCompletedPartNum + 1

	// Check that all import Jobs are completed
	finalPartNum, err := r.getNumberOfParts(backup)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to calcuate num of export data parts: %w", err)
	}
	if partNum == finalPartNum {
		// Make sure the (final) RBD snapshot is created.
		snapshot, err := ceph.FindRBDSnapshot(
			r.ceph,
			snapshotTarget.poolName,
			snapshotTarget.imageName,
			backup.GetName(),
		)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to find imported RBD snapshot: %w", err)
		}

		// Update the status of the MantleBackup to set True to the ReadyToUse condition.
		if err := updateStatus(ctx, r.Client, backup, func() error {
			backup.Status.SnapID = &snapshot.Id
			meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
				Type:   mantlev1.BackupConditionReadyToUse,
				Status: metav1.ConditionTrue,
				Reason: mantlev1.ConditionReasonReadyToUseNoProblem,
			})
			return nil
		}); err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{}, nil
	}

	// Check that the export data is already uploaded.
	uploaded, err := r.isExportDataAlreadyUploaded(ctx, backup, partNum)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to check if part of export data is not already uploaded: %d: %w", partNum, err)
	}
	if !uploaded {
		return requeueReconciliation(), nil
	}

	// create or update an import Job
	if err := r.createOrUpdateImportJob(ctx, backup, snapshotTarget, partNum); err != nil {
		return ctrl.Result{}, err
	}

	return requeueReconciliation(), nil
}

func (r *MantleBackupReconciler) createOrUpdateImportJob(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	snapshotTarget *snapshotTarget,
	partNum int,
) error {
	if backup.Status.TransferPartSize == nil {
		return errors.New("status.transferPartSize is nil")
	}
	transferPartSize, ok := backup.Status.TransferPartSize.AsInt64()
	if !ok {
		return errors.New("status.transferPartSize can't be converted to int64")
	}

	script := os.Getenv(EnvImportJobScript)
	if script == "" {
		script = EmbedJobImportScript
	}

	var job batchv1.Job

	job.SetName(MakeImportJobName(backup, partNum))
	job.SetNamespace(r.managedCephClusterID)

	_, err := ctrl.CreateOrUpdate(ctx, r.Client, &job, func() error {
		labels := job.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels["app.kubernetes.io/name"] = labelAppNameValue
		labels["app.kubernetes.io/component"] = labelComponentImportJob
		job.SetLabels(labels)

		job.Spec.BackoffLimit = ptr.To(int32(65535))

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
	})

	return err
}

func (r *MantleBackupReconciler) primaryCleanup(
	ctx context.Context,
	target *mantlev1.MantleBackup,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	diffFrom, ok := target.GetAnnotations()[annotDiffFrom]
	if ok {
		var source mantlev1.MantleBackup
		if err := r.Get(
			ctx,
			types.NamespacedName{Name: diffFrom, Namespace: target.GetNamespace()},
			&source,
		); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to get source MantleBackup: %w", err)
		}
		delete(source.GetAnnotations(), annotDiffTo)
		if err := r.Update(ctx, &source); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to update source MantleBackup: %w", err)
		}
	}

	if err := r.deleteAllExportJobs(ctx, target); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to delete export Jobs: %w", err)
	}

	if err := r.deleteAllUploadJobs(ctx, target); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to delete upload Jobs: %w", err)
	}

	if err := r.deleteAllExportDataPVCs(ctx, target); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to delete export data PVCs: %w", err)
	}

	delete(target.GetAnnotations(), annotDiffFrom)
	delete(target.GetAnnotations(), annotSyncMode)
	if err := r.Update(ctx, target); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to delete annotations of diff-from and sync-mode: %w", err)
	}

	if !target.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	if err := r.updateMantleBackupCondition(
		ctx, target,
		mantlev1.BackupConditionSyncedToRemote,
		metav1.ConditionTrue,
		mantlev1.ConditionReasonSyncedToRemoteNoProblem,
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to set SyncedToRemote condition to True: %w", err)
	}
	logger.Info("succeeded to sync a backup to the remote ceph cluster")

	duration := time.Since(target.GetCreationTimestamp().Time).Seconds()
	metrics.BackupDurationSeconds.With(prometheus.Labels{
		"persistentvolumeclaim": target.Spec.PVC,
		"resource_namespace":    target.GetNamespace(),
	}).Observe(duration)

	return ctrl.Result{}, nil
}

func (r *MantleBackupReconciler) getNumberOfPartsForResourceDeletion(backup *mantlev1.MantleBackup) (int, error) {
	if backup.Status.SnapSize == nil || backup.Status.TransferPartSize == nil {
		return 0, nil
	}
	return r.getNumberOfParts(backup)
}

func (r *MantleBackupReconciler) deleteAllJobsOfComponent(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	makeJobName func(*mantlev1.MantleBackup, int) string,
) error {
	numParts, err := r.getNumberOfPartsForResourceDeletion(backup)
	if err != nil {
		return fmt.Errorf("failed to get the number of the parts of the exported data: %w", err)
	}

	for partNum := 0; partNum < numParts; partNum++ {
		var job batchv1.Job
		job.SetName(makeJobName(backup, partNum))
		job.SetNamespace(r.managedCephClusterID)
		if err := r.Delete(ctx, &job, &client.DeleteOptions{
			PropagationPolicy: ptr.To(metav1.DeletePropagationBackground),
		}); err != nil && !aerrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete Job: %s/%s: %w", job.GetNamespace(), job.GetName(), err)
		}
	}

	return nil
}

func (r *MantleBackupReconciler) deleteAllExportJobs(ctx context.Context, backup *mantlev1.MantleBackup) error {
	return r.deleteAllJobsOfComponent(ctx, backup, MakeExportJobName)
}

func (r *MantleBackupReconciler) deleteAllUploadJobs(ctx context.Context, backup *mantlev1.MantleBackup) error {
	return r.deleteAllJobsOfComponent(ctx, backup, MakeUploadJobName)
}

func (r *MantleBackupReconciler) deleteAllExportDataPVCs(ctx context.Context, backup *mantlev1.MantleBackup) error {
	numParts, err := r.getNumberOfPartsForResourceDeletion(backup)
	if err != nil {
		return fmt.Errorf("failed to get the number of the parts of the exported data: %w", err)
	}

	for partNum := 0; partNum < numParts; partNum++ {
		pvc := corev1.PersistentVolumeClaim{}
		pvc.SetName(MakeExportDataPVCName(backup, partNum))
		pvc.SetNamespace(r.managedCephClusterID)
		if err := r.Delete(ctx, &pvc, &client.DeleteOptions{
			PropagationPolicy: ptr.To(metav1.DeletePropagationBackground),
		}); err != nil && !aerrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete PVC: %s/%s: %w", pvc.GetNamespace(), pvc.GetName(), err)
		}
	}

	return nil
}

func (r *MantleBackupReconciler) secondaryCleanup(
	ctx context.Context,
	target *mantlev1.MantleBackup,
	deleteExportData bool,
) (ctrl.Result, error) {
	diffFrom, ok := target.GetAnnotations()[annotDiffFrom]
	if ok {
		var source mantlev1.MantleBackup
		if err := r.Get(
			ctx,
			types.NamespacedName{Name: diffFrom, Namespace: target.GetNamespace()},
			&source,
		); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to get source MantleBackup: %w", err)
		}
		delete(source.GetAnnotations(), annotDiffTo)
		if err := r.Update(ctx, &source); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to update source MantleBackup: %w", err)
		}
	}

	if err := r.deleteAllImportJobs(ctx, target); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to delete import Jobs: %w", err)
	}

	var zeroOutJob batchv1.Job
	zeroOutJob.SetName(MakeZeroOutJobName(target))
	zeroOutJob.SetNamespace(r.managedCephClusterID)
	if err := r.Delete(ctx, &zeroOutJob, &client.DeleteOptions{
		PropagationPolicy: ptr.To(metav1.DeletePropagationBackground),
	}); err != nil && !aerrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("failed to delete zeroout Job: %w", err)
	}

	var zeroOutPVC corev1.PersistentVolumeClaim
	zeroOutPVC.SetName(MakeZeroOutPVCName(target))
	zeroOutPVC.SetNamespace(r.managedCephClusterID)
	if err := r.Delete(ctx, &zeroOutPVC); err != nil && !aerrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("failed to delete zeroout PVC: %w", err)
	}

	var zeroOutPV corev1.PersistentVolume
	zeroOutPV.SetName(MakeZeroOutPVName(target))
	if err := r.Delete(ctx, &zeroOutPV); err != nil && !aerrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("failed to delete zeroout PV: %w", err)
	}

	if deleteExportData {
		if err := r.deleteAllExportedData(ctx, target); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to delete exported data in the object storage: %w", err)
		}
	}

	delete(target.GetAnnotations(), annotDiffFrom)
	delete(target.GetAnnotations(), annotSyncMode)
	if err := r.Update(ctx, target); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update target MantleBackup: %w", err)
	}

	if err := r.deleteMiddleSnapshots(target); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to delete middle snapshots: %w", err)
	}

	return ctrl.Result{}, nil
}

func (r *MantleBackupReconciler) deleteAllImportJobs(ctx context.Context, backup *mantlev1.MantleBackup) error {
	return r.deleteAllJobsOfComponent(ctx, backup, MakeImportJobName)
}

func (r *MantleBackupReconciler) deleteAllExportedData(ctx context.Context, backup *mantlev1.MantleBackup) error {
	numParts, err := r.getNumberOfPartsForResourceDeletion(backup)
	if err != nil {
		return fmt.Errorf("failed to get the number of the parts of the exported data: %w", err)
	}

	for partNum := 0; partNum < numParts; partNum++ {
		key := MakeObjectNameOfExportedData(backup.GetName(), backup.GetAnnotations()[annotRemoteUID], partNum)
		if err := r.objectStorageClient.Delete(ctx, key); err != nil {
			return fmt.Errorf("failed to delete exported data in the object storage: %s: %w", key, err)
		}
	}

	return nil
}

func (r *MantleBackupReconciler) deleteMiddleSnapshots(backup *mantlev1.MantleBackup) error {
	// Check that middle snapshots can exist
	_, ok := backup.GetAnnotations()[annotRemoteUID]
	if !ok {
		return nil
	}
	if backup.Status.PVManifest == "" || backup.Status.TransferPartSize == nil || backup.Status.SnapSize == nil {
		return nil
	}

	numParts, err := r.getNumberOfPartsForResourceDeletion(backup)
	if err != nil {
		return fmt.Errorf("failed to get number of parts: %s/%s: %w", backup.GetNamespace(), backup.GetName(), err)
	}

	transferPartSize, ok := backup.Status.TransferPartSize.AsInt64()
	if !ok {
		return fmt.Errorf("failed to get transferPartSize as int64: %s/%s", backup.GetNamespace(), backup.GetName())
	}

	pool, image, err := r.getPoolAndImageFromStatusPVManifest(backup)
	if err != nil {
		return fmt.Errorf("failed to get pool and image from status.PVManifest: %w", err)
	}

	snaps, err := r.ceph.RBDSnapLs(pool, image)
	if err != nil {
		return fmt.Errorf("failed to list snapshots: %s: %s: %w", pool, image, err)
	}

	for i := 0; i < numParts; i++ {
		snapIndex := slices.IndexFunc(snaps, func(snap ceph.RBDSnapshot) bool {
			return snap.Name == MakeMiddleSnapshotName(backup, i*int(transferPartSize))
		})
		if snapIndex == -1 {
			continue
		}
		if err := r.ceph.RBDSnapRm(pool, image, snaps[snapIndex].Name); err != nil {
			return fmt.Errorf("failed to remove snapshot: %s: %s: %w", pool, image, err)
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
) error {
	// Update the status of the MantleBackup. Use Patch here because Update() is
	// likely to fail due to "the object has been modified" error.
	willBeApplied := target.DeepCopy()
	meta.SetStatusCondition(&willBeApplied.Status.Conditions, metav1.Condition{
		Type:   conditionType,
		Status: status,
		Reason: reason,
	})
	if err := r.Client.Status().Patch(ctx, willBeApplied, client.MergeFrom(target)); err != nil {
		return fmt.Errorf("failed to update MantleBackup condition by patch: %w", err)
	}
	return nil
}
