package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/ceph"
	corev1 "k8s.io/api/core/v1"
	aerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// MantleRestoreReconciler reconciles a MantleRestore object.
type MantleRestoreReconciler struct {
	client               client.Client
	Scheme               *runtime.Scheme
	managedCephClusterID string
	ceph                 ceph.CephCmd
	role                 string
}

const (
	mbAnnotationSkipVerifyKey       = "mantle.cybozu.io/skip-verify"
	mbAnnotationSkipVerifyValue     = "true"
	MantleRestoreFinalizerName      = "mantlerestore.mantle.cybozu.io/finalizer"
	RestoringPVFinalizerName        = "mantle.cybozu.io/restoring-pv-finalizer"
	PVAnnotationRestoredBy          = "mantle.cybozu.io/restored-by"
	PVAnnotationRestoredByName      = "mantle.cybozu.io/restored-by-name"
	PVAnnotationRestoredByNamespace = "mantle.cybozu.io/restored-by-namespace"
	PVCAnnotationRestoredBy         = "mantle.cybozu.io/restored-by"
	labelRestoringPVKey             = "mantle.cybozu.io/restoring-pv"
	labelRestoringPVValue           = "true"
)

// +kubebuilder:rbac:groups=mantle.cybozu.io,resources=mantlerbackup,verbs=get;list;watch
// +kubebuilder:rbac:groups=mantle.cybozu.io,resources=mantlerestores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mantle.cybozu.io,resources=mantlerestores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mantle.cybozu.io,resources=mantlerestores/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumes/finalizers,verbs=update

func NewMantleRestoreReconciler(
	client client.Client,
	scheme *runtime.Scheme,
	managedCephClusterID,
	role string,
) *MantleRestoreReconciler {
	return &MantleRestoreReconciler{
		client:               client,
		Scheme:               scheme,
		managedCephClusterID: managedCephClusterID,
		ceph:                 ceph.NewCephCmd(),
		role:                 role,
	}
}

func (r *MantleRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var restore mantlev1.MantleRestore
	err := r.client.Get(ctx, req.NamespacedName, &restore)
	if aerrors.IsNotFound(err) {
		logger.Info("MantleRestore resource not found", "error", err)

		return ctrl.Result{}, nil
	}
	if err != nil {
		logger.Error(err, "failed to get MantleRestore")

		return ctrl.Result{}, err
	}

	if restore.DeletionTimestamp.IsZero() {
		return r.restore(ctx, &restore)
	} else {
		return r.cleanup(ctx, &restore)
	}
}

func (r *MantleRestoreReconciler) restore(ctx context.Context, restore *mantlev1.MantleRestore) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("restoring", "backup", restore.Spec.Backup)

	// skip if already ReadyToUse
	if restore.IsReady() {
		return ctrl.Result{}, nil
	}

	// get the MantleBackup resource bound to this MantleRestore
	var backup mantlev1.MantleBackup
	err := r.client.Get(ctx, client.ObjectKey{Name: restore.Spec.Backup, Namespace: restore.Namespace}, &backup)
	if err != nil {
		logger.Error(err, "failed to get MantleBackup", "name", restore.Spec.Backup, "namespace", restore.Namespace)

		return ctrl.Result{}, err
	}

	var pvc corev1.PersistentVolumeClaim
	err = json.Unmarshal([]byte(backup.Status.PVCManifest), &pvc)
	if err != nil {
		logger.Error(err, "failed to unmarshal PVC manifest", "backup", backup.Name, "namespace", backup.Namespace)

		return ctrl.Result{}, err
	}

	// check if the PVC is managed by the target Ceph cluster
	clusterID, err := getCephClusterIDFromPVC(ctx, r.client, &pvc)
	if err != nil {
		logger.Error(err, "failed to get Ceph cluster ID", "backup", backup.Name, "namespace", backup.Namespace)

		return ctrl.Result{}, err
	}
	if clusterID != r.managedCephClusterID {
		logger.Info("backup is not managed by the target Ceph cluster", "backup", backup.Name, "namespace", backup.Namespace, "clusterID", clusterID)

		return ctrl.Result{}, nil
	}

	// store the cluster ID in the status
	restore.Status.ClusterID = clusterID
	err = r.client.Status().Update(ctx, restore)
	if err != nil {
		logger.Error(err, "failed to update status.clusterID", "status", restore.Status)

		return ctrl.Result{}, err
	}

	// set the finalizer to this MantleRestore
	controllerutil.AddFinalizer(restore, MantleRestoreFinalizerName)
	err = r.client.Update(ctx, restore)
	if err != nil {
		logger.Error(err, "failed to add finalizer")

		return ctrl.Result{}, err
	}

	// check if the backup is SnapshotCaptured
	if !backup.IsSnapshotCaptured() {
		logger.Info("snapshot is not captured", "backup", backup.Name, "namespace", backup.Namespace)

		return requeueReconciliation(), nil
	}

	// check if the backup is verified or verification is skipped
	if skip, ok := backup.GetAnnotations()[mbAnnotationSkipVerifyKey]; !backup.IsVerifiedTrue() && (!ok || skip != mbAnnotationSkipVerifyValue) {
		logger.Info("verification is not completed", "backup", backup.Name, "namespace", backup.Namespace)

		return requeueReconciliation(), nil
	}

	// create a clone image from the backup
	if err := r.cloneImageFromBackup(ctx, restore, &backup); err != nil {
		logger.Error(err, "failed to clone image from backup", "backup", backup.Name, "namespace", backup.Namespace)

		return ctrl.Result{}, err
	}

	// create a restore PV with the clone image
	if err := r.createRestoringPVIfNotExists(ctx, restore, &backup); err != nil {
		logger.Error(err, "failed to create PV")

		return ctrl.Result{}, err
	}

	// create a restore PVC with the restore PV
	if err := r.createRestoringPVCIfNotExists(ctx, restore, &backup); err != nil {
		logger.Error(err, "failed to create PVC")

		return ctrl.Result{}, err
	}

	// update the status of this MantleRestore to ReadyToUse
	meta.SetStatusCondition(&restore.Status.Conditions, metav1.Condition{
		Type:   mantlev1.RestoreConditionReadyToUse,
		Status: metav1.ConditionTrue,
		Reason: mantlev1.RestoreReasonNone,
	})
	err = r.client.Status().Update(ctx, restore)
	if err != nil {
		logger.Error(err, "failed to update status", "status", restore.Status)

		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *MantleRestoreReconciler) restoringRBDImageName(restore *mantlev1.MantleRestore) string {
	return fmt.Sprintf("mantle-%s-%s", restore.Namespace, restore.Name)
}

func (r *MantleRestoreReconciler) restoringPVName(restore *mantlev1.MantleRestore) string {
	return fmt.Sprintf("mr-%s-%s", restore.Namespace, restore.Name)
}

func (r *MantleRestoreReconciler) cloneImageFromBackup(ctx context.Context, restore *mantlev1.MantleRestore, backup *mantlev1.MantleBackup) error {
	pv := corev1.PersistentVolume{}
	err := json.Unmarshal([]byte(backup.Status.PVManifest), &pv)
	if err != nil {
		return fmt.Errorf("failed to unmarshal PV manifest: %w", err)
	}

	return createCloneByPV(ctx, r.ceph, &pv, backup.Name, r.restoringRBDImageName(restore))
}

// createRestoringPVIfNotExists creates a PersistentVolume (PV) for restoring from the backup if it does not exist.
// If the PV already exists, it checks its validity.
func (r *MantleRestoreReconciler) createRestoringPVIfNotExists(ctx context.Context, restore *mantlev1.MantleRestore, backup *mantlev1.MantleBackup) error {
	pvName := r.restoringPVName(restore)
	pvcName := restore.Name
	pvcNamespace := restore.Namespace
	restoredBy := string(restore.UID)

	var currentPV corev1.PersistentVolume
	err := r.client.Get(ctx, client.ObjectKey{Name: pvName}, &currentPV)
	if err == nil {
		// If the PV already exists, we check its validity.
		annotations := currentPV.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}
		existingRestoredBy, ok := annotations[PVAnnotationRestoredBy]
		if !ok {
			return fmt.Errorf("the existing PV was not created by mantle-controller: %s", pvName)
		}
		if existingRestoredBy != restoredBy {
			return fmt.Errorf("the existing PV has a different MantleRestore UID: %s, %s",
				pvName, existingRestoredBy)
		}
		// PV already exists and is valid.
		return nil
	} else if !aerrors.IsNotFound(err) {
		return fmt.Errorf("failed to get PV %s: %w", pvName, err)
	}

	// Get the source PV from the backup.
	var srcPV corev1.PersistentVolume
	if err := json.Unmarshal([]byte(backup.Status.PVManifest), &srcPV); err != nil {
		return fmt.Errorf("failed to unmarshal PV manifest: %w", err)
	}
	if srcPV.Spec.CSI == nil {
		return errors.New("PV is not a CSI volume")
	}
	// Use the snapshot size as the capacity of the restoring PV.
	capacity, err := resource.ParseQuantity(strconv.FormatInt(*backup.Status.SnapSize, 10))
	if err != nil {
		return fmt.Errorf("failed to parse quantity: %w", err)
	}

	// Create a new PV.
	newPV := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName,
			Annotations: map[string]string{
				PVAnnotationRestoredBy:          restoredBy,
				PVAnnotationRestoredByName:      restore.GetName(),
				PVAnnotationRestoredByNamespace: restore.GetNamespace(),
			},
			Labels: map[string]string{
				labelRestoringPVKey: labelRestoringPVValue,
			},
			Finalizers: []string{
				RestoringPVFinalizerName,
			},
		},
	}
	newPV.Spec = *srcPV.Spec.DeepCopy()
	newPV.Spec.Capacity[corev1.ResourceStorage] = capacity
	// Set ClaimRef to bind only to the restoring PVC.
	newPV.Spec.ClaimRef = &corev1.ObjectReference{
		Namespace: pvcNamespace,
		Name:      pvcName,
	}
	newPV.Spec.CSI.VolumeAttributes = map[string]string{
		"clusterID":     srcPV.Spec.CSI.VolumeAttributes["clusterID"],
		"pool":          srcPV.Spec.CSI.VolumeAttributes["pool"],
		"staticVolume":  "true",
		"imageFeatures": srcPV.Spec.CSI.VolumeAttributes["imageFeatures"] + ",deep-flatten",
	}
	newPV.Spec.CSI.VolumeHandle = r.restoringRBDImageName(restore)
	newPV.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
	// No StorageClass to indicate static provisioning.
	newPV.Spec.StorageClassName = ""

	if err := r.client.Create(ctx, newPV); err != nil {
		return fmt.Errorf("failed to create PV %s: %w", pvName, err)
	}

	return nil
}

// createRestoringPVCIfNotExists creates a PersistentVolumeClaim (PVC) for restoring from the backup if it does not exist.
// If the PVC already exists, it checks its validity.
func (r *MantleRestoreReconciler) createRestoringPVCIfNotExists(ctx context.Context, restore *mantlev1.MantleRestore, backup *mantlev1.MantleBackup) error {
	pvName := r.restoringPVName(restore)
	pvcName := restore.Name
	pvcNamespace := restore.Namespace
	restoredBy := string(restore.UID)

	var currentPVC corev1.PersistentVolumeClaim
	err := r.client.Get(ctx, client.ObjectKey{Namespace: pvcNamespace, Name: pvcName}, &currentPVC)
	if err == nil {
		// If the PVC already exists, we check its validity.
		annotations := currentPVC.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}
		existingRestoredBy, ok := annotations[PVCAnnotationRestoredBy]
		if !ok {
			return fmt.Errorf("the existing PVC was not created by any mantle-controller: %s/%s",
				pvcNamespace, pvcName)
		}
		if existingRestoredBy != restoredBy {
			return fmt.Errorf("the existing PVC has a different MantleRestore UID: %s, %s",
				pvcName, existingRestoredBy)
		}
		// PVC already exists and is valid.
		return nil
	} else if !aerrors.IsNotFound(err) {
		return fmt.Errorf("failed to get PVC %s/%s: %w", pvcNamespace, pvcName, err)
	}

	// Get the source PVC from the backup.
	srcPVC := corev1.PersistentVolumeClaim{}
	if err := json.Unmarshal([]byte(backup.Status.PVCManifest), &srcPVC); err != nil {
		return fmt.Errorf("failed to unmarshal PVC manifest: %w", err)
	}
	// Use the snapshot size as the capacity of the restoring PVC.
	capacity, err := resource.ParseQuantity(strconv.FormatInt(*backup.Status.SnapSize, 10))
	if err != nil {
		return fmt.Errorf("failed to parse quantity: %w", err)
	}

	// Create a new PVC.
	newPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: pvcNamespace,
			Name:      pvcName,
			Annotations: map[string]string{
				PVCAnnotationRestoredBy: restoredBy,
			},
		},
	}
	newPVC.Spec = *srcPVC.Spec.DeepCopy()
	newPVC.Spec.Resources = corev1.VolumeResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceStorage: capacity,
		},
	}
	// No StorageClass to indicate static provisioning.
	// and ensure this PVC statically binds to the specific PV.
	newPVC.Spec.StorageClassName = ptr.To("")
	newPVC.Spec.VolumeName = pvName

	if err := controllerutil.SetControllerReference(restore, newPVC, r.Scheme); err != nil {
		return fmt.Errorf("failed to set controller reference: %w", err)
	}

	if err := r.client.Create(ctx, newPVC); err != nil {
		return fmt.Errorf("failed to create PVC %s/%s: %w", pvcNamespace, pvcName, err)
	}

	return nil
}

func (r *MantleRestoreReconciler) cleanup(ctx context.Context, restore *mantlev1.MantleRestore) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	// check if the cluster ID matches
	if restore.Status.ClusterID != r.managedCephClusterID {
		return ctrl.Result{}, nil
	}

	logger.Info("deleting")

	// delete the PVC
	if err := r.deleteRestoringPVC(ctx, restore); err != nil {
		logger.Error(err, "failed to delete PVC")

		return ctrl.Result{}, err
	}

	// delete the PV
	err := r.deleteRestoringPV(ctx, restore)
	if err != nil {
		logger.Error(err, "failed to get PV")

		return ctrl.Result{}, err
	}

	// remove the finalizer
	controllerutil.RemoveFinalizer(restore, MantleRestoreFinalizerName)
	err = r.client.Update(ctx, restore)
	if err != nil {
		logger.Error(err, "failed to remove finalizer")

		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// deleteRestoringPVC deletes the restoring PVC.
func (r *MantleRestoreReconciler) deleteRestoringPVC(ctx context.Context, restore *mantlev1.MantleRestore) error {
	pvcName := restore.Name
	pvcNamespace := restore.Namespace
	pvc := corev1.PersistentVolumeClaim{}
	if err := r.client.Get(ctx, client.ObjectKey{Name: pvcName, Namespace: pvcNamespace}, &pvc); err != nil {
		if aerrors.IsNotFound(err) {
			return nil
		}

		return fmt.Errorf("failed to get PVC: %w", err)
	}

	if pvc.Annotations[PVCAnnotationRestoredBy] != string(restore.UID) {
		return fmt.Errorf("PVC is having different MantleRestore UID: %s, %s", pvcName, pvc.Annotations[PVCAnnotationRestoredBy])
	}

	if err := r.client.Delete(ctx, &pvc); err != nil {
		return fmt.Errorf("failed to delete PVC: %w", err)
	}

	return nil
}

// deleteRestoringPV deletes the restoring PV.
func (r *MantleRestoreReconciler) deleteRestoringPV(ctx context.Context, restore *mantlev1.MantleRestore) error {
	pv := corev1.PersistentVolume{}
	if err := r.client.Get(ctx, client.ObjectKey{Name: r.restoringPVName(restore)}, &pv); err != nil {
		if aerrors.IsNotFound(err) {
			// NOTE: Since the cache of the client may be stale, we may look
			// over some PVs that should be removed here.  Such PVs will be
			// removed by GarbageCollectorRunner.
			return nil
		}

		return fmt.Errorf("failed to get PV: %w", err)
	}

	if pv.Annotations[PVAnnotationRestoredBy] != string(restore.UID) {
		return fmt.Errorf("PV is having different MantleRestore UID: %s, %s", pv.Name, pv.Annotations[PVAnnotationRestoredBy])
	}

	if err := r.client.Delete(ctx, &pv); err != nil {
		return fmt.Errorf("failed to delete PV: %w", err)
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *MantleRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mantlev1.MantleRestore{}).
		Complete(r)
}
