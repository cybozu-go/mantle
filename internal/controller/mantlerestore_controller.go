package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/ceph"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// MantleRestoreReconciler reconciles a MantleRestore object
type MantleRestoreReconciler struct {
	client.Client
	Scheme               *runtime.Scheme
	managedCephClusterID string
	ceph                 ceph.CephCmd
}

const (
	MantleRestoreFinalizerName = "mantlerestore.mantle.cybozu.io/finalizer"
	PVAnnotationRestoredBy     = "mantle.cybozu.io/restored-by"
	PVCAnnotationRestoredBy    = "mantle.cybozu.io/restored-by"
)

// +kubebuilder:rbac:groups=mantle.cybozu.io,resources=mantlerbackup,verbs=get;list;watch
// +kubebuilder:rbac:groups=mantle.cybozu.io,resources=mantlerestores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=mantle.cybozu.io,resources=mantlerestores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=mantle.cybozu.io,resources=mantlerestores/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch;create;update;patch;delete

func NewMantleRestoreReconciler(cli client.Client, scheme *runtime.Scheme, managedCephClusterID string) *MantleRestoreReconciler {
	return &MantleRestoreReconciler{
		Client:               cli,
		Scheme:               scheme,
		managedCephClusterID: managedCephClusterID,
		ceph:                 ceph.NewCephCmd(),
	}
}

func (r *MantleRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logger.With("MantleRestore", req.NamespacedName)

	var restore mantlev1.MantleRestore
	err := r.Get(ctx, req.NamespacedName, &restore)
	if errors.IsNotFound(err) {
		logger.Info("MantleRestore resource not found", "name", req.Name, "error", err)
		return ctrl.
			Result{}, nil
	}
	if err != nil {
		logger.Error("failed to get MantleRestore", "name", req.NamespacedName, "error", err)
		return ctrl.Result{}, err
	}

	if restore.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.restore(ctx, logger, &restore)
	} else {
		return r.cleanup(ctx, logger, &restore)
	}
}

func (r *MantleRestoreReconciler) restore(ctx context.Context, logger *slog.Logger, restore *mantlev1.MantleRestore) (ctrl.Result, error) {
	logger.Info("restoring", "name", restore.Name, "namespace", restore.Namespace, "backup", restore.Spec.Backup)
	// get the MantleBackup resource bound to this MantleRestore
	var backup mantlev1.MantleBackup
	err := r.Get(ctx, client.ObjectKey{Name: restore.Spec.Backup, Namespace: restore.Namespace}, &backup)
	if err != nil {
		logger.Error("failed to get MantleBackup", "name", restore.Spec.Backup, "namespace", restore.Namespace, "error", err)
		return ctrl.Result{}, err
	}

	var pvc corev1.PersistentVolumeClaim
	err = json.Unmarshal([]byte(backup.Status.PVCManifest), &pvc)
	if err != nil {
		logger.Error("failed to unmarshal PVC manifest", "backup", backup.Name, "namespace", backup.Namespace, "error", err)
		return ctrl.Result{}, err
	}

	// check if the PVC is managed by the target Ceph cluster
	clusterID, err := getCephClusterIDFromPVC(ctx, logger, r.Client, &pvc)
	if err != nil {
		logger.Error("failed to get Ceph cluster ID", "backup", backup.Name, "namespace", backup.Namespace, "error", err)
		return ctrl.Result{}, err
	}
	if clusterID != r.managedCephClusterID {
		logger.Info("backup is not managed by the target Ceph cluster", "backup", backup.Name, "namespace", backup.Namespace, "clusterID", clusterID)
		return ctrl.Result{}, nil
	}

	// store the cluster ID in the status
	restore.Status.ClusterID = clusterID
	err = r.Status().Update(ctx, restore)
	if err != nil {
		logger.Error("failed to update status.clusterID", "status", restore.Status, "error", err)
		return ctrl.Result{}, err
	}

	// set the finalizer to this MantleRestore
	controllerutil.AddFinalizer(restore, MantleRestoreFinalizerName)
	err = r.Update(ctx, restore)
	if err != nil {
		logger.Error("failed to add finalizer", "restore", restore.Name, "namespace", restore.Namespace, "error", err)
		return ctrl.Result{}, err
	}

	// check if the backup is ReadyToUse
	condition := meta.FindStatusCondition(backup.Status.Conditions, mantlev1.BackupConditionReadyToUse)
	if condition == nil || condition.Status != metav1.ConditionTrue {
		logger.Info("backup is not ready to use", "backup", backup.Name, "namespace", backup.Namespace)
		return ctrl.Result{Requeue: true}, nil
	}

	// store the pool name in the status
	var pv corev1.PersistentVolume
	err = json.Unmarshal([]byte(backup.Status.PVManifest), &pv)
	if err != nil {
		logger.Error("failed to unmarshal PV manifest", "backup", backup.Name, "namespace", backup.Namespace, "error", err)
		return ctrl.Result{}, err
	}
	restore.Status.Pool = pv.Spec.CSI.VolumeAttributes["pool"]
	if restore.Status.Pool == "" {
		logger.Error("pool not found in PV manifest", "backup", backup.Name, "namespace", backup.Namespace)
		return ctrl.Result{}, fmt.Errorf("pool not found in PV manifest")
	}
	err = r.Status().Update(ctx, restore)
	if err != nil {
		logger.Error("failed to update status.pool", "status", restore.Status, "error", err)
		return ctrl.Result{}, err
	}

	// create a clone image from the backup
	if err := r.cloneImageFromBackup(logger, restore, &backup); err != nil {
		logger.Error("failed to clone image from backup", "backup", backup.Name, "namespace", backup.Namespace, "error", err)
		return ctrl.Result{}, err
	}

	// create a restore PV with the clone image
	if err := r.createRestoringPV(ctx, restore, &backup); err != nil {
		logger.Error("failed to create PV", "restore", restore.Name, "namespace", restore.Namespace, "error", err)
		return ctrl.Result{}, err
	}

	// create a restore PVC with the restore PV
	if err := r.createRestoringPVC(ctx, restore, &backup); err != nil {
		logger.Error("failed to create PVC", "restore", restore.Name, "namespace", restore.Namespace, "error", err)
		return ctrl.Result{}, err
	}

	// update the status of this MantleRestore to ReadyToUse
	meta.SetStatusCondition(&restore.Status.Conditions, metav1.Condition{
		Type:   mantlev1.RestoreConditionReadyToUse,
		Status: metav1.ConditionTrue,
	})
	err = r.Status().Update(ctx, restore)
	if err != nil {
		logger.Error("failed to update status", "status", restore.Status, "error", err)
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

func (r *MantleRestoreReconciler) cloneImageFromBackup(logger *slog.Logger, restore *mantlev1.MantleRestore, backup *mantlev1.MantleBackup) error {
	pv := corev1.PersistentVolume{}
	err := json.Unmarshal([]byte(backup.Status.PVManifest), &pv)
	if err != nil {
		return fmt.Errorf("failed to unmarshal PV manifest: %v", err)
	}

	pool := pv.Spec.CSI.VolumeAttributes["pool"]
	if pool == "" {
		return fmt.Errorf("pool not found in PV manifest")
	}

	bkImage := pv.Spec.CSI.VolumeAttributes["imageName"]
	if bkImage == "" {
		return fmt.Errorf("imageName not found in PV manifest")
	}

	images, err := r.ceph.RBDLs(pool)
	if err != nil {
		return fmt.Errorf("failed to list RBD images: %v", err)
	}

	// check if the image already exists
	if slices.Contains(images, r.restoringRBDImageName(restore)) {
		info, err := r.ceph.RBDInfo(pool, r.restoringRBDImageName(restore))
		if err != nil {
			return fmt.Errorf("failed to get RBD info: %v", err)
		}

		if info.ParentPool == pool && info.ParentImage == bkImage && info.ParentSnap == backup.Name {
			logger.Info("image already exists", "image", r.restoringRBDImageName(restore))
			return nil
		} else {
			return fmt.Errorf("image already exists but not a clone of the backup: %s", r.restoringRBDImageName(restore))
		}
	}

	features := pv.Spec.CSI.VolumeAttributes["imageFeatures"]
	if features == "" {
		features = "deep-flatten"
	} else {
		features += ",deep-flatten"
	}

	// create a clone image from the backup
	return r.ceph.RBDClone(pool, bkImage, backup.Name, r.restoringRBDImageName(restore), features)
}

func (r *MantleRestoreReconciler) createRestoringPV(ctx context.Context, restore *mantlev1.MantleRestore, backup *mantlev1.MantleBackup) error {
	pvName := r.restoringPVName(restore)
	restoredBy := string(restore.UID)

	// check if the PV already exists
	existingPV := corev1.PersistentVolume{}
	if err := r.Get(ctx, client.ObjectKey{Name: pvName}, &existingPV); err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to get existing PV: %v", err)
		}

	} else if existingPV.Annotations[PVAnnotationRestoredBy] != restoredBy {
		return fmt.Errorf("existing PV is having different MantleRestore UID: %s, %s", pvName, existingPV.Annotations[PVAnnotationRestoredBy])
	} else {
		// PV already exists and restored by the same MantleRestore
		return nil
	}

	// get the source PV from the backup
	srcPV := corev1.PersistentVolume{}
	err := json.Unmarshal([]byte(backup.Status.PVManifest), &srcPV)
	if err != nil {
		return fmt.Errorf("failed to unmarshal PV manifest: %v", err)
	}

	// create a new restoring PV corresponding to a restoring RBD image
	newPV := corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName,
			Annotations: map[string]string{
				PVAnnotationRestoredBy: restoredBy,
			},
		},
		Spec: *srcPV.Spec.DeepCopy(),
	}
	newPV.Spec.ClaimRef = nil
	newPV.Spec.CSI.VolumeAttributes = map[string]string{
		"clusterID":     srcPV.Spec.CSI.VolumeAttributes["clusterID"],
		"pool":          srcPV.Spec.CSI.VolumeAttributes["pool"],
		"staticVolume":  "true",
		"imageFeatures": srcPV.Spec.CSI.VolumeAttributes["imageFeatures"] + ",deep-flatten",
	}
	newPV.Spec.CSI.VolumeHandle = r.restoringRBDImageName(restore)
	newPV.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain

	return r.Create(ctx, &newPV)
}

func (r *MantleRestoreReconciler) createRestoringPVC(ctx context.Context, restore *mantlev1.MantleRestore, backup *mantlev1.MantleBackup) error {
	pvcName := restore.Name
	pvcNamespace := restore.Namespace
	restoredBy := string(restore.UID)

	// check if the PVC already exists
	existingPVC := corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, client.ObjectKey{Name: pvcName, Namespace: pvcNamespace}, &existingPVC); err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to get existing PVC: %v", err)
		}

	} else if existingPVC.Annotations[PVCAnnotationRestoredBy] != restoredBy {
		return fmt.Errorf("existing PVC is having different MantleRestore UID: %s, %s", pvcName, existingPVC.Annotations[PVCAnnotationRestoredBy])
	} else {
		// PVC already exists and restored by the same MantleRestore
		return nil
	}

	// get the source PVC from the backup
	srcPVC := corev1.PersistentVolumeClaim{}
	err := json.Unmarshal([]byte(backup.Status.PVCManifest), &srcPVC)
	if err != nil {
		return fmt.Errorf("failed to unmarshal PVC manifest: %v", err)
	}

	newPVC := corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: pvcNamespace,
			Annotations: map[string]string{
				PVCAnnotationRestoredBy: restoredBy,
			},
		},
		Spec: *srcPVC.Spec.DeepCopy(),
	}
	newPVC.Spec.VolumeName = r.restoringPVName(restore)

	return r.Create(ctx, &newPVC)
}

func (r *MantleRestoreReconciler) cleanup(ctx context.Context, logger *slog.Logger, restore *mantlev1.MantleRestore) (ctrl.Result, error) {
	// check if the cluster ID matches
	if restore.Status.ClusterID != r.managedCephClusterID {
		return ctrl.Result{}, nil
	}

	logger.Info("deleting", "name", restore.Name, "namespace", restore.Namespace)

	// delete the PVC
	if err := r.deleteRestoringPVC(ctx, restore); err != nil {
		logger.Error("failed to delete PVC", "restore", restore.Name, "namespace", restore.Namespace, "error", err)
		return ctrl.Result{}, err
	}

	// delete the PV
	err := r.deleteRestoringPV(ctx, restore)
	if err != nil {
		logger.Error("failed to get PV", "restore", restore.Name, "namespace", restore.Namespace, "error", err)
		return ctrl.Result{}, err
	}

	// delete the clone image
	if err := r.removeRBDImage(restore); err != nil {
		logger.Error("failed to remove image", "restore", restore.Name, "namespace", restore.Namespace, "error", err)
		return ctrl.Result{}, err
	}

	// remove the finalizer
	controllerutil.RemoveFinalizer(restore, MantleRestoreFinalizerName)
	err = r.Update(ctx, restore)
	if err != nil {
		logger.Error("failed to remove finalizer", "restore", restore.Name, "namespace", restore.Namespace, "error", err)
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *MantleRestoreReconciler) deleteRestoringPVC(ctx context.Context, restore *mantlev1.MantleRestore) error {
	pvcName := restore.Name
	pvcNamespace := restore.Namespace
	pvc := corev1.PersistentVolumeClaim{}
	if err := r.Get(ctx, client.ObjectKey{Name: pvcName, Namespace: pvcNamespace}, &pvc); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get PVC: %v", err)
	}

	if pvc.Annotations[PVCAnnotationRestoredBy] != string(restore.UID) {
		return fmt.Errorf("PVC is having different MantleRestore UID: %s, %s", pvcName, pvc.Annotations[PVCAnnotationRestoredBy])
	}

	if err := r.Delete(ctx, &pvc); err != nil {
		return fmt.Errorf("failed to delete PVC: %v", err)
	}

	if err := r.Get(ctx, client.ObjectKey{Name: pvcName, Namespace: pvcNamespace}, &pvc); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get PVC: %v", err)
	} else {
		return fmt.Errorf("PVC still exists: %s", pvcName)
	}
}

func (r *MantleRestoreReconciler) deleteRestoringPV(ctx context.Context, restore *mantlev1.MantleRestore) error {
	pv := corev1.PersistentVolume{}
	if err := r.Get(ctx, client.ObjectKey{Name: r.restoringPVName(restore)}, &pv); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get PV: %v", err)
	}

	if pv.Annotations[PVAnnotationRestoredBy] != string(restore.UID) {
		return fmt.Errorf("PV is having different MantleRestore UID: %s, %s", pv.Name, pv.Annotations[PVAnnotationRestoredBy])
	}

	if err := r.Delete(ctx, &pv); err != nil {
		return fmt.Errorf("failed to delete PV: %v", err)
	}

	if err := r.Get(ctx, client.ObjectKey{Name: r.restoringPVName(restore)}, &pv); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get PV: %v", err)
	} else {
		return fmt.Errorf("PV still exists: %s", pv.Name)
	}
}

func (r *MantleRestoreReconciler) removeRBDImage(restore *mantlev1.MantleRestore) error {
	image := r.restoringRBDImageName(restore)
	pool := restore.Status.Pool
	logger.Info("removing image", "restore", restore.Name, "namespace", restore.Namespace, "pool", pool, "image", image)
	images, err := r.ceph.RBDLs(pool)
	if err != nil {
		return fmt.Errorf("failed to list RBD images: %v", err)
	}

	if !slices.Contains(images, image) {
		return nil
	}

	return r.ceph.RBDRm(pool, image)
}

// SetupWithManager sets up the controller with the Manager.
func (r *MantleRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&mantlev1.MantleRestore{}).
		Complete(r)
}
