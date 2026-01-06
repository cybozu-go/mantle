package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/ceph"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// MantleRestoreReconciler reconciles a MantleRestore object
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
	if errors.IsNotFound(err) {
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

	// check if the backup is ReadyToUse
	if !backup.IsReady() {
		logger.Info("backup is not ready to use", "backup", backup.Name, "namespace", backup.Namespace)

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
	if err := r.createOrUpdateRestoringPV(ctx, restore, &backup); err != nil {
		logger.Error(err, "failed to create PV")

		return ctrl.Result{}, err
	}

	// create a restore PVC with the restore PV
	if err := r.createOrUpdateRestoringPVC(ctx, restore, &backup); err != nil {
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

func (r *MantleRestoreReconciler) createOrUpdateRestoringPV(ctx context.Context, restore *mantlev1.MantleRestore, backup *mantlev1.MantleBackup) error {
	pvName := r.restoringPVName(restore)
	restoredBy := string(restore.UID)

	var pv corev1.PersistentVolume
	pv.SetName(pvName)
	_, err := ctrl.CreateOrUpdate(ctx, r.client, &pv, func() error {
		if !pv.GetDeletionTimestamp().IsZero() {
			return fmt.Errorf("the restoring PV already began to be deleted: %s", pvName)
		}

		if pv.Annotations == nil {
			pv.Annotations = map[string]string{}
		}
		if annot, ok := pv.Annotations[PVAnnotationRestoredBy]; ok && annot != restoredBy {
			return fmt.Errorf("the existing PV is having different MantleRestore UID: %s, %s",
				pvName, pv.Annotations[PVAnnotationRestoredBy])
		}
		pv.Annotations[PVAnnotationRestoredBy] = restoredBy
		pv.Annotations[PVAnnotationRestoredByName] = restore.GetName()
		pv.Annotations[PVAnnotationRestoredByNamespace] = restore.GetNamespace()

		// get the source PV from the backup
		srcPV := corev1.PersistentVolume{}
		if err := json.Unmarshal([]byte(backup.Status.PVManifest), &srcPV); err != nil {
			return fmt.Errorf("failed to unmarshal PV manifest: %w", err)
		}

		controllerutil.AddFinalizer(&pv, RestoringPVFinalizerName)

		if pv.Labels == nil {
			pv.Labels = map[string]string{}
		}
		pv.Labels[labelRestoringPVKey] = labelRestoringPVValue

		pv.Spec = *srcPV.Spec.DeepCopy()
		capacity, err := resource.ParseQuantity(strconv.FormatInt(*backup.Status.SnapSize, 10))
		if err != nil {
			return fmt.Errorf("failed to parse quantity: %w", err)
		}
		pv.Spec.Capacity[corev1.ResourceStorage] = capacity
		pv.Spec.ClaimRef = nil
		pv.Spec.CSI.VolumeAttributes = map[string]string{
			"clusterID":     srcPV.Spec.CSI.VolumeAttributes["clusterID"],
			"pool":          srcPV.Spec.CSI.VolumeAttributes["pool"],
			"staticVolume":  "true",
			"imageFeatures": srcPV.Spec.CSI.VolumeAttributes["imageFeatures"] + ",deep-flatten",
		}
		pv.Spec.CSI.VolumeHandle = r.restoringRBDImageName(restore)
		pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain

		return nil
	})

	return err
}

func (r *MantleRestoreReconciler) createOrUpdateRestoringPVC(ctx context.Context, restore *mantlev1.MantleRestore, backup *mantlev1.MantleBackup) error {
	pvcName := restore.Name
	pvcNamespace := restore.Namespace
	restoredBy := string(restore.UID)

	var pvc corev1.PersistentVolumeClaim
	pvc.SetName(pvcName)
	pvc.SetNamespace(pvcNamespace)
	_, err := ctrl.CreateOrUpdate(ctx, r.client, &pvc, func() error {
		if pvc.Annotations == nil {
			pvc.Annotations = map[string]string{}
		}
		annotatedRestoredBy, ok := pvc.Annotations[PVCAnnotationRestoredBy]
		if !pvc.CreationTimestamp.IsZero() && !ok {
			return fmt.Errorf("the existing PVC was not created by any mantle-controller: %s/%s",
				pvcNamespace, pvcName)
		}
		if ok && annotatedRestoredBy != restoredBy {
			return fmt.Errorf("the existing PVC was created by different mantle-controller: %s, %s",
				pvcName, pvc.Annotations[PVCAnnotationRestoredBy])
		}
		pvc.Annotations[PVCAnnotationRestoredBy] = restoredBy

		// get the source PVC from the backup
		srcPVC := corev1.PersistentVolumeClaim{}
		if err := json.Unmarshal([]byte(backup.Status.PVCManifest), &srcPVC); err != nil {
			return fmt.Errorf("failed to unmarshal PVC manifest: %w", err)
		}

		pvc.Spec = *srcPVC.Spec.DeepCopy()
		capacity, err := resource.ParseQuantity(strconv.FormatInt(*backup.Status.SnapSize, 10))
		if err != nil {
			return fmt.Errorf("failed to parse quantity: %w", err)
		}
		pvc.Spec.Resources = corev1.VolumeResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceStorage: capacity,
			},
		}
		pvc.Spec.VolumeName = r.restoringPVName(restore)

		if err := controllerutil.SetControllerReference(restore, &pvc, r.Scheme); err != nil {
			return fmt.Errorf("failed to set controller reference: %w", err)
		}

		return nil
	})

	return err
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
		if errors.IsNotFound(err) {
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
		if errors.IsNotFound(err) {
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
