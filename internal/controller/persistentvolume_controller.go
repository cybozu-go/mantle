package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/cybozu-go/mantle/internal/ceph"
	corev1 "k8s.io/api/core/v1"
	aerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// PersistentVolumeReconciler reconciles a PersistentVolume object
type PersistentVolumeReconciler struct {
	client               client.Client
	Scheme               *runtime.Scheme
	ceph                 ceph.CephCmd
	managedCephClusterID string
}

// +kubebuilder:rbac:groups="",resources=persistentvolumes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumes/finalizers,verbs=update

func NewPersistentVolumeReconciler(
	client client.Client,
	scheme *runtime.Scheme,
	managedCephClusterID string,
) *PersistentVolumeReconciler {
	return &PersistentVolumeReconciler{
		client:               client,
		Scheme:               scheme,
		ceph:                 ceph.NewCephCmd(),
		managedCephClusterID: managedCephClusterID,
	}
}

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the PersistentVolume object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.16.3/pkg/reconcile
func (r *PersistentVolumeReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Get the PV being reconciled.
	var pv corev1.PersistentVolume
	if err := r.client.Get(ctx, req.NamespacedName, &pv); err != nil {
		if aerrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get PersistentVolume: %w", err)
	}

	// Make sure the PV has the finalizer.
	if !controllerutil.ContainsFinalizer(&pv, RestoringPVFinalizerName) {
		return ctrl.Result{}, nil
	}

	// Check if the PV is managed by the target Ceph cluster.
	clusterID, err := getCephClusterIDFromSCName(ctx, r.client, pv.Spec.StorageClassName)
	if err != nil {
		if errors.Is(err, errEmptyClusterID) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if clusterID != r.managedCephClusterID {
		logger.Info("PV is not provisioned by the target Ceph cluster", "pv", pv.Name, "clusterID", clusterID)
		return ctrl.Result{}, nil
	}

	// Make sure the PV has a deletionTimestamp.
	if pv.GetDeletionTimestamp().IsZero() {
		return ctrl.Result{}, nil
	}

	// Wait until the PV's status becomes Released.
	if pv.Status.Phase != corev1.VolumeReleased {
		return ctrl.Result{}, nil
	}

	// Delete the RBD clone image.
	if err := r.removeRBDImage(ctx, &pv); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to remove RBD image: %s: %w", pv.Name, err)
	}

	// Remove the finalizer of the PV.
	controllerutil.RemoveFinalizer(&pv, RestoringPVFinalizerName)
	if err := r.client.Update(ctx, &pv); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to remove finalizer from PersistentVolume: %s: %s: %w", RestoringPVFinalizerName, pv.Name, err)
	}

	logger.Info("finalize PV successfully", "pvName", pv.Name)

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PersistentVolumeReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.PersistentVolume{}).
		WithEventFilter(predicate.Funcs{
			CreateFunc:  func(event.CreateEvent) bool { return true },
			UpdateFunc:  func(event.UpdateEvent) bool { return true },
			GenericFunc: func(event.GenericEvent) bool { return true },
			DeleteFunc: func(ev event.DeleteEvent) bool {
				return !controllerutil.ContainsFinalizer(ev.Object, RestoringPVFinalizerName)
			},
		}).
		Complete(r)
}

func (r *PersistentVolumeReconciler) removeRBDImage(ctx context.Context, pv *corev1.PersistentVolume) error {
	logger := log.FromContext(ctx)

	image := pv.Spec.CSI.VolumeHandle
	pool := pv.Spec.CSI.VolumeAttributes["pool"]
	logger.Info("removing image", "pool", pool, "image", image)

	images, err := r.ceph.RBDLs(pool)
	if err != nil {
		return fmt.Errorf("failed to list RBD images: %w", err)
	}

	if !slices.Contains(images, image) {
		return nil
	}

	imageInfo, err := r.ceph.RBDInfo(pool, image)
	if err != nil {
		return fmt.Errorf("failed to get info about the RBD image: %s/%s: %w", pool, image, err)
	}

	if err := r.ceph.RBDTrashMv(pool, image); err != nil {
		return fmt.Errorf("failed to move the RBD image to trash: %s/%s: %w", pool, image, err)
	}

	if err := r.ceph.CephRBDTaskAddTrashRemove(pool, imageInfo.ID); err != nil {
		return fmt.Errorf("failed to add task to remove the RBD image from trash: %s/%s: %w", pool, image, err)
	}

	return nil
}
