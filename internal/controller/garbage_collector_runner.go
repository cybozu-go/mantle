package controller

import (
	"context"
	"fmt"
	"time"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	corev1 "k8s.io/api/core/v1"
	aerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type GarbageCollectorRunner struct {
	client               client.Client
	interval             time.Duration
	managedCephClusterID string
}

func NewGarbageCollectorRunner(
	client client.Client,
	interval time.Duration,
	managedCephClusterID string,
) *GarbageCollectorRunner {
	return &GarbageCollectorRunner{
		client:               client,
		interval:             interval,
		managedCephClusterID: managedCephClusterID,
	}
}

func (r *GarbageCollectorRunner) Start(ctx context.Context) error {
	logger := log.FromContext(ctx)

	for {
		ctxSleep, cancelSleep := context.WithTimeout(ctx, r.interval)
		<-ctxSleep.Done()
		cancelSleep()
		if ctx.Err() != nil {
			break
		}

		if err := r.deleteOrphanedPVs(ctx); err != nil {
			logger.Error(err, "failed to delete orphaned PVs", "error", err)
		}
	}

	return nil
}

func (r *GarbageCollectorRunner) deleteOrphanedPVs(ctx context.Context) error {
	logger := log.FromContext(ctx)

	requirement, err := labels.NewRequirement(labelRestoringPVKey, selection.Exists, []string{})
	if err != nil {
		return fmt.Errorf("failed to create a new labels requirement: %w", err)
	}
	selector := labels.ValidatedSetSelector{}.Add(*requirement)

	var pvList corev1.PersistentVolumeList
	if err := r.client.List(ctx, &pvList, &client.ListOptions{
		LabelSelector: selector,
	}); err != nil {
		return fmt.Errorf("failed to list PVs: %w", err)
	}

	for _, pv := range pvList.Items {
		shouldDelete, err := r.isMantleRestoreAlreadyDeleted(ctx, &pv)
		if err != nil {
			return fmt.Errorf("failed to check if a PV should be deleted: %w", err)
		}
		if !shouldDelete {
			continue
		}
		if err := r.client.Delete(ctx, &pv, &client.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &pv.UID, ResourceVersion: &pv.ResourceVersion},
		}); err != nil {
			return fmt.Errorf("failed to delete PV: %w", err)
		}
		logger.Info("an orphaned PV is removed", "name", pv.GetName())
	}

	return nil
}

func (r *GarbageCollectorRunner) isMantleRestoreAlreadyDeleted(ctx context.Context, pv *corev1.PersistentVolume) (bool, error) {
	restoreUID, ok := pv.GetAnnotations()[PVAnnotationRestoredBy]
	if !ok {
		return false, fmt.Errorf("failed to find annotation: %s: %s", PVAnnotationRestoredBy, pv.GetName())
	}
	restoreName, ok := pv.GetAnnotations()[PVAnnotationRestoredByName]
	if !ok {
		return false, fmt.Errorf("failed to find annotation: %s: %s", PVAnnotationRestoredByName, pv.GetName())
	}
	restoreNamespace, ok := pv.GetAnnotations()[PVAnnotationRestoredByNamespace]
	if !ok {
		return false, fmt.Errorf("failed to find annotation: %s: %s", PVAnnotationRestoredByNamespace, pv.GetName())
	}

	clusterID, ok := pv.Spec.CSI.VolumeAttributes["clusterID"]
	if !ok {
		return false, fmt.Errorf("failed to find cluster ID: %s", pv.GetName())
	}
	if r.managedCephClusterID != clusterID {
		return false, nil
	}

	var restore mantlev1.MantleRestore
	if err := r.client.Get(
		ctx,
		types.NamespacedName{Name: restoreName, Namespace: restoreNamespace},
		&restore,
	); err != nil {
		if aerrors.IsNotFound(err) {
			return true, nil
		} else {
			return false, fmt.Errorf("failed to get MantleRestore: %s: %w", pv.GetName(), err)
		}
	}

	if string(restore.GetUID()) != restoreUID {
		return true, nil
	}

	return false, nil
}
