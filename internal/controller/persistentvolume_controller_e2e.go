package controller

import (
	"context"

	"github.com/cybozu-go/mantle/internal/ceph"
	corev1 "k8s.io/api/core/v1"
)

// PersistentVolumeReconcilerE2E is a wrapper of PersistentVolumeReconciler.
// This module is used to test removeRBDImage in e2e tests.
type PersistentVolumeReconcilerE2E struct {
	PersistentVolumeReconciler
}

func NewPersistentVolumeReconcilerE2E(toolsNamespace string) *PersistentVolumeReconcilerE2E {
	return &PersistentVolumeReconcilerE2E{
		PersistentVolumeReconciler{
			ceph: ceph.NewCephCmdWithTools(toolsNamespace),
		},
	}
}

func (r *PersistentVolumeReconcilerE2E) RemoveRBDImage(ctx context.Context, pv *corev1.PersistentVolume) error {
	return r.removeRBDImage(ctx, pv)
}
