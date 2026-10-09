package domain

import (
	"context"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// MantleBackup related constants.
const (
	// AnnotRemoteUID is the annotation key that the secondary mantle-controller attaches to
	// the PVCs and MantleBackups it creates to hold the UIDs of the corresponding resources
	// on the primary cluster.
	AnnotRemoteUID = "mantle.cybozu.io/remote-uid"
)

// isCreatedWhenMantleControllerWasSecondary returns true iff the MantleBackup
// is created by the secondary mantle.
func isCreatedWhenMantleControllerWasSecondary(backup *mantlev1.MantleBackup) bool {
	_, ok := backup.Annotations[AnnotRemoteUID]

	return ok
}

// skipBackupCreatedByRemote stops the reconciliation of a MantleBackup on a standalone or
// primary cluster that was created by a remote mantle-controller.
func skipBackupCreatedByRemote(ctx context.Context) *reconcile.Result {
	log.FromContext(ctx).Info(
		"skipping to reconcile the MantleBackup created by a remote mantle-controller to prevent accidental data loss",
	)

	return reconcile.Succeeded()
}

// skipBackupNotCreatedAsSecondary stops the reconciliation of a MantleBackup on a secondary
// cluster that was not created by this mantle-controller as a secondary.
func skipBackupNotCreatedAsSecondary(ctx context.Context) *reconcile.Result {
	log.FromContext(ctx).Info(
		"skipping to reconcile the MantleBackup created by a different mantle-controller to prevent accidental data loss",
	)

	return reconcile.Succeeded()
}
