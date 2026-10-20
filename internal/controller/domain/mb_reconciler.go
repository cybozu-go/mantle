package domain

import (
	"context"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// isCreatedWhenMantleControllerWasSecondary returns true iff the MantleBackup
// is created by the secondary mantle.
func isCreatedWhenMantleControllerWasSecondary(backup *mantlev1.MantleBackup) bool {
	_, ok := backup.Annotations[AnnotRemoteUID]

	return ok
}

// skipBackupCreatedAsSecondary stops the reconciliation of a MantleBackup on a standalone or
// primary cluster that was created by a remote mantle-controller.
func skipBackupCreatedAsSecondary(ctx context.Context) *reconcile.Result {
	log.FromContext(ctx).Info(
		"skipping to reconcile the MantleBackup created by a remote mantle-controller to prevent accidental data loss",
	)

	return reconcile.Succeeded()
}
