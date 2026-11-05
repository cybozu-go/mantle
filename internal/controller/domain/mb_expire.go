package domain

import (
	"context"
	"time"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
	"k8s.io/kube-openapi/pkg/validation/strfmt"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// DeleteMantleBackupOperation represents an operation to delete a MantleBackup.
type DeleteMantleBackupOperation struct {
	Backup *mantlev1.MantleBackup
}

func (*DeleteMantleBackupOperation) isOperation() {}

// ScheduleExpireOperation represents an operation to reconcile a MantleBackup again
// when it expires.
type ScheduleExpireOperation struct {
	Backup *mantlev1.MantleBackup
}

func (*ScheduleExpireOperation) isOperation() {}

// expire deletes the backup if expired, or schedules its reconciliation at the
// expiration if not yet. Unlike the other processes in Provision, it returns nil to
// continue the reconciliation after scheduling, because it doesn't change the backup.
func expire(
	ctx context.Context,
	backup *mantlev1.MantleBackup,
	operations *ReconcilerOperations,
) *reconcile.Result {
	if backup.Status.CreatedAt.IsZero() {
		// the RBD snapshot has not be taken yet, do nothing.
		return nil
	}

	if !backup.DeletionTimestamp.IsZero() {
		// DeletionTimestamp is already set; let the caller handle finalization.
		return nil
	}

	if v, ok := backup.Annotations[annotRetainIfExpired]; ok && v == annotRetainIfExpiredValue {
		// retain this backup.
		// If the annotation is deleted, reconciliation will run, so no need to schedule.
		return nil
	}

	expire, err := strfmt.ParseDuration(backup.Spec.Expire)
	if err != nil {
		return reconcile.Failed("failed to parse expire duration: %w", err)
	}

	expireAt := backup.Status.CreatedAt.Add(expire)
	if time.Now().UTC().After(expireAt) {
		// already expired, delete it immediately.
		log.FromContext(ctx).Info("delete expired backup", "createdAt", backup.Status.CreatedAt, "expire", expire)
		operations.Append(&DeleteMantleBackupOperation{Backup: backup})

		return reconcile.Succeeded()
	}

	// not expired yet. schedule deletion.
	// The operation may be issued many times, but it is safe because workqueue AddAfter
	// deduplicates events for same object.
	operations.Append(&ScheduleExpireOperation{Backup: backup})

	return nil
}
