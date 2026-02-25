// Package usecase provides use case implementations for the mantle controller.
// It orchestrates domain logic and handles the business workflows for
// MantleBackupConfig and related resources.
package usecase

import (
	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/pkg/errors"
)

// ReconcileMBCPrimary is a use case that handles the reconciliation of
// MantleBackupConfig resources on the primary cluster.
type ReconcileMBCPrimary struct {
	reconciler *domain.MBCPrimaryReconciler
}

// NewReconcileMBCPrimary creates a new ReconcileMBCPrimary instance.
func NewReconcileMBCPrimary() *ReconcileMBCPrimary {
	return &ReconcileMBCPrimary{
		reconciler: domain.NewMBCPrimaryReconciler(),
	}
}

// Run executes the reconciliation logic for a MantleBackupConfig resource.
// It determines whether to provision or finalize based on the deletion timestamp.
func (r *ReconcileMBCPrimary) Run() error {
	var mbc mantlev1.MantleBackupConfig
	if mbc.DeletionTimestamp.IsZero() {
		return r.runProvision()
	}

	return r.runFinalize()
}

func (r *ReconcileMBCPrimary) runProvision() error {
	return errors.Wrap(r.reconciler.Provision(), "provision failed")
}

func (r *ReconcileMBCPrimary) runFinalize() error {
	return errors.Wrap(r.reconciler.Finalize(), "finalize failed")
}
