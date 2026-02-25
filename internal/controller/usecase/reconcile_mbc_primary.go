// Package usecase provides use case implementations for the mantle controller.
// It orchestrates domain logic and handles the business workflows for
// MantleBackupConfig and related resources.
package usecase

import (
	"fmt"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
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

// Run executes the reconciliation logic for a MantleBackupConfig resource. It
// basically does the following steps:
//
// 1. Fetch MantleBackupConfig and other related resources.
// 2. Run Provision or Finalize based on the deletion timestamp of MantleBackupConfig.
// 3. Update the status of MantleBackupConfig and related resources accordingly.
func (r *ReconcileMBCPrimary) Run() error {
	var mbc mantlev1.MantleBackupConfig
	if mbc.DeletionTimestamp.IsZero() {
		return r.runProvision()
	}

	return r.runFinalize()
}

func (r *ReconcileMBCPrimary) runProvision() error {
	err := r.reconciler.Provision()
	if err != nil {
		return fmt.Errorf("provision failed: %w", err)
	}

	return nil
}

func (r *ReconcileMBCPrimary) runFinalize() error {
	err := r.reconciler.Finalize()
	if err != nil {
		return fmt.Errorf("finalize failed: %w", err)
	}

	return nil
}
