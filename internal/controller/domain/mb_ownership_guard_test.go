package domain_test

import (
	"context"
	"testing"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
	"github.com/stretchr/testify/require"
)

type mbReconciler interface {
	Provision(ctx context.Context, backup *mantlev1.MantleBackup) *reconcile.Result
	Finalize(ctx context.Context, backup *mantlev1.MantleBackup) *reconcile.Result
}

// testOwnershipGuard checks that both Provision and Finalize of the reconciler skip a
// MantleBackup built with skippedOpts, and leave one built with continuedOpts to the
// legacy reconciliation, without modifying either of them.
func testOwnershipGuard(
	t *testing.T,
	newReconciler func() mbReconciler,
	skippedOpts []optionMB,
	continuedOpts []optionMB,
) {
	t.Helper()

	testCases := []struct {
		name     string
		opts     []optionMB
		expected *reconcile.Result
	}{
		{name: "Provision skips", opts: skippedOpts, expected: reconcile.Succeeded()},
		{name: "Provision continues", opts: continuedOpts, expected: reconcile.ContinueWithLegacyReconcile()},
		{name: "Finalize skips", opts: append([]optionMB{mbBeingDeleted()}, skippedOpts...),
			expected: reconcile.Succeeded()},
		{name: "Finalize continues", opts: append([]optionMB{mbBeingDeleted()}, continuedOpts...),
			expected: reconcile.ContinueWithLegacyReconcile()},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			reconciler := newReconciler()
			backup := newMB(testCase.opts...)
			origBackup := backup.DeepCopy()

			// Act
			var result *reconcile.Result
			if backup.DeletionTimestamp.IsZero() {
				result = reconciler.Provision(t.Context(), backup)
			} else {
				result = reconciler.Finalize(t.Context(), backup)
			}

			// Assert
			require.Equal(t, testCase.expected, result)
			require.Equal(t, origBackup, backup)
		})
	}
}

func TestMBStandaloneReconciler_SkipBackupCreatedByRemote(t *testing.T) {
	t.Parallel()
	testOwnershipGuard(t,
		func() mbReconciler { return domain.NewMBStandaloneReconciler() },
		[]optionMB{mbWithRemoteUID()},
		nil,
	)
}

func TestMBPrimaryReconciler_SkipBackupCreatedByRemote(t *testing.T) {
	t.Parallel()
	testOwnershipGuard(t,
		func() mbReconciler { return domain.NewMBPrimaryReconciler() },
		[]optionMB{mbWithRemoteUID()},
		nil,
	)
}

func TestMBSecondaryReconciler_SkipBackupNotCreatedAsSecondary(t *testing.T) {
	t.Parallel()
	testOwnershipGuard(t,
		func() mbReconciler { return domain.NewMBSecondaryReconciler() },
		nil,
		[]optionMB{mbWithRemoteUID()},
	)
}
