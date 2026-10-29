package domain_test

import (
	"context"
	"slices"
	"testing"
	"time"

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

type expireExpectation struct {
	result     *reconcile.Result
	failed     bool
	operations func(backup *mantlev1.MantleBackup) []domain.Operation
}

type expireTestCase struct {
	name     string
	opts     []optionMB
	expected expireExpectation
}

func noOperations(*mantlev1.MantleBackup) []domain.Operation {
	return []domain.Operation{}
}

func deleteOperation(backup *mantlev1.MantleBackup) []domain.Operation {
	return []domain.Operation{&domain.DeleteMantleBackupOperation{Backup: backup}}
}

func scheduleExpireOperation(backup *mantlev1.MantleBackup) []domain.Operation {
	return []domain.Operation{&domain.ScheduleExpireOperation{Backup: backup}}
}

func expireTestCases() []expireTestCase {
	const expire = "2w"

	expireDuration := 14 * 24 * time.Hour
	expiredAt := time.Now().UTC().Add(-expireDuration).Add(-time.Hour)
	notExpiredAt := time.Now().UTC().Add(-expireDuration).Add(time.Hour)

	continued := expireExpectation{
		result: reconcile.ContinueWithLegacyReconcile(), failed: false, operations: noOperations,
	}
	deleted := expireExpectation{result: reconcile.Succeeded(), failed: false, operations: deleteOperation}
	scheduled := expireExpectation{
		result: reconcile.ContinueWithLegacyReconcile(), failed: false, operations: scheduleExpireOperation,
	}
	failed := expireExpectation{result: nil, failed: true, operations: noOperations}

	return []expireTestCase{
		{
			name:     "does nothing before the snapshot is captured",
			opts:     []optionMB{mbWithExpire(expire)},
			expected: continued,
		},
		{
			name:     "deletes the expired backup",
			opts:     []optionMB{mbWithExpire(expire), mbWithCreatedAt(expiredAt)},
			expected: deleted,
		},
		{
			name: "retains the expired backup with the retain-if-expired annotation",
			opts: []optionMB{
				mbWithExpire(expire), mbWithCreatedAt(expiredAt),
				mbWithAnnotation("mantle.cybozu.io/retain-if-expired", "true"),
			},
			expected: continued,
		},
		{
			name: "deletes the expired backup with the retain-if-expired annotation not set to true",
			opts: []optionMB{
				mbWithExpire(expire), mbWithCreatedAt(expiredAt),
				mbWithAnnotation("mantle.cybozu.io/retain-if-expired", "false"),
			},
			expected: deleted,
		},
		{
			name:     "schedules the expiration and continues if the backup has not expired yet",
			opts:     []optionMB{mbWithExpire(expire), mbWithCreatedAt(notExpiredAt)},
			expected: scheduled,
		},
		{
			name:     "fails if the expire duration is invalid",
			opts:     []optionMB{mbWithExpire("invalid"), mbWithCreatedAt(expiredAt)},
			expected: failed,
		},
		{
			name:     "does nothing for the expired backup being deleted",
			opts:     []optionMB{mbWithExpire(expire), mbWithCreatedAt(expiredAt), mbBeingDeleted()},
			expected: continued,
		},
	}
}

// testExpire checks the expiration handled by the reconciler. baseOpts are applied
// to every MantleBackup so that it passes the ownership guard of the reconciler.
func testExpire(
	t *testing.T,
	newReconciler func() (mbReconciler, *domain.ReconcilerOperations),
	baseOpts []optionMB,
) {
	t.Helper()

	for _, testCase := range expireTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			reconciler, operations := newReconciler()
			backup := newMB(append(slices.Clone(baseOpts), testCase.opts...)...)
			origBackup := backup.DeepCopy()

			// Act
			var result *reconcile.Result
			if backup.DeletionTimestamp.IsZero() {
				result = reconciler.Provision(t.Context(), backup)
			} else {
				result = reconciler.Finalize(t.Context(), backup)
			}

			// Assert
			if testCase.expected.failed {
				_, err := result.ToCtrlResult()
				require.Error(t, err)
			} else {
				require.Equal(t, testCase.expected.result, result)
			}

			require.Equal(t, testCase.expected.operations(backup), operations.TakeAll())
			require.Equal(t, origBackup, backup)
		})
	}
}
