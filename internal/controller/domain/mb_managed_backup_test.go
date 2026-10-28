package domain_test

import (
	"testing"

	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
	"github.com/stretchr/testify/require"
)

const (
	managedCephClusterID = "managed-ceph-cluster-id"
	otherCephClusterID   = "other-ceph-cluster-id"
)

func TestNeedsPVCClusterIDToCheckManagedBackup(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		opts     []optionMB
		expected bool
	}{
		{name: "without the cluster-id label", opts: nil, expected: true},
		{
			name:     "with the cluster-id label of the managed cluster",
			opts:     []optionMB{mbWithClusterIDLabel(managedCephClusterID)},
			expected: false,
		},
		{
			name:     "with the cluster-id label of another cluster",
			opts:     []optionMB{mbWithClusterIDLabel(otherCephClusterID)},
			expected: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			backup := newMB(testCase.opts...)

			// Act
			needed := domain.NeedsPVCClusterIDToCheckManagedBackup(backup)

			// Assert
			require.Equal(t, testCase.expected, needed)
		})
	}
}

type checkManagedBackupTestCase struct {
	name         string
	opts         []optionMB
	pvcClusterID *string
	expected     *reconcile.Result
}

func checkManagedBackupTestCases() []checkManagedBackupTestCase {
	return []checkManagedBackupTestCase{
		{
			name:         "continues when the label points to the managed cluster",
			opts:         []optionMB{mbWithClusterIDLabel(managedCephClusterID)},
			pvcClusterID: nil,
			expected:     nil,
		},
		{
			name:         "skips when the label points to another cluster",
			opts:         []optionMB{mbWithClusterIDLabel(otherCephClusterID)},
			pvcClusterID: nil,
			expected:     reconcile.Succeeded(),
		},
		{
			name:         "ignores the PVC when the label exists",
			opts:         []optionMB{mbWithClusterIDLabel(managedCephClusterID)},
			pvcClusterID: new(otherCephClusterID),
			expected:     nil,
		},
		{
			name:         "continues when the PVC belongs to the managed cluster",
			opts:         nil,
			pvcClusterID: new(managedCephClusterID),
			expected:     nil,
		},
		{
			name:         "skips when the PVC belongs to another cluster",
			opts:         nil,
			pvcClusterID: new(otherCephClusterID),
			expected:     reconcile.Succeeded(),
		},
		{
			name:         "skips when the PVC is not managed by any Ceph cluster",
			opts:         nil,
			pvcClusterID: new(""),
			expected:     reconcile.Succeeded(),
		},
		{
			name:         "requeues when the PVC is not found",
			opts:         nil,
			pvcClusterID: nil,
			expected:     reconcile.Requeue(),
		},
	}
}

func TestCheckManagedBackup(t *testing.T) {
	t.Parallel()

	for _, testCase := range checkManagedBackupTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			backup := newMB(testCase.opts...)
			origBackup := backup.DeepCopy()

			// Act
			result := domain.CheckManagedBackup(t.Context(), backup, managedCephClusterID, testCase.pvcClusterID)

			// Assert
			require.Equal(t, testCase.expected, result)
			require.Equal(t, origBackup, backup)
		})
	}
}
