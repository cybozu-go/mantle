package domain_test

import (
	"context"
	"errors"
	"testing"

	"github.com/cybozu-go/mantle/internal/controller/domain"
	"github.com/cybozu-go/mantle/internal/controller/internal/reconcile"
	"github.com/stretchr/testify/require"
)

const (
	managedCephClusterID = "managed-ceph-cluster-id"
	otherCephClusterID   = "other-ceph-cluster-id"
)

var errGetPVCClusterID = errors.New("get PVC cluster ID error")

// fakePVCClusterIDGetter is a domain.PVCClusterIDGetter that returns the Ceph
// cluster ID of the PVC from pvcClusterIDs. A PVC that is not in pvcClusterIDs
// is treated as not found. It records the PVCs that are looked up.
type fakePVCClusterIDGetter struct {
	pvcClusterIDs map[string]string
	err           error
	lookups       []string
}

var _ domain.PVCClusterIDGetter = (*fakePVCClusterIDGetter)(nil)

func (g *fakePVCClusterIDGetter) GetCephClusterIDOfPVC(
	_ context.Context, namespace, name string,
) (string, bool, error) {
	g.lookups = append(g.lookups, namespace+"/"+name)
	if g.err != nil {
		return "", false, g.err
	}

	clusterID, found := g.pvcClusterIDs[name]

	return clusterID, found, nil
}

type checkManagedBackupTestCase struct {
	name string
	opts []optionMB
	// pvcClusterID is the Ceph cluster ID of the PVC. A nil pvcClusterID means
	// that the PVC does not exist.
	pvcClusterID    *string
	getterErr       error
	expected        *reconcile.Result
	expectedFailed  bool
	expectedLookups []string
}

// labeledCheckManagedBackupTestCases returns the test cases for MantleBackups with
// the cluster-id label. The PVC must not be looked up in these cases.
func labeledCheckManagedBackupTestCases() []checkManagedBackupTestCase {
	return []checkManagedBackupTestCase{
		{
			name:            "continues when the label points to the managed cluster",
			opts:            []optionMB{mbWithClusterIDLabel(managedCephClusterID)},
			pvcClusterID:    nil,
			getterErr:       nil,
			expected:        nil,
			expectedFailed:  false,
			expectedLookups: nil,
		},
		{
			name:            "skips when the label points to another cluster",
			opts:            []optionMB{mbWithClusterIDLabel(otherCephClusterID)},
			pvcClusterID:    nil,
			getterErr:       nil,
			expected:        reconcile.Succeeded(),
			expectedFailed:  false,
			expectedLookups: nil,
		},
		{
			name:            "ignores the PVC when the label exists",
			opts:            []optionMB{mbWithClusterIDLabel(managedCephClusterID)},
			pvcClusterID:    new(otherCephClusterID),
			getterErr:       nil,
			expected:        nil,
			expectedFailed:  false,
			expectedLookups: nil,
		},
	}
}

// unlabeledCheckManagedBackupTestCases returns the test cases for MantleBackups
// without the cluster-id label. The PVC must be looked up in these cases.
func unlabeledCheckManagedBackupTestCases() []checkManagedBackupTestCase {
	pvcLookups := []string{"mb-namespace/pvc-name"}

	return []checkManagedBackupTestCase{
		{
			name:            "continues when the PVC belongs to the managed cluster",
			opts:            nil,
			pvcClusterID:    new(managedCephClusterID),
			getterErr:       nil,
			expected:        nil,
			expectedFailed:  false,
			expectedLookups: pvcLookups,
		},
		{
			name:            "skips when the PVC belongs to another cluster",
			opts:            nil,
			pvcClusterID:    new(otherCephClusterID),
			getterErr:       nil,
			expected:        reconcile.Succeeded(),
			expectedFailed:  false,
			expectedLookups: pvcLookups,
		},
		{
			name:            "skips when the PVC is not managed by any Ceph cluster",
			opts:            nil,
			pvcClusterID:    new(""),
			getterErr:       nil,
			expected:        reconcile.Succeeded(),
			expectedFailed:  false,
			expectedLookups: pvcLookups,
		},
		{
			name:            "requeues when the PVC is not found",
			opts:            nil,
			pvcClusterID:    nil,
			getterErr:       nil,
			expected:        reconcile.Requeue(),
			expectedFailed:  false,
			expectedLookups: pvcLookups,
		},
		{
			name:            "fails when the Ceph cluster ID of the PVC cannot be got",
			opts:            nil,
			pvcClusterID:    nil,
			getterErr:       errGetPVCClusterID,
			expected:        nil,
			expectedFailed:  true,
			expectedLookups: pvcLookups,
		},
	}
}

func TestCheckManagedBackup(t *testing.T) {
	t.Parallel()

	testCases := append(labeledCheckManagedBackupTestCases(), unlabeledCheckManagedBackupTestCases()...)
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			// Arrange
			backup := newMB(testCase.opts...)
			origBackup := backup.DeepCopy()
			getter := &fakePVCClusterIDGetter{pvcClusterIDs: map[string]string{}, err: testCase.getterErr, lookups: nil}

			if testCase.pvcClusterID != nil {
				getter.pvcClusterIDs[backup.Spec.PVC] = *testCase.pvcClusterID
			}

			// Act
			result := domain.CheckManagedBackup(t.Context(), backup, managedCephClusterID, getter)

			// Assert
			if testCase.expectedFailed {
				_, err := result.ToCtrlResult()
				require.ErrorIs(t, err, testCase.getterErr)
			} else {
				require.Equal(t, testCase.expected, result)
			}

			require.Equal(t, testCase.expectedLookups, getter.lookups)
			require.Equal(t, origBackup, backup)
		})
	}
}
