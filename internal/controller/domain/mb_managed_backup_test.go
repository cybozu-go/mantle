package domain_test

import (
	"context"
	"errors"
	"slices"
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
	pvcClusterID *string
	getterErr    error
	// expected is the result of the reconciliation. A nil expected means that the
	// MantleBackup is managed and is left to the legacy reconciliation.
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

// newManagedPVCClusterIDGetter returns a PVCClusterIDGetter that reports the PVC of
// newMB belongs to the managed Ceph cluster, so that a MantleBackup built by newMB
// passes the check of whether it is managed.
func newManagedPVCClusterIDGetter() *fakePVCClusterIDGetter {
	return &fakePVCClusterIDGetter{
		pvcClusterIDs: map[string]string{newMB().Spec.PVC: managedCephClusterID},
		err:           nil,
		lookups:       nil,
	}
}

// testManagedBackupCheck checks that both Provision and Finalize of the reconciler
// stop the reconciliation of a MantleBackup that is not managed, and leave a managed
// one to the legacy reconciliation. baseOpts are applied to every MantleBackup so
// that it passes the ownership guard of the reconciler.
func testManagedBackupCheck(
	t *testing.T,
	newReconciler func(pvcClusterIDGetter domain.PVCClusterIDGetter) mbReconciler,
	baseOpts []optionMB,
) {
	t.Helper()

	testCases := append(labeledCheckManagedBackupTestCases(), unlabeledCheckManagedBackupTestCases()...)
	for _, method := range []string{"Provision", "Finalize"} {
		for _, testCase := range testCases {
			t.Run(method+"/"+testCase.name, func(t *testing.T) {
				t.Parallel()

				// Arrange
				opts := append(slices.Clone(baseOpts), testCase.opts...)
				if method == "Finalize" {
					opts = append(opts, mbBeingDeleted())
				}

				backup := newMB(opts...)
				origBackup := backup.DeepCopy()
				getter := &fakePVCClusterIDGetter{pvcClusterIDs: map[string]string{}, err: testCase.getterErr, lookups: nil}

				if testCase.pvcClusterID != nil {
					getter.pvcClusterIDs[backup.Spec.PVC] = *testCase.pvcClusterID
				}

				reconciler := newReconciler(getter)

				// Act
				var result *reconcile.Result
				if method == "Provision" {
					result = reconciler.Provision(t.Context(), backup)
				} else {
					result = reconciler.Finalize(t.Context(), backup)
				}

				// Assert
				switch {
				case testCase.expectedFailed:
					_, err := result.ToCtrlResult()
					require.ErrorIs(t, err, testCase.getterErr)
				case testCase.expected == nil:
					require.Equal(t, reconcile.ContinueWithLegacyReconcile(), result)
				default:
					require.Equal(t, testCase.expected, result)
				}

				require.Equal(t, testCase.expectedLookups, getter.lookups)
				require.Equal(t, origBackup, backup)
			})
		}
	}
}
