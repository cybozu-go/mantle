package reconcile

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestToCtrlResult(t *testing.T) {
	t.Setenv("REQUEUE_RECONCILIATION_AFTER", "10s")
	t.Setenv("SLOW_REQUEUE_RECONCILIATION_AFTER", "1m")
	require.NoError(t, InitSlowRequeueAfter())

	t.Run("Requeue uses REQUEUE_RECONCILIATION_AFTER", func(t *testing.T) {
		result, err := Requeue().ToCtrlResult()
		require.NoError(t, err)
		require.Equal(t, 10*time.Second, result.RequeueAfter)
	})

	t.Run("SlowRequeue uses SLOW_REQUEUE_RECONCILIATION_AFTER", func(t *testing.T) {
		result, err := SlowRequeue().ToCtrlResult()
		require.NoError(t, err)
		require.Equal(t, time.Minute, result.RequeueAfter)
	})

	t.Run("SlowRequeue.ShouldReturn is true", func(t *testing.T) {
		require.True(t, SlowRequeue().ShouldReturn())
	})

	t.Run("SlowRequeue falls back to REQUEUE_RECONCILIATION_AFTER when unset", func(t *testing.T) {
		t.Setenv("SLOW_REQUEUE_RECONCILIATION_AFTER", "")
		require.NoError(t, InitSlowRequeueAfter())

		result, err := SlowRequeue().ToCtrlResult()
		require.NoError(t, err)
		require.Equal(t, 10*time.Second, result.RequeueAfter)
	})
}

func TestInitSlowRequeueAfter(t *testing.T) {
	t.Run("returns nil when unset, and SlowRequeueAfter falls back to RequeueAfter", func(t *testing.T) {
		t.Setenv("REQUEUE_RECONCILIATION_AFTER", "10s")
		t.Setenv("SLOW_REQUEUE_RECONCILIATION_AFTER", "")
		require.NoError(t, InitSlowRequeueAfter())
	})

	t.Run("returns an error when set but unparsable", func(t *testing.T) {
		t.Setenv("SLOW_REQUEUE_RECONCILIATION_AFTER", "not-a-duration")
		require.Error(t, InitSlowRequeueAfter())
	})
}
