// Package reconcile provides a small state machine for tracking the outcome of a
// reconciliation attempt, shared by the reconcilers under internal/controller.
package reconcile

import (
	"fmt"
	"os"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
)

// Result represents the outcome of a reconciliation attempt, holding one
// of five results: finished, requeueing, slow requeueing, error, or legacy
// delegation. At most one of these is set at a time. A nil Result, or a Result
// with none of these set, indicates that the reconciliation should continue to
// the next step.
type Result struct {
	finished       bool
	requeueing     bool
	slowRequeueing bool
	legacy         bool
	err            error
}

func Succeeded() *Result {
	return &Result{finished: true}
}

func Requeue() *Result {
	return &Result{requeueing: true}
}

// SlowRequeue behaves like Requeue, but waits longer before the next reconciliation
// attempt. Use it when there's no need to requeue soon — for example, while waiting
// for a higher priority backup to finish.
func SlowRequeue() *Result {
	return &Result{slowRequeueing: true}
}

func Failed(format string, args ...any) *Result {
	return &Result{err: fmt.Errorf(format, args...)}
}

// ContinueWithLegacyReconcile returns a Result that tells the caller to continue
// reconciliation through the legacy MantleBackup controller path. This is temporary
// and will be removed once the migration to the new reconcilers is complete.
func ContinueWithLegacyReconcile() *Result {
	return &Result{legacy: true}
}

// ShouldContinueWithLegacyReconcile reports whether the caller must run the legacy
// reconciliation path. Callers have to check this before ToCtrlResult.
func (r *Result) ShouldContinueWithLegacyReconcile() bool {
	if r == nil {
		return false
	}

	return r.legacy
}

func (r *Result) ShouldReturn() bool {
	if r == nil {
		return false
	}

	return r.finished || r.requeueing || r.slowRequeueing || r.err != nil
}

// WrapIfError wraps the held error with the given message only when this result holds
// an error (i.e. it was created by Failed). Otherwise it returns the result
// unchanged. This is intended to add context to an error propagated from a callee.
func (r *Result) WrapIfError(format string, args ...any) *Result {
	if r == nil || r.err == nil {
		return r
	}

	return Failed(format+": %w", append(args, r.err)...)
}

func (r *Result) ToCtrlResult() (ctrl.Result, error) {
	if r == nil {
		return ctrl.Result{}, nil
	}

	if r.err != nil {
		return ctrl.Result{}, r.err
	}

	if r.requeueing {
		return RequeueAfter(), nil
	}

	if r.slowRequeueing {
		return SlowRequeueAfter(), nil
	}

	// finished (or none of the above set, which also means continue/finish with no error)
	return ctrl.Result{}, nil
}

// RequeueAfter returns a ctrl.Result that requeues reconciliation after the duration
// configured in the REQUEUE_RECONCILIATION_AFTER environment variable.
func RequeueAfter() ctrl.Result {
	requeueAfter := os.Getenv("REQUEUE_RECONCILIATION_AFTER")
	if len(requeueAfter) == 0 {
		panic("You should set REQUEUE_RECONCILIATION_AFTER env var.")
	}
	duration, err := time.ParseDuration(requeueAfter)
	if err != nil {
		panic(fmt.Sprintf("Set REQUEUE_RECONCILIATION_AFTER properly: %v", err))
	}

	return ctrl.Result{RequeueAfter: duration}
}

var slowRequeueAfter *time.Duration

// SlowRequeueAfter is like RequeueAfter but uses SLOW_REQUEUE_RECONCILIATION_AFTER,
// expected to be set longer than REQUEUE_RECONCILIATION_AFTER. Unlike RequeueAfter,
// an unset value falls back to RequeueAfter's duration rather than panicking.
func SlowRequeueAfter() ctrl.Result {
	if slowRequeueAfter == nil {
		return RequeueAfter()
	}

	return ctrl.Result{RequeueAfter: *slowRequeueAfter}
}

// InitSlowRequeueAfter parses SLOW_REQUEUE_RECONCILIATION_AFTER once, if set, and
// caches the result for SlowRequeueAfter to use, so a misconfigured value fails
// fast as an error at startup instead of panicking the first time SlowRequeueAfter
// is called. REQUEUE_RECONCILIATION_AFTER is unaffected: RequeueAfter keeps
// reading and panicking on it on every call, as before.
func InitSlowRequeueAfter() error {
	slowRequeueAfter = nil

	slowRequeueAfterEnv := os.Getenv("SLOW_REQUEUE_RECONCILIATION_AFTER")
	if len(slowRequeueAfterEnv) == 0 {
		return nil
	}

	duration, err := time.ParseDuration(slowRequeueAfterEnv)
	if err != nil {
		return fmt.Errorf("set SLOW_REQUEUE_RECONCILIATION_AFTER properly: %w", err)
	}
	slowRequeueAfter = &duration

	return nil
}
