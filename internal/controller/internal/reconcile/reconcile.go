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
// of three results: finished, requeueing, or error. At most one of these is set at a
// time. A nil Result, or a Result with none of these set, indicates that the
// reconciliation should continue to the next step.
type Result struct {
	finished   bool
	requeueing bool
	err        error
}

func Succeeded() *Result {
	return &Result{finished: true}
}

func Requeue() *Result {
	return &Result{requeueing: true}
}

func Failed(format string, args ...any) *Result {
	return &Result{err: fmt.Errorf(format, args...)}
}

func (r *Result) ShouldReturn() bool {
	if r == nil {
		return false
	}

	return r.finished || r.requeueing || r.err != nil
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

	// error
	if r.err != nil {
		return ctrl.Result{}, r.err
	}

	// requeueing
	if r.requeueing {
		return RequeueAfter(), nil
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
