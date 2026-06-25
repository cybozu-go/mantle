package controller

import (
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
)

// reconcileResult represents the outcome of a reconciliation attempt, holding one
// of three results: success, requeueing, or error. At most one of these is set at a
// time. When none of them is set, it indicates that the reconciliation should continue.
type reconcileResult struct {
	success    bool
	requeueing bool
	err        error
}

func succeedReconcile() *reconcileResult {
	return &reconcileResult{success: true}
}

func requeueReconcile() *reconcileResult {
	return &reconcileResult{requeueing: true}
}

func failReconcile(format string, args ...any) *reconcileResult {
	return &reconcileResult{err: fmt.Errorf(format, args...)}
}

func (r *reconcileResult) isFinished() bool {
	if r == nil {
		return false
	}

	return r.success || r.requeueing || r.err != nil
}

// wrapIfError wraps the held error with the given message only when this result holds
// an error (i.e. it was created by failReconcile). Otherwise it returns the result
// unchanged. This is intended to add context to an error propagated from a callee.
func (r *reconcileResult) wrapIfError(format string, args ...any) *reconcileResult {
	if r == nil || r.err == nil {
		return r
	}

	return failReconcile(format+": %w", append(args, r.err)...)
}

func (r *reconcileResult) getResult() (ctrl.Result, error) {
	// success
	if r == nil || r.success {
		return ctrl.Result{}, nil
	}

	// requeueing
	if r.requeueing {
		return requeueReconciliation(), nil
	}

	// error
	return ctrl.Result{}, r.err
}
