// Package domain provides domain logic for the mantle controller.
// It contains reconciler implementations that handle the core business logic
// for MantleBackupConfig and related resources.
package domain

// MBCPrimaryReconciler is a reconciler for MantleBackupConfig resources
// running on the primary cluster.
type MBCPrimaryReconciler struct{}

// NewMBCPrimaryReconciler creates a new MBCPrimaryReconciler instance.
func NewMBCPrimaryReconciler() *MBCPrimaryReconciler {
	return &MBCPrimaryReconciler{}
}

// Provision handles the provisioning logic for MantleBackupConfig resources.
// This function should be written in the following way:
//
//	if condition1 {
//	  return process1()
//	}
//	if condition2 {
//	  return process2()
//	}
//	...
//
// processN() should do the actual work and make conditionN false eventually.
// Comments should be left where this rule is not followed to explain why.
func (r *MBCPrimaryReconciler) Provision() error {
	return nil
}

// Finalize handles the finalization logic for MantleBackupConfig resources.
// This function should be written in the following way:
//
//	if condition1 {
//	  return process1()
//	}
//	if condition2 {
//	  return process2()
//	}
//	...
//
// processN() should do the actual work and make conditionN false eventually.
// Comments should be left where this rule is not followed to explain why.
func (r *MBCPrimaryReconciler) Finalize() error {
	return nil
}
