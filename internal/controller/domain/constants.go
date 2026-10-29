package domain

// MantleBackup related constants.
const (
	// AnnotRemoteUID is the annotation key that the secondary mantle-controller attaches to
	// the PVCs and MantleBackups it creates to hold the UIDs of the corresponding resources
	// on the primary cluster.
	AnnotRemoteUID = "mantle.cybozu.io/remote-uid"

	// LabelClusterID identifies the controller that should reconcile the
	// MantleBackup, guaranteeing there is never a MantleBackup that has a
	// non-nil deletionTimestamp but no controller responsible for it. To
	// uphold this guarantee, the label is attached at the same time as the
	// finalizer, and any cleanup that must run (e.g. deleting the RBD snapshot)
	// is only performed after both this label and the finalizer are attached.
	LabelClusterID = "mantle.cybozu.io/cluster-id"

	annotRetainIfExpired      = "mantle.cybozu.io/retain-if-expired"
	annotRetainIfExpiredValue = "true"
)
