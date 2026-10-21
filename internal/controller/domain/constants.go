package domain

// MantleBackup related constants.
const (
	// AnnotRemoteUID is the annotation key that the secondary mantle-controller attaches to
	// the PVCs and MantleBackups it creates to hold the UIDs of the corresponding resources
	// on the primary cluster.
	AnnotRemoteUID = "mantle.cybozu.io/remote-uid"
)
