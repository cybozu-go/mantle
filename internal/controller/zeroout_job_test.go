package controller

import (
	"testing"
)

func TestZeroOutScriptInvalidatesCleanSnapshot(t *testing.T) {
	for _, present := range []bool{true, false} {
		name := "absent metadata"
		meta := map[string]string{}
		if present {
			name = "existing metadata"
			meta["mantle.clean-snap"] = snapName1
		}
		t.Run(name, func(t *testing.T) {
			result := runRBDJobScripts(t, meta, nil, EmbedJobZeroOutScript)
			if result.err != nil {
				t.Fatalf("zeroout failed: %v\n%s", result.err, result.stdout)
			}
			discard := result.indexOf("blkdiscard -z /dev/zeroout-rbd")
			if discard < 0 {
				t.Fatalf("the image is not zeroed out: %v", result.rbdArgs)
			}
			if present {
				remove := result.indexOf("image-meta remove pool/image mantle.clean-snap")
				if remove < 0 || discard <= remove {
					t.Fatalf("metadata must be invalidated before zeroout: %v", result.rbdArgs)
				}
			}
			if _, ok := result.metadata(t)["mantle.clean-snap"]; ok {
				t.Fatal("clean snapshot metadata remains after zeroout")
			}
		})
	}
}

func TestZeroOutScriptStopsOnMetadataError(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
	}{
		{"removal", map[string]string{"STUB_META_REMOVE_FAIL": "true"}},
		{"read", map[string]string{"STUB_META_LIST_FAIL_AT": "1"}},
		// An unreadable metadata must not be taken as the absence of the key:
		// zeroing the image while a stale key remains would make a later
		// import Job skip a rollback that is actually necessary.
		{"invalid read", map[string]string{"STUB_META_LIST_INVALID": "true"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runRBDJobScripts(t,
				map[string]string{"mantle.clean-snap": snapName1},
				tt.env,
				EmbedJobZeroOutScript,
			)
			if result.err == nil {
				t.Fatal("zeroout succeeded despite a metadata error")
			}
			if result.invoked("blkdiscard") {
				t.Fatal("zeroout modified the image despite a metadata error")
			}
			if got := result.metadata(t)["mantle.clean-snap"]; got != snapName1 {
				t.Errorf("metadata = %q, want snap1", got)
			}
		})
	}
}

func TestZeroOutScriptCancelledFullImportRequiresRollback(t *testing.T) {
	// The full backup expires after zeroout. The next import is incremental
	// from snap1, whose unchanged data must be restored before import-diff.
	result := runRBDJobScripts(t,
		map[string]string{"mantle.clean-snap": snapName1},
		map[string]string{"FROM_SNAP_NAME": snapName1},
		EmbedJobZeroOutScript,
		EmbedJobImportScript,
	)
	if result.err != nil {
		t.Fatalf("script failed: %v\n%s", result.err, result.stdout)
	}

	discard := result.indexOf("blkdiscard")
	rollback := result.indexOf("snap rollback pool/image@snap1")
	importDiff := result.indexOf("import-diff")
	if discard < 0 || rollback <= discard || importDiff <= rollback {
		t.Fatalf("incremental import must roll back after zeroout: %v", result.rbdArgs)
	}
}
