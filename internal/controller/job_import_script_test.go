package controller

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeRBDScript is a stub of the rbd command. It records the arguments of every
// invocation to ${CMD_LOG} and returns ${FAKE_*}.
const fakeRBDScript = `#!/bin/bash
set -eu -o pipefail

echo "rbd $*" >> "${CMD_LOG}"

case "$1" in
info)
    spec="${*: -1}"
    if [ "${FAKE_INFO_OMITS_SIZE}" = "true" ]; then
        echo "{\"name\":\"image\"}"
        exit 0
    fi
    if [[ "${spec}" == *@* ]]; then
        if [ "${FAKE_INFO_FAILS}" = "snap" ]; then
            echo "rbd: error opening image: (2) No such file or directory" >&2
            exit 1
        fi
        echo "{\"name\":\"image\",\"size\":${FAKE_SNAP_SIZE}}"
    else
        if [ "${FAKE_INFO_FAILS}" = "head" ]; then
            echo "rbd: error opening image: (2) No such file or directory" >&2
            exit 1
        fi
        echo "{\"name\":\"image\",\"size\":${FAKE_HEAD_SIZE}}"
    fi
    ;;
diff)
    if [ "${FAKE_DIFF_FAILS}" = "true" ]; then
        echo "rbd: diff error: (2) No such file or directory" >&2
        exit 1
    fi
    if [ "${FAKE_DIFF_PRINTS_NOTHING}" = "true" ]; then
        exit 0
    fi
    printf '%s\n' "${FAKE_DIFF_OUTPUT}"
    ;;
snap)
    if [ "$2" = "ls" ]; then
        printf '%s' "${FAKE_SNAP_LS_OUTPUT}"
    fi
    ;;
import-diff)
    cat > "${IMPORT_STDIN_FILE}"
    ;;
esac
`

// fakeS5cmdScript is a stub recording its arguments to ${CMD_LOG} and returning
// ${EXPORTED_DATA}.
const fakeS5cmdScript = `#!/bin/bash
set -eu -o pipefail
echo "s5cmd $*" >> "${CMD_LOG}"
printf '%s' "${EXPORTED_DATA}"
`

// fakeZstdScript is a stub recording its arguments to ${CMD_LOG}.
const fakeZstdScript = `#!/bin/bash
set -eu -o pipefail
echo "zstd $*" >> "${CMD_LOG}"
cat
`

const (
	// The rbd invocations the script is expected to make.
	rbdInfoHead            = "rbd info --format json pool/image"
	rbdInfoSnap1           = "rbd info --format json pool/image@snap1"
	rbdDiffSnap1           = "rbd diff --format json --from-snap snap1 pool/image"
	rbdRollbackSnap1       = "rbd snap rollback pool/image@snap1"
	rbdImportDiff          = "rbd import-diff -p pool - image"
	rbdSnapLs              = "rbd snap ls pool/image"
	rbdInfoInitialSnap     = "rbd info --format json pool/image@initialsnap"
	rbdDiffInitialSnap     = "rbd diff --format json --from-snap initialsnap pool/image"
	rbdCreateInitialSnap   = "rbd snap create pool/image@initialsnap"
	rbdRollbackInitialSnap = "rbd snap rollback pool/image@initialsnap"
	rbdRmInitialSnap       = "rbd snap rm pool/image@initialsnap"

	// The s5cmd and zstd invocations the script is expected to make.
	s5cmdCat         = "s5cmd --endpoint-url http://localhost:9000 cat s3://bucket/obj"
	s5cmdCatWithCert = "s5cmd --endpoint-url http://localhost:9000 --credentials-file /etc/cert/cert.json cat s3://bucket/obj"
	zstdDecompress   = "zstd -d -q -c"
	certFile         = "/etc/cert/cert.json"

	// The exported data that the stubbed s5cmd emits.
	exportedData = "exported data"

	// initialSnapLsOutput is an "rbd snap ls" output that contains initialsnap,
	// i.e. what the script sees when a previous import Job left it behind.
	initialSnapLsOutput = "SNAPID  NAME          SIZE     PROTECTED  TIMESTAMP\n64      initialsnap   1.7 TiB             Tue Mar 24 08:52:56 2026\n"
)

const (
	// defaultSize is the size both the HEAD and the snapshot have unless a
	// test case says otherwise.
	defaultSize = "10737418240"
	smallSize   = "5368709120"

	// noDiffJSON and someDiffJSON are the outputs of
	// "rbd diff --format json" with and without any differing extent.
	noDiffJSON   = `[]`
	someDiffJSON = `[{"offset":0,"length":4194304,"exists":"true"}]`

	// nullDiffJSON and objectDiffJSON are JSON values that "rbd diff" never
	// prints today. The script must not mistake them for "no differing
	// extent".
	nullDiffJSON   = `null`
	objectDiffJSON = `{}`
)

type fakeRBDConfig struct {
	headSize  string
	snapSize  string
	diffJSON  string
	diffFails bool
	// diffPrintsNothing makes the stubbed "rbd diff" succeed without printing
	// anything, which makes "jq -c" print nothing as well.
	diffPrintsNothing bool
	infoFails         string // "", "head" or "snap"
	// infoOmitsSize makes the stubbed "rbd info" print JSON without the size
	// field, which makes "jq -r .size" print "null".
	infoOmitsSize bool
	snapLsOutput  string

	certFile            string
	transferCompression string
}

type importScriptResult struct {
	// calls holds the arguments of every command invocation in the order the
	// commands were started.
	calls []string

	// importStdin holds the data "rbd import-diff" received on its stdin.
	importStdin string

	// output holds everything the script printed.
	output string
	err    error
}

// runImportScript runs the import Job script with the rbd command stubbed out.
func runImportScript(t *testing.T, fromSnapName string, conf fakeRBDConfig) importScriptResult {
	t.Helper()

	if conf.headSize == "" {
		conf.headSize = defaultSize
	}
	if conf.snapSize == "" {
		conf.snapSize = defaultSize
	}
	if conf.diffJSON == "" {
		conf.diffJSON = noDiffJSON
	}

	// The directory name contains a space so that the tests catch unquoted
	// uses of the paths in the script.
	dir := filepath.Join(t.TempDir(), "dir with space")
	binDir := filepath.Join(dir, "bin")
	require.NoError(t, os.MkdirAll(binDir, 0o755))
	for name, content := range map[string]string{
		"rbd":   fakeRBDScript,
		"s5cmd": fakeS5cmdScript,
		"zstd":  fakeZstdScript,
	} {
		require.NoError(t, os.WriteFile(filepath.Join(binDir, name), []byte(content), 0o755))
	}

	monConfig := filepath.Join(dir, "mon-endpoints")
	require.NoError(t, os.WriteFile(monConfig, []byte("a=192.168.0.1:3300,b=192.168.0.2:3300,c=192.168.0.3:3300"), 0o644))
	cmdLog := filepath.Join(dir, "cmd.log")
	importStdinFile := filepath.Join(dir, "import-stdin")
	for _, path := range []string{cmdLog, importStdinFile} {
		require.NoError(t, os.WriteFile(path, nil, 0o644))
	}

	cmd := exec.Command("/bin/bash", "-c", EmbedJobImportScript)
	cmd.Env = append(os.Environ(),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"CEPH_CONFIG="+filepath.Join(dir, "ceph.conf"),
		"MON_CONFIG="+monConfig,
		"KEYRING_FILE="+filepath.Join(dir, "keyring"),
		"ROOK_CEPH_USERNAME=client.admin",
		"ROOK_CEPH_SECRET=dummy-secret",
		"POOL_NAME=pool",
		"DST_IMAGE_NAME=image",
		"FROM_SNAP_NAME="+fromSnapName,
		"OBJ_NAME=obj",
		"BUCKET_NAME=bucket",
		"OBJECT_STORAGE_ENDPOINT=http://localhost:9000",
		"CERT_FILE="+conf.certFile,
		"TRANSFER_COMPRESSION="+conf.transferCompression,
		"IMPORT_STDIN_FILE="+importStdinFile,
		"EXPORTED_DATA="+exportedData,
		"CMD_LOG="+cmdLog,
		"FAKE_HEAD_SIZE="+conf.headSize,
		"FAKE_SNAP_SIZE="+conf.snapSize,
		"FAKE_DIFF_OUTPUT="+conf.diffJSON,
		"FAKE_DIFF_FAILS="+strconv.FormatBool(conf.diffFails),
		"FAKE_DIFF_PRINTS_NOTHING="+strconv.FormatBool(conf.diffPrintsNothing),
		"FAKE_INFO_FAILS="+conf.infoFails,
		"FAKE_INFO_OMITS_SIZE="+strconv.FormatBool(conf.infoOmitsSize),
		"FAKE_SNAP_LS_OUTPUT="+conf.snapLsOutput,
	)
	out, err := cmd.CombinedOutput()
	t.Logf("script output:\n%s", out)

	importStdin, readErr := os.ReadFile(importStdinFile)
	require.NoError(t, readErr)

	return importScriptResult{
		calls:       readLogLines(t, cmdLog),
		importStdin: string(importStdin),
		output:      string(out),
		err:         err,
	}
}

func readLogLines(t *testing.T, path string) []string {
	t.Helper()

	content, err := os.ReadFile(path)
	require.NoError(t, err)
	var lines []string
	for line := range strings.SplitSeq(string(content), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}

	return lines
}

// assertCalls checks that the script made exactly the calls in wantCalls, and
// that it made the ones in wantOrderedCalls in that order.
//
// The commands of a pipeline are started concurrently, so the script only
// guarantees the order of the commands it runs sequentially. wantCalls
// therefore ignores the order, and wantOrderedCalls lists the subset of the
// calls whose order is deterministic.
func assertCalls(t *testing.T, wantCalls, wantOrderedCalls, gotCalls []string) {
	t.Helper()

	assert.ElementsMatch(t, wantCalls, gotCalls, "calls")

	wanted := make(map[string]struct{}, len(wantOrderedCalls))
	for _, call := range wantOrderedCalls {
		wanted[call] = struct{}{}
	}
	var gotOrderedCalls []string
	for _, call := range gotCalls {
		if _, ok := wanted[call]; ok {
			gotOrderedCalls = append(gotOrderedCalls, call)
		}
	}
	assert.Equal(t, wantOrderedCalls, gotOrderedCalls, "ordered calls")
}

// TestImportJobScriptRollback checks which rbd commands the script runs, i.e.
// that it rolls the destination image back to the snapshot the incremental
// data is based on, and that it skips the rollback exactly when the HEAD is
// already identical to that snapshot.
func TestImportJobScriptRollback(t *testing.T) {
	tests := []struct {
		name          string
		fromSnapName  string
		conf          fakeRBDConfig
		expectFailure bool
		wantCalls     []string
		// wantOrderedCalls lists the rbd calls, which the script runs
		// sequentially, so their order is deterministic.
		wantOrderedCalls []string
		// wantOutput holds the substrings the script must print, so that an
		// operator can tell from the log why the rollback was performed or
		// skipped.
		wantOutput []string
	}{
		{
			name:             "incremental: skip rollback if the HEAD is identical to the snapshot",
			fromSnapName:     "snap1",
			wantCalls:        []string{rbdInfoHead, rbdInfoSnap1, rbdDiffSnap1, s5cmdCat, rbdImportDiff},
			wantOrderedCalls: []string{rbdInfoHead, rbdInfoSnap1, rbdDiffSnap1, rbdImportDiff},
			wantOutput:       []string{"skip rollback: pool/image is already identical to snap1"},
		},
		{
			name:         "incremental: roll back if the HEAD has extents updated after the snapshot",
			fromSnapName: "snap1",
			conf:         fakeRBDConfig{diffJSON: someDiffJSON},
			wantCalls: []string{
				rbdInfoHead, rbdInfoSnap1, rbdDiffSnap1, rbdRollbackSnap1, s5cmdCat, rbdImportDiff,
			},
			wantOrderedCalls: []string{rbdInfoHead, rbdInfoSnap1, rbdDiffSnap1, rbdRollbackSnap1, rbdImportDiff},
			wantOutput: []string{
				"rollback needed: pool/image differs from snap1: " +
					"head_size=10737418240 snap_size=10737418240 diff=" + someDiffJSON,
			},
		},
		{
			name:             "incremental: roll back if the HEAD is smaller than the snapshot",
			fromSnapName:     "snap1",
			conf:             fakeRBDConfig{headSize: smallSize},
			wantCalls:        []string{rbdInfoHead, rbdInfoSnap1, rbdRollbackSnap1, s5cmdCat, rbdImportDiff},
			wantOrderedCalls: []string{rbdInfoHead, rbdInfoSnap1, rbdRollbackSnap1, rbdImportDiff},
			wantOutput: []string{
				"rollback needed: pool/image differs from snap1: " +
					"head_size=5368709120 snap_size=10737418240 diff=not compared",
			},
		},
		{
			name:             "incremental: roll back if the HEAD is larger than the snapshot",
			fromSnapName:     "snap1",
			conf:             fakeRBDConfig{snapSize: smallSize},
			wantCalls:        []string{rbdInfoHead, rbdInfoSnap1, rbdRollbackSnap1, s5cmdCat, rbdImportDiff},
			wantOrderedCalls: []string{rbdInfoHead, rbdInfoSnap1, rbdRollbackSnap1, rbdImportDiff},
			wantOutput: []string{
				"rollback needed: pool/image differs from snap1: " +
					"head_size=10737418240 snap_size=5368709120 diff=not compared",
			},
		},
		{
			// "rbd diff" prints nothing only if something is wrong with it,
			// so the HEAD must not be assumed to be identical to the snapshot.
			name:         "incremental: roll back if rbd diff prints nothing",
			fromSnapName: "snap1",
			conf:         fakeRBDConfig{diffPrintsNothing: true},
			wantCalls: []string{
				rbdInfoHead, rbdInfoSnap1, rbdDiffSnap1, rbdRollbackSnap1, s5cmdCat, rbdImportDiff,
			},
			wantOrderedCalls: []string{rbdInfoHead, rbdInfoSnap1, rbdDiffSnap1, rbdRollbackSnap1, rbdImportDiff},
		},
		{
			name:         "incremental: roll back if rbd diff prints a JSON null",
			fromSnapName: "snap1",
			conf:         fakeRBDConfig{diffJSON: nullDiffJSON},
			wantCalls: []string{
				rbdInfoHead, rbdInfoSnap1, rbdDiffSnap1, rbdRollbackSnap1, s5cmdCat, rbdImportDiff,
			},
			wantOrderedCalls: []string{rbdInfoHead, rbdInfoSnap1, rbdDiffSnap1, rbdRollbackSnap1, rbdImportDiff},
		},
		{
			name:         "incremental: roll back if rbd diff prints a JSON object",
			fromSnapName: "snap1",
			conf:         fakeRBDConfig{diffJSON: objectDiffJSON},
			wantCalls: []string{
				rbdInfoHead, rbdInfoSnap1, rbdDiffSnap1, rbdRollbackSnap1, s5cmdCat, rbdImportDiff,
			},
			wantOrderedCalls: []string{rbdInfoHead, rbdInfoSnap1, rbdDiffSnap1, rbdRollbackSnap1, rbdImportDiff},
		},
		{
			// "jq -r .size" prints "null" for JSON without a size field, which
			// is not a number, so the sizes must not be taken for equal.
			name:             "incremental: roll back if rbd info prints no size",
			fromSnapName:     "snap1",
			conf:             fakeRBDConfig{infoOmitsSize: true},
			wantCalls:        []string{rbdInfoHead, rbdInfoSnap1, rbdRollbackSnap1, s5cmdCat, rbdImportDiff},
			wantOrderedCalls: []string{rbdInfoHead, rbdInfoSnap1, rbdRollbackSnap1, rbdImportDiff},
			wantOutput: []string{
				"rollback needed: pool/image differs from snap1: " +
					"head_size=null snap_size=null diff=not compared",
			},
		},
		{
			name:             "incremental: fail without importing if rbd info fails for the HEAD",
			fromSnapName:     "snap1",
			conf:             fakeRBDConfig{infoFails: "head"},
			expectFailure:    true,
			wantCalls:        []string{rbdInfoHead},
			wantOrderedCalls: []string{rbdInfoHead},
		},
		{
			name:             "incremental: fail without importing if rbd info fails for the snapshot",
			fromSnapName:     "snap1",
			conf:             fakeRBDConfig{infoFails: "snap"},
			expectFailure:    true,
			wantCalls:        []string{rbdInfoHead, rbdInfoSnap1},
			wantOrderedCalls: []string{rbdInfoHead, rbdInfoSnap1},
		},
		{
			name:             "incremental: fail without importing if rbd diff fails",
			fromSnapName:     "snap1",
			conf:             fakeRBDConfig{diffFails: true},
			expectFailure:    true,
			wantCalls:        []string{rbdInfoHead, rbdInfoSnap1, rbdDiffSnap1},
			wantOrderedCalls: []string{rbdInfoHead, rbdInfoSnap1, rbdDiffSnap1},
		},
		{
			name:             "full: create initialsnap without rolling back if it doesn't exist",
			fromSnapName:     "",
			wantCalls:        []string{rbdSnapLs, rbdCreateInitialSnap, s5cmdCat, rbdImportDiff, rbdRmInitialSnap},
			wantOrderedCalls: []string{rbdSnapLs, rbdCreateInitialSnap, rbdImportDiff, rbdRmInitialSnap},
		},
		{
			name:         "full: skip rollback if the HEAD is identical to initialsnap",
			fromSnapName: "",
			conf:         fakeRBDConfig{snapLsOutput: initialSnapLsOutput},
			wantCalls: []string{
				rbdSnapLs, rbdInfoHead, rbdInfoInitialSnap, rbdDiffInitialSnap,
				s5cmdCat, rbdImportDiff, rbdRmInitialSnap,
			},
			wantOrderedCalls: []string{
				rbdSnapLs, rbdInfoHead, rbdInfoInitialSnap, rbdDiffInitialSnap, rbdImportDiff, rbdRmInitialSnap,
			},
			wantOutput: []string{"skip rollback: pool/image is already identical to initialsnap"},
		},
		{
			name:         "full: roll back if the HEAD differs from initialsnap",
			fromSnapName: "",
			conf:         fakeRBDConfig{diffJSON: someDiffJSON, snapLsOutput: initialSnapLsOutput},
			wantCalls: []string{
				rbdSnapLs, rbdInfoHead, rbdInfoInitialSnap, rbdDiffInitialSnap,
				rbdRollbackInitialSnap, s5cmdCat, rbdImportDiff, rbdRmInitialSnap,
			},
			wantOrderedCalls: []string{
				rbdSnapLs, rbdInfoHead, rbdInfoInitialSnap, rbdDiffInitialSnap,
				rbdRollbackInitialSnap, rbdImportDiff, rbdRmInitialSnap,
			},
			wantOutput: []string{
				"rollback needed: pool/image differs from initialsnap: " +
					"head_size=10737418240 snap_size=10737418240 diff=" + someDiffJSON,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := runImportScript(t, tt.fromSnapName, tt.conf)
			if tt.expectFailure {
				require.Error(t, res.err, "the script unexpectedly succeeded")
			} else {
				require.NoError(t, res.err, "the script unexpectedly failed")
			}

			assertCalls(t, tt.wantCalls, tt.wantOrderedCalls, res.calls)
			if !tt.expectFailure {
				assert.Equal(t, exportedData, res.importStdin, "import-diff stdin")
			}

			for _, want := range tt.wantOutput {
				assert.Contains(t, res.output, want, "script output")
			}
		})
	}
}

// TestImportJobScriptImportPaths checks the commands the script runs to fetch
// and apply the exported data, i.e. that the correct branch of the function
// rbd_import is taken.
func TestImportJobScriptImportPaths(t *testing.T) {
	tests := []struct {
		name      string
		conf      fakeRBDConfig
		wantCalls []string
	}{
		{
			name:      "no credentials file and no compression",
			wantCalls: []string{rbdInfoHead, rbdInfoSnap1, rbdDiffSnap1, s5cmdCat, rbdImportDiff},
		},
		{
			name:      "credentials file without compression",
			conf:      fakeRBDConfig{certFile: certFile},
			wantCalls: []string{rbdInfoHead, rbdInfoSnap1, rbdDiffSnap1, s5cmdCatWithCert, rbdImportDiff},
		},
		{
			name: "zstd compression without credentials file",
			conf: fakeRBDConfig{transferCompression: "zstd"},
			wantCalls: []string{
				rbdInfoHead, rbdInfoSnap1, rbdDiffSnap1, s5cmdCat, zstdDecompress, rbdImportDiff,
			},
		},
		{
			name: "credentials file with zstd compression",
			conf: fakeRBDConfig{
				certFile:            certFile,
				transferCompression: "zstd",
			},
			wantCalls: []string{
				rbdInfoHead, rbdInfoSnap1, rbdDiffSnap1, s5cmdCatWithCert, zstdDecompress, rbdImportDiff,
			},
		},
	}

	// The rbd commands run sequentially, so the script always checks whether
	// the image needs the rollback before it applies the data, whichever
	// branch it takes.
	wantOrderedCalls := []string{rbdInfoHead, rbdInfoSnap1, rbdDiffSnap1, rbdImportDiff}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := runImportScript(t, "snap1", tt.conf)
			require.NoError(t, res.err, "the script unexpectedly failed")

			assertCalls(t, tt.wantCalls, wantOrderedCalls, res.calls)
			assert.Equal(t, exportedData, res.importStdin, "import-diff stdin")
		})
	}
}
