package controller

import (
	"os"
	"os/exec"
	"path/filepath"
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
	rbdRollbackSnap1       = "rbd snap rollback pool/image@snap1"
	rbdImportDiff          = "rbd import-diff -p pool - image"
	rbdSnapLs              = "rbd snap ls pool/image"
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

type fakeRBDConfig struct {
	snapLsOutput        string
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

// runImportScript runs the import job script with the stubbed commands.
func runImportScript(t *testing.T, fromSnapName string, conf fakeRBDConfig) importScriptResult {
	t.Helper()

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
// data is based on before it applies the data.
func TestImportJobScriptRollback(t *testing.T) {
	tests := []struct {
		name             string
		fromSnapName     string
		conf             fakeRBDConfig
		wantCalls        []string
		wantOrderedCalls []string
	}{
		{
			name:             "incremental: roll back to the base snapshot before importing",
			fromSnapName:     "snap1",
			wantCalls:        []string{rbdRollbackSnap1, s5cmdCat, rbdImportDiff},
			wantOrderedCalls: []string{rbdRollbackSnap1, rbdImportDiff},
		},
		{
			name:             "full: create initialsnap without rolling back if it doesn't exist",
			fromSnapName:     "",
			wantCalls:        []string{rbdSnapLs, rbdCreateInitialSnap, s5cmdCat, rbdImportDiff, rbdRmInitialSnap},
			wantOrderedCalls: []string{rbdSnapLs, rbdCreateInitialSnap, rbdImportDiff, rbdRmInitialSnap},
		},
		{
			name:             "full: roll back to initialsnap if it already exists",
			fromSnapName:     "",
			conf:             fakeRBDConfig{snapLsOutput: initialSnapLsOutput},
			wantCalls:        []string{rbdSnapLs, rbdRollbackInitialSnap, s5cmdCat, rbdImportDiff, rbdRmInitialSnap},
			wantOrderedCalls: []string{rbdSnapLs, rbdRollbackInitialSnap, rbdImportDiff, rbdRmInitialSnap},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := runImportScript(t, tt.fromSnapName, tt.conf)
			require.NoError(t, res.err, "the script unexpectedly failed")

			assertCalls(t, tt.wantCalls, tt.wantOrderedCalls, res.calls)
			assert.Equal(t, exportedData, res.importStdin, "import-diff stdin")
		})
	}
}

// TestImportJobScriptImportPaths checks that the incremental data is fetched
// and decompressed as the environment tells, whichever of the four branches of
// rbd_import is taken.
func TestImportJobScriptImportPaths(t *testing.T) {
	tests := []struct {
		name      string
		conf      fakeRBDConfig
		wantCalls []string
	}{
		{
			name:      "no credentials file and no compression",
			wantCalls: []string{rbdRollbackSnap1, s5cmdCat, rbdImportDiff},
		},
		{
			name:      "credentials file without compression",
			conf:      fakeRBDConfig{certFile: certFile},
			wantCalls: []string{rbdRollbackSnap1, s5cmdCatWithCert, rbdImportDiff},
		},
		{
			name:      "zstd compression without credentials file",
			conf:      fakeRBDConfig{transferCompression: "zstd"},
			wantCalls: []string{rbdRollbackSnap1, s5cmdCat, zstdDecompress, rbdImportDiff},
		},
		{
			name: "credentials file with zstd compression",
			conf: fakeRBDConfig{
				certFile:            certFile,
				transferCompression: "zstd",
			},
			wantCalls: []string{rbdRollbackSnap1, s5cmdCatWithCert, zstdDecompress, rbdImportDiff},
		},
	}

	// The rbd commands run sequentially, so the script always rolls the image
	// back before it applies the data, whichever branch it takes.
	wantOrderedCalls := []string{rbdRollbackSnap1, rbdImportDiff}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := runImportScript(t, "snap1", tt.conf)
			require.NoError(t, res.err, "the script unexpectedly failed")

			assertCalls(t, tt.wantCalls, wantOrderedCalls, res.calls)
			assert.Equal(t, exportedData, res.importStdin, "import-diff stdin")
		})
	}
}
