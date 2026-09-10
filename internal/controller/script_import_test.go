package controller

import (
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// stubRbdScript records every invocation of the rbd command and emulates the
// subcommands used by the import and zeroout Jobs. Its behavior is controlled
// by the environment variables prefixed with STUB_, the image metadata store
// STUB_META_FILE, and the snapshot store STUB_SNAP_FILE.
const stubRbdScript = `#!/bin/bash
# Consume the global options given by the zeroout Job. Note that rbd reads the
# file given to --keyfile with fstat(2) and read(2), so a pipe such as a
# process substitution is read as an empty key and cephx gets disabled.
# cf. bufferlist::read_file() called from md_config_t::parse_argv()
while [[ "$1" == --* ]]; do
    if [ "$1" = "--keyfile" ] && { [ ! -f "$2" ] || [ ! -s "$2" ]; }; then
        echo "auth: unable to find a keyring: (2) No such file or directory" >&2
        echo "rbd: couldn't connect to the cluster!" >&2
        exit 13
    fi
    shift 2
done
echo "$@" >> "${RBD_LOG}"

# rbd may write warnings to stderr even if it succeeds.
warn_if_requested() {
    if [ "${STUB_RBD_WARNING}" = "true" ]; then
        echo "rbd: this is a harmless warning" >&2
    fi
}

# The snapshots of the image are kept in STUB_SNAP_FILE, one name per line, so
# that the tests can observe the snapshots created and removed across the runs
# of a script.
snap_exists() {
    grep -qx -- "$1" "${STUB_SNAP_FILE}"
}

snap_add() {
    echo "$1" >> "${STUB_SNAP_FILE}"
}

snap_remove() {
    grep -vx -- "$1" "${STUB_SNAP_FILE}" > "${STUB_SNAP_FILE}.tmp" || true
    mv "${STUB_SNAP_FILE}.tmp" "${STUB_SNAP_FILE}"
}

# rbd snap ls prints at least an empty array in the JSON format.
print_snaps_json() {
    local separator="" name
    printf '['
    while read -r name; do
        [ -n "${name}" ] || continue
        printf '%s{"id":4,"name":"%s","size":1073741824}' "${separator}" "${name}"
        separator=","
    done < "${STUB_SNAP_FILE}"
    printf ']\n'
}

# fail_count_reached reports whether the given subcommand has been invoked as
# many times as the given limit, so that a test can fail only one of the
# invocations of a subcommand.
fail_count_reached() {
    local limit="$2"
    if [ -z "${limit}" ]; then
        return 1
    fi
    [ "$(grep -c "^$1 " "${RBD_LOG}")" = "${limit}" ]
}

case "$1 $2" in
    "info --format")
        if [ "${STUB_INFO_FAIL}" = "true" ]; then
            echo "rbd: error opening image: (110) Connection timed out" >&2
            exit 110
        fi
        # rbd always reports the size, but the script must not mistake the
        # absence of the size for two equal sizes.
        if [ "${STUB_INFO_NO_SIZE}" = "true" ]; then
            echo '{"name":"image"}'
            exit 0
        fi
        # The last argument is an image or a snapshot spec.
        if [[ "${!#}" == *@* ]]; then
            echo "{\"size\": ${STUB_SNAP_SIZE}}"
        else
            echo "{\"size\": ${STUB_HEAD_SIZE}}"
        fi
        ;;
    "snap ls")
        if [[ "$*" != *"--format json"* ]]; then
            echo "stub: rbd snap ls is supported only with --format json" >&2
            exit 22
        fi
        if [ "${STUB_SNAP_LS_FAIL}" = "true" ]; then
            echo "rbd: failed to list snapshots: (110) Connection timed out" >&2
            exit 110
        fi
        if [ "${STUB_SNAP_LS_INVALID}" = "true" ]; then
            echo "this is not JSON"
            exit 0
        fi
        warn_if_requested
        print_snaps_json
        ;;
    "snap create")
        name="${3##*@}"
        if snap_exists "${name}"; then
            echo "rbd: failed to create snapshot: (17) File exists" >&2
            exit 17
        fi
        snap_add "${name}"
        ;;
    "snap rm")
        name="${3##*@}"
        if ! snap_exists "${name}"; then
            echo "rbd: failed to remove snapshot: (2) No such file or directory" >&2
            exit 2
        fi
        if fail_count_reached "snap rm" "${STUB_SNAP_RM_FAIL_AT}"; then
            echo "rbd: failed to remove snapshot: (110) Connection timed out" >&2
            exit 110
        fi
        snap_remove "${name}"
        ;;
    "snap rollback")
        if [ "${STUB_ROLLBACK_FAIL}" = "true" ]; then
            echo "rollback failed" >&2
            exit 110
        fi
        ;;
    "image-meta list")
        if fail_count_reached "image-meta list" "${STUB_META_LIST_FAIL_AT}"; then
            echo "failed to list metadata of image : (110) Connection timed out" >&2
            exit 110
        fi
        if [ "${STUB_META_LIST_INVALID}" = "true" ]; then
            echo "this is not JSON"
            exit 0
        fi
        warn_if_requested
        # rbd prints nothing if the image has no metadata at all.
        if [ -s "${STUB_META_FILE}" ]; then
            separator=""
            printf '{'
            while IFS='=' read -r key value; do
                if [ -n "${key}" ]; then
                    printf '%s"%s":"%s"' "${separator}" "${key}" "${value}"
                    separator=","
                fi
            done < "${STUB_META_FILE}"
            printf '}\n'
        fi
        ;;
    "image-meta set")
        if [ "${STUB_META_SET_FAIL}" = "true" ]; then
            echo "metadata update failed" >&2
            exit 110
        fi
        grep -v "^$4=" "${STUB_META_FILE}" > "${STUB_META_FILE}.tmp" || true
        mv "${STUB_META_FILE}.tmp" "${STUB_META_FILE}"
        echo "$4=$5" >> "${STUB_META_FILE}"
        ;;
    "image-meta remove")
        if [ "${STUB_META_REMOVE_FAIL}" = "true" ]; then
            echo "metadata removal failed" >&2
            exit 110
        fi
        if ! grep -q "^$4=" "${STUB_META_FILE}"; then
            echo "rbd: no existing metadata key $4 of image : (2) No such file or directory" >&2
            exit 2
        fi
        grep -v "^$4=" "${STUB_META_FILE}" > "${STUB_META_FILE}.tmp" || true
        mv "${STUB_META_FILE}.tmp" "${STUB_META_FILE}"
        ;;
    "import-diff "*)
        cat > /dev/null
        # rbd import-diff aborts if the snapshot it has to create already
        # exists, and creates it only after the whole diff has been applied.
        # cf. do_image_snap_to() and do_import_diff_fd() in rbd/action/Import.cc
        if [ -z "${TO_SNAP_NAME}" ]; then
            exit 0
        fi
        if snap_exists "${TO_SNAP_NAME}"; then
            echo "end snapshot '${TO_SNAP_NAME}' already exists, aborting" >&2
            exit 17
        fi
        if [ "${STUB_IMPORT_NO_SNAP}" != "true" ]; then
            snap_add "${TO_SNAP_NAME}"
        fi
        ;;
esac
exit 0
`

// Snapshot names shared by the tests of the Job scripts.
const (
	snapName1 = "snap1"
	snapName2 = "snap2"

	// initialSnapName is the snapshot the full import creates to be able to
	// roll the image back if it is interrupted halfway.
	initialSnapName = "initialsnap"

	// fullImportToSnap is the snapshot a full import creates, i.e., the name
	// of the MantleBackup being imported.
	fullImportToSnap = "backup1"
)

const stubS5cmdScript = `#!/bin/bash
echo "exported data"
exit 0
`

const stubBlkdiscardScript = `#!/bin/bash
echo "blkdiscard $*" >> "${RBD_LOG}"
exit 0
`

func writeStub(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
}

type importScriptResult struct {
	rbdArgs  []string
	metaFile string
	snapFile string
	stdout   string
	// err is the error of the first run that failed, if any.
	err error
	// errs holds the error of every run in order, nil if the run succeeded.
	errs []error
}

func (r *importScriptResult) invoked(subcommand string) bool {
	return r.indexOf(subcommand) >= 0
}

func (r *importScriptResult) indexOf(subcommand string) int {
	for i, args := range r.rbdArgs {
		if strings.HasPrefix(args, subcommand) {
			return i
		}
	}

	return -1
}

// count returns how many times the given subcommand was invoked across all the
// runs, which tells whether a retry repeated an operation it should have
// skipped.
func (r *importScriptResult) count(subcommand string) int {
	n := 0
	for _, args := range r.rbdArgs {
		if strings.HasPrefix(args, subcommand) {
			n++
		}
	}

	return n
}

func (r *importScriptResult) metadata(t *testing.T) map[string]string {
	t.Helper()

	raw, err := os.ReadFile(r.metaFile)
	if err != nil {
		t.Fatal(err)
	}
	meta := map[string]string{}
	for line := range strings.SplitSeq(string(raw), "\n") {
		if k, v, ok := strings.Cut(line, "="); ok {
			meta[k] = v
		}
	}

	return meta
}

func (r *importScriptResult) snapshots(t *testing.T) []string {
	t.Helper()

	raw, err := os.ReadFile(r.snapFile)
	if err != nil {
		t.Fatal(err)
	}
	var snaps []string
	for line := range strings.SplitSeq(string(raw), "\n") {
		if line != "" {
			snaps = append(snaps, line)
		}
	}

	return snaps
}

// scriptRun is a single run of a Job script with the environment variables that
// are specific to it, which lets a test emulate the retries of a Job and the
// consecutive parts of a multipart import.
type scriptRun struct {
	script string
	env    map[string]string
}

// runImportScript runs the embedded job-import.sh with the stub commands and
// returns the recorded rbd invocations. The given env entries override the
// default ones.
func runImportScript(t *testing.T, meta map[string]string, env map[string]string) *importScriptResult {
	t.Helper()
	result := runImportScriptWithError(t, meta, env)
	if result.err != nil {
		t.Fatalf("job-import.sh failed: %v\n%s", result.err, result.stdout)
	}

	return result
}

func runImportScriptWithError(t *testing.T, meta map[string]string, env map[string]string) *importScriptResult {
	t.Helper()

	return runRBDJobScripts(t, meta, env, EmbedJobImportScript)
}

// runRBDJobScripts runs the given Job scripts in this order with the stub
// commands, sharing the image metadata, the snapshots, and the recorded
// invocations among them.
func runRBDJobScripts(
	t *testing.T,
	meta map[string]string,
	env map[string]string,
	scripts ...string,
) *importScriptResult {
	t.Helper()

	runs := make([]scriptRun, 0, len(scripts))
	for _, script := range scripts {
		runs = append(runs, scriptRun{script: script})
	}

	return runScriptRuns(t, meta, env, runs)
}

// runScriptRuns runs every given run in order even if some of them fail, so
// that a test can observe how a Job recovers on a retry.
func runScriptRuns(
	t *testing.T,
	meta map[string]string,
	env map[string]string,
	runs []scriptRun,
) *importScriptResult {
	t.Helper()

	dir := t.TempDir()
	binDir := filepath.Join(dir, "bin")
	if err := os.Mkdir(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeStub(t, filepath.Join(binDir, "rbd"), stubRbdScript)
	writeStub(t, filepath.Join(binDir, "s5cmd"), stubS5cmdScript)
	writeStub(t, filepath.Join(binDir, "blkdiscard"), stubBlkdiscardScript)

	monConfig := filepath.Join(dir, "mon-endpoints")
	if err := os.WriteFile(monConfig, []byte("a=192.168.0.1:6789\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	rbdLog := filepath.Join(dir, "rbd.log")
	if err := os.WriteFile(rbdLog, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	var metaContent strings.Builder
	for k, v := range meta {
		metaContent.WriteString(k + "=" + v + "\n")
	}
	metaFile := filepath.Join(dir, "image-meta")
	if err := os.WriteFile(metaFile, []byte(metaContent.String()), 0o644); err != nil {
		t.Fatal(err)
	}

	envs := map[string]string{
		"PATH":                    binDir + string(os.PathListSeparator) + os.Getenv("PATH"),
		"CEPH_CONFIG":             filepath.Join(dir, "ceph.conf"),
		"KEYRING_FILE":            filepath.Join(dir, "keyring"),
		"MON_CONFIG":              monConfig,
		"RBD_LOG":                 rbdLog,
		"STUB_META_FILE":          metaFile,
		"ROOK_CEPH_SECRET":        "dummy-secret",
		"ROOK_CEPH_USERNAME":      "client.admin",
		"MON_ENDPOINTS":           "a=192.168.0.1:6789",
		"POOL_NAME":               "pool",
		"DST_IMAGE_NAME":          "image",
		"OBJ_NAME":                "obj",
		"BUCKET_NAME":             "bucket",
		"OBJECT_STORAGE_ENDPOINT": "http://localhost:9000",
		"CERT_FILE":               "",
		"TRANSFER_COMPRESSION":    "",
		"FROM_SNAP_NAME":          "",
		"TO_SNAP_NAME":            snapName2,
		"STUB_HEAD_SIZE":          "1073741824",
		"STUB_SNAP_SIZE":          "1073741824",
		// The snapshots the destination image has before the first run. The
		// import creates TO_SNAP_NAME, so it must not be listed here unless
		// the test emulates an import that has already been applied. Note that
		// the snapshot store is shared by the runs, so this is read from the
		// common env only, not from the env of an individual run.
		"STUB_SNAPS": snapName1,
	}
	maps.Copy(envs, env)

	// The snapshot store is shared by the runs, but its initial contents are
	// given per test, so it is written after the overrides are applied.
	snapFile := filepath.Join(dir, "snapshots")
	var snapContent strings.Builder
	for name := range strings.FieldsSeq(envs["STUB_SNAPS"]) {
		snapContent.WriteString(name + "\n")
	}
	if err := os.WriteFile(snapFile, []byte(snapContent.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	envs["STUB_SNAP_FILE"] = snapFile

	var stdout strings.Builder
	errs := make([]error, 0, len(runs))
	var firstErr error
	for _, run := range runs {
		runEnvs := maps.Clone(envs)
		maps.Copy(runEnvs, run.env)

		// Pass only the variables above so that the environment of the test
		// process doesn't affect the result.
		cmdEnv := make([]string, 0, len(runEnvs))
		for k, v := range runEnvs {
			cmdEnv = append(cmdEnv, k+"="+v)
		}

		cmd := exec.Command("bash", "-c", run.script)
		cmd.Env = cmdEnv
		out, err := cmd.CombinedOutput()
		stdout.Write(out)
		errs = append(errs, err)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	rawLog, err := os.ReadFile(rbdLog)
	if err != nil {
		t.Fatal(err)
	}
	var rbdArgs []string
	for line := range strings.SplitSeq(string(rawLog), "\n") {
		if line != "" {
			rbdArgs = append(rbdArgs, line)
		}
	}

	return &importScriptResult{
		rbdArgs:  rbdArgs,
		metaFile: metaFile,
		snapFile: snapFile,
		stdout:   stdout.String(),
		err:      firstErr,
		errs:     errs,
	}
}

func TestImportScriptStopsOnMetadataError(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
	}{
		{"full import read", map[string]string{"STUB_META_LIST_FAIL_AT": "1"}},
		{"rollback decision read", map[string]string{"FROM_SNAP_NAME": snapName1, "STUB_META_LIST_FAIL_AT": "1"}},
		{"before import read", map[string]string{"FROM_SNAP_NAME": snapName1, "STUB_META_LIST_FAIL_AT": "2"}},
		{"full import removal", map[string]string{"STUB_META_REMOVE_FAIL": "true"}},
		{"incremental import removal", map[string]string{"FROM_SNAP_NAME": snapName1, "STUB_META_REMOVE_FAIL": "true"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runImportScriptWithError(t, map[string]string{"mantle.clean-snap": snapName1}, tt.env)
			if result.err == nil {
				t.Error("expected the script to fail")
			}
			for _, command := range []string{"import-diff", "snap rollback", "snap create", "image-meta set"} {
				if result.invoked(command) {
					t.Errorf("%s must not run after a metadata error; invocations: %v", command, result.rbdArgs)
				}
			}
			if got := result.metadata(t)["mantle.clean-snap"]; got != snapName1 {
				t.Errorf("metadata = %q, want snap1", got)
			}
		})
	}
}

func TestImportScriptStopsOnRBDError(t *testing.T) {
	// The script must not silently ignore the errors of the rbd commands it
	// bases the rollback decision and the recorded state on. In particular, a
	// failure to list the snapshots must not be mistaken for the absence of
	// the snapshot the import creates.
	tests := []struct {
		name string
		env  map[string]string
	}{
		{
			name: "the size of the image is unavailable",
			env:  map[string]string{"FROM_SNAP_NAME": snapName1, "STUB_INFO_FAIL": "true"},
		},
		{
			name: "the snapshots of the image are unavailable",
			env:  map[string]string{"FROM_SNAP_NAME": snapName1, "STUB_SNAP_LS_FAIL": "true"},
		},
		{
			name: "the snapshot list is not valid JSON",
			env:  map[string]string{"FROM_SNAP_NAME": snapName1, "STUB_SNAP_LS_INVALID": "true"},
		},
		{
			// Two missing sizes must not compare equal and skip the rollback.
			name: "the size of the image is missing from the output",
			env:  map[string]string{"FROM_SNAP_NAME": snapName1, "STUB_INFO_NO_SIZE": "true"},
		},
		{
			name: "the metadata is not valid JSON",
			env:  map[string]string{"FROM_SNAP_NAME": snapName1, "STUB_META_LIST_INVALID": "true"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runImportScriptWithError(t,
				map[string]string{"mantle.clean-snap": snapName1},
				tt.env,
			)
			if result.err == nil {
				t.Errorf("expected the script to fail; invocations: %v", result.rbdArgs)
			}
			for _, command := range []string{"snap rollback", "import-diff", "image-meta set"} {
				if result.invoked(command) {
					t.Errorf("%s must not run after an rbd error; invocations: %v", command, result.rbdArgs)
				}
			}
		})
	}
}

func TestImportScriptIgnoresRBDWarnings(t *testing.T) {
	// rbd may write warnings to stderr even if it succeeds. They must not be
	// taken as a part of the recorded snapshot name.
	result := runImportScript(t,
		map[string]string{"mantle.clean-snap": snapName1},
		map[string]string{
			"FROM_SNAP_NAME":   snapName1,
			"STUB_RBD_WARNING": "true",
		},
	)

	if result.invoked("snap rollback") {
		t.Errorf("the rollback must be skipped; invocations: %v", result.rbdArgs)
	}
	if !strings.Contains(result.stdout, "skip rollback") {
		t.Errorf("the skip of the rollback is not logged; stdout:\n%s", result.stdout)
	}
}

func TestImportScriptSkipsUnnecessaryRollback(t *testing.T) {
	tests := []struct {
		name         string
		meta         map[string]string
		env          map[string]string
		wantRollback bool
	}{
		{
			name: "incremental import: the image is known to be identical to the snapshot",
			meta: map[string]string{"mantle.clean-snap": snapName1},
			env: map[string]string{
				"FROM_SNAP_NAME": snapName1,
			},
			wantRollback: false,
		},
		{
			name: "incremental import: the state of the image is unknown",
			meta: map[string]string{},
			env: map[string]string{
				"FROM_SNAP_NAME": snapName1,
			},
			wantRollback: true,
		},
		{
			name: "incremental import: the image is identical to another snapshot",
			meta: map[string]string{"mantle.clean-snap": "snap0"},
			env: map[string]string{
				"FROM_SNAP_NAME": snapName1,
			},
			wantRollback: true,
		},
		{
			name: "incremental import: the size of the image differs from the snapshot",
			meta: map[string]string{"mantle.clean-snap": snapName1},
			env: map[string]string{
				"FROM_SNAP_NAME": snapName1,
				"STUB_HEAD_SIZE": "2147483648",
				"STUB_SNAP_SIZE": "1073741824",
			},
			wantRollback: true,
		},
		{
			// An older zeroout Job may have modified the image head before this
			// Job, so the recorded state is discarded on a full import and the
			// rollback is always executed.
			name: "full import: initialsnap exists and the image is recorded as identical to it",
			meta: map[string]string{"mantle.clean-snap": initialSnapName},
			env: map[string]string{
				"FROM_SNAP_NAME": "",
				"TO_SNAP_NAME":   fullImportToSnap,
				"STUB_SNAPS":     initialSnapName,
			},
			wantRollback: true,
		},
		{
			name: "full import: initialsnap exists and the state of the image is unknown",
			meta: map[string]string{},
			env: map[string]string{
				"FROM_SNAP_NAME": "",
				"TO_SNAP_NAME":   fullImportToSnap,
				"STUB_SNAPS":     initialSnapName,
			},
			wantRollback: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runImportScript(t, tt.meta, tt.env)

			if got := result.invoked("snap rollback"); got != tt.wantRollback {
				t.Errorf("rollback executed = %t, want %t; rbd invocations: %v",
					got, tt.wantRollback, result.rbdArgs)
			}
			if !tt.wantRollback && !strings.Contains(result.stdout, "skip rollback") {
				t.Errorf("the skip of the rollback is not logged; stdout:\n%s", result.stdout)
			}
			if !result.invoked("import-diff") {
				t.Errorf("import-diff was not executed; rbd invocations: %v", result.rbdArgs)
			}
			// rbd diff is impractical unless the fast-diff feature is enabled,
			// so the script must not depend on it.
			if result.invoked("diff ") {
				t.Errorf("rbd diff must not be used; rbd invocations: %v", result.rbdArgs)
			}
		})
	}
}

func TestImportScriptRecordsCleanSnapshot(t *testing.T) {
	// The image is dirty while the import is running, so the metadata must be
	// removed before the import and set to the imported snapshot after it.
	result := runImportScript(t,
		map[string]string{"mantle.clean-snap": snapName1},
		map[string]string{
			"FROM_SNAP_NAME": snapName1,
			"TO_SNAP_NAME":   snapName2,
		},
	)

	removeIndex := result.indexOf("image-meta remove pool/image mantle.clean-snap")
	importIndex := result.indexOf("import-diff")
	setIndex := result.indexOf("image-meta set pool/image mantle.clean-snap snap2")
	if removeIndex < 0 || importIndex < 0 || setIndex < 0 ||
		removeIndex > importIndex || importIndex > setIndex {
		t.Errorf("the clean snapshot metadata is not maintained around the import; rbd invocations: %v",
			result.rbdArgs)
	}

	if got := result.metadata(t)["mantle.clean-snap"]; got != snapName2 {
		t.Errorf("mantle.clean-snap = %q, want %q", got, snapName2)
	}
}

func TestImportScriptDoesNotRecordMissingSnapshot(t *testing.T) {
	// If the snapshot the import-diff is expected to create doesn't exist, the
	// state of the image head is unknown and thus must not be recorded.
	result := runImportScript(t,
		map[string]string{"mantle.clean-snap": snapName1},
		map[string]string{
			"FROM_SNAP_NAME":      snapName1,
			"TO_SNAP_NAME":        snapName2,
			"STUB_IMPORT_NO_SNAP": "true",
		},
	)

	if result.invoked("image-meta set") {
		t.Errorf("the clean snapshot metadata must not be set; rbd invocations: %v", result.rbdArgs)
	}
	if got, ok := result.metadata(t)["mantle.clean-snap"]; ok {
		t.Errorf("mantle.clean-snap = %q, want it to be absent", got)
	}
}

func TestImportScriptDoesNotRecordWithoutToSnapName(t *testing.T) {
	// TO_SNAP_NAME is empty if the Job was created by an older version of
	// mantle. In that case, the state of the image head is left unknown.
	result := runImportScript(t,
		map[string]string{"mantle.clean-snap": snapName1},
		map[string]string{
			"FROM_SNAP_NAME": snapName1,
			"TO_SNAP_NAME":   "",
		},
	)

	if result.invoked("image-meta set") {
		t.Errorf("the clean snapshot metadata must not be set; rbd invocations: %v", result.rbdArgs)
	}
	if got, ok := result.metadata(t)["mantle.clean-snap"]; ok {
		t.Errorf("mantle.clean-snap = %q, want it to be absent", got)
	}
}

func TestImportScriptRecordsCleanSnapshotAfterRollback(t *testing.T) {
	// The image head is identical to the snapshot just after a rollback, and
	// therefore no rollback is needed if the Job is retried before the import
	// starts.
	result := runImportScript(t,
		map[string]string{"mantle.clean-snap": snapName2},
		map[string]string{
			"FROM_SNAP_NAME": snapName1,
			"TO_SNAP_NAME":   snapName2,
		},
	)

	rollbackIndex := result.indexOf("snap rollback pool/image@snap1")
	removeIndex := result.indexOf("image-meta remove pool/image mantle.clean-snap")
	setIndex := result.indexOf("image-meta set pool/image mantle.clean-snap snap1")
	importIndex := result.indexOf("import-diff")
	if removeIndex < 0 || rollbackIndex < 0 || setIndex < 0 || importIndex < 0 ||
		removeIndex > rollbackIndex || rollbackIndex > setIndex || setIndex > importIndex {
		t.Errorf("the rolled back snapshot is not recorded as the clean one; rbd invocations: %v",
			result.rbdArgs)
	}
}

func TestImportScriptInvalidatesCleanSnapshotBeforeRollback(t *testing.T) {
	tests := []struct {
		name         string
		env          map[string]string
		wantRollback bool
		wantClean    string
	}{
		{"metadata read fails", map[string]string{"STUB_META_LIST_FAIL_AT": "2"}, false, snapName2},
		{"metadata removal fails", map[string]string{"STUB_META_REMOVE_FAIL": "true"}, false, snapName2},
		{"rollback fails", map[string]string{"STUB_ROLLBACK_FAIL": "true"}, true, ""},
		{"metadata update fails", map[string]string{"STUB_META_SET_FAIL": "true"}, true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := map[string]string{"FROM_SNAP_NAME": snapName1}
			maps.Copy(env, tt.env)
			result := runImportScriptWithError(t,
				map[string]string{"mantle.clean-snap": snapName2}, env)
			if result.err == nil {
				t.Fatal("expected the script to fail")
			}
			if got := result.invoked("snap rollback"); got != tt.wantRollback {
				t.Errorf("rollback executed = %t, want %t; invocations: %v", got, tt.wantRollback, result.rbdArgs)
			}
			if result.invoked("import-diff") {
				t.Errorf("import-diff must not run after an error; invocations: %v", result.rbdArgs)
			}
			if got := result.metadata(t)["mantle.clean-snap"]; got != tt.wantClean {
				t.Errorf("mantle.clean-snap = %q, want %q", got, tt.wantClean)
			}
		})
	}
}

func TestImportScriptDoesNotRecordInitialSnapshotAsClean(t *testing.T) {
	// A full import discards the recorded state before it uses it, so
	// recording initialsnap would be pointless: the retry of this Job would
	// discard it again before the rollback.
	result := runImportScript(t,
		map[string]string{},
		map[string]string{
			"FROM_SNAP_NAME": "",
			"TO_SNAP_NAME":   fullImportToSnap,
			"STUB_SNAPS":     "",
		},
	)

	createIndex := result.indexOf("snap create pool/image@initialsnap")
	if createIndex < 0 {
		t.Errorf("initialsnap is not created; rbd invocations: %v", result.rbdArgs)
	}
	if result.invoked("image-meta set pool/image mantle.clean-snap initialsnap") {
		t.Errorf("initialsnap must not be recorded as the clean snapshot; rbd invocations: %v",
			result.rbdArgs)
	}
	if result.invoked("snap rollback") {
		t.Errorf("rollback must not be executed; rbd invocations: %v", result.rbdArgs)
	}
}

func TestImportScriptDiscardsCleanSnapshotOnFullImport(t *testing.T) {
	// Older zeroout Jobs did not invalidate the metadata, so a full import
	// must still discard the recorded state before it is used.
	result := runImportScript(t,
		map[string]string{"mantle.clean-snap": initialSnapName},
		map[string]string{
			"FROM_SNAP_NAME": "",
			"TO_SNAP_NAME":   fullImportToSnap,
			"STUB_SNAPS":     initialSnapName,
		},
	)

	removeIndex := result.indexOf("image-meta remove pool/image mantle.clean-snap")
	rollbackIndex := result.indexOf("snap rollback pool/image@initialsnap")
	if removeIndex < 0 || rollbackIndex < 0 || removeIndex > rollbackIndex {
		t.Errorf("the clean snapshot metadata is not discarded before the rollback; rbd invocations: %v",
			result.rbdArgs)
	}
}

func TestImportScriptSkipsAppliedImport(t *testing.T) {
	// rbd import-diff creates TO_SNAP_NAME only after it has applied the whole
	// diff, so the existence of the snapshot means the import has completed.
	// Repeating it is not only wasteful but impossible: import-diff aborts
	// with EEXIST if the snapshot it has to create already exists.
	tests := []struct {
		name string
		meta map[string]string
		env  map[string]string
	}{
		{
			name: "incremental import",
			meta: map[string]string{},
			env: map[string]string{
				"FROM_SNAP_NAME": snapName1,
				"TO_SNAP_NAME":   snapName2,
				"STUB_SNAPS":     snapName1 + " " + snapName2,
			},
		},
		{
			name: "incremental import with the recorded state",
			meta: map[string]string{"mantle.clean-snap": snapName2},
			env: map[string]string{
				"FROM_SNAP_NAME": snapName1,
				"TO_SNAP_NAME":   snapName2,
				"STUB_SNAPS":     snapName1 + " " + snapName2,
			},
		},
		{
			name: "full import",
			meta: map[string]string{},
			env: map[string]string{
				"FROM_SNAP_NAME": "",
				"TO_SNAP_NAME":   fullImportToSnap,
				"STUB_SNAPS":     initialSnapName + " " + fullImportToSnap,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := maps.Clone(tt.meta)
			result := runImportScript(t, tt.meta, tt.env)

			for _, command := range []string{"snap rollback", "import-diff", "snap create"} {
				if result.invoked(command) {
					t.Errorf("%s must not run for an applied import; invocations: %v",
						command, result.rbdArgs)
				}
			}
			// The existence of the snapshot tells nothing about the current
			// contents of the image head, so the recorded state must be
			// neither set nor discarded.
			for _, command := range []string{"image-meta set", "image-meta remove"} {
				if result.invoked(command) {
					t.Errorf("%s must not run for an applied import; invocations: %v",
						command, result.rbdArgs)
				}
			}
			if got := result.metadata(t); !maps.Equal(got, want) {
				t.Errorf("metadata = %v, want %v", got, want)
			}
		})
	}
}

func TestImportScriptRemovesInitialSnapAfterSkippedImport(t *testing.T) {
	// The full import is complete only after initialsnap is removed, so the
	// skip path must still remove it.
	result := runImportScript(t,
		map[string]string{},
		map[string]string{
			"FROM_SNAP_NAME": "",
			"TO_SNAP_NAME":   fullImportToSnap,
			"STUB_SNAPS":     initialSnapName + " " + fullImportToSnap,
		},
	)

	if !result.invoked("snap rm pool/image@initialsnap") {
		t.Errorf("initialsnap is not removed; rbd invocations: %v", result.rbdArgs)
	}
	if got := result.snapshots(t); len(got) != 1 || got[0] != fullImportToSnap {
		t.Errorf("snapshots = %v, want [%s]", got, fullImportToSnap)
	}
}

func TestImportScriptRetriesAfterPostImportFailure(t *testing.T) {
	// A failure between the end of import-diff and the end of the Job, such as
	// a transient failure of recording the clean snapshot, must not make the
	// Job unable to succeed: the retry has to detect the applied import and
	// finish without repeating it.
	result := runRBDJobScripts(t,
		map[string]string{"mantle.clean-snap": snapName1},
		map[string]string{
			"FROM_SNAP_NAME":     snapName1,
			"TO_SNAP_NAME":       snapName2,
			"STUB_META_SET_FAIL": "true",
		},
		EmbedJobImportScript,
		EmbedJobImportScript,
	)

	if result.errs[0] == nil {
		t.Fatalf("the first run must fail; invocations: %v", result.rbdArgs)
	}
	if result.errs[1] != nil {
		t.Fatalf("the retry must succeed: %v\n%s", result.errs[1], result.stdout)
	}
	if got := result.count("import-diff"); got != 1 {
		t.Errorf("import-diff ran %d times, want 1; invocations: %v", got, result.rbdArgs)
	}
	if result.invoked("snap rollback") {
		t.Errorf("the retry must not roll back the applied import; invocations: %v", result.rbdArgs)
	}
	// The first run removed the metadata before the import and failed to set
	// it again, so the state of the image head stays unknown.
	if got, ok := result.metadata(t)["mantle.clean-snap"]; ok {
		t.Errorf("mantle.clean-snap = %q, want it to be absent", got)
	}
}

func TestImportScriptRetriesAfterInitialSnapRemovalFailure(t *testing.T) {
	// The full import removes initialsnap after the import, which is another
	// step that can fail once the import has been applied.
	result := runRBDJobScripts(t,
		map[string]string{},
		map[string]string{
			"FROM_SNAP_NAME":       "",
			"TO_SNAP_NAME":         fullImportToSnap,
			"STUB_SNAPS":           "",
			"STUB_SNAP_RM_FAIL_AT": "1",
		},
		EmbedJobImportScript,
		EmbedJobImportScript,
	)

	if result.errs[0] == nil {
		t.Fatalf("the first run must fail; invocations: %v", result.rbdArgs)
	}
	if result.errs[1] != nil {
		t.Fatalf("the retry must succeed: %v\n%s", result.errs[1], result.stdout)
	}
	if got := result.count("import-diff"); got != 1 {
		t.Errorf("import-diff ran %d times, want 1; invocations: %v", got, result.rbdArgs)
	}
	if got := result.snapshots(t); len(got) != 1 || got[0] != fullImportToSnap {
		t.Errorf("snapshots = %v, want [%s]", got, fullImportToSnap)
	}
}

func TestImportScriptIsIdempotent(t *testing.T) {
	// A Job can be restarted after its script has completed but before the Job
	// itself was recorded as complete, so running the script again must be a
	// no-op that succeeds.
	tests := []struct {
		name string
		env  map[string]string
	}{
		{
			name: "incremental import",
			env: map[string]string{
				"FROM_SNAP_NAME": snapName1,
				"TO_SNAP_NAME":   snapName2,
			},
		},
		{
			name: "full import",
			env: map[string]string{
				"FROM_SNAP_NAME": "",
				"TO_SNAP_NAME":   fullImportToSnap,
				"STUB_SNAPS":     "",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := runRBDJobScripts(t,
				map[string]string{"mantle.clean-snap": snapName1},
				tt.env,
				EmbedJobImportScript,
				EmbedJobImportScript,
			)

			for i, err := range result.errs {
				if err != nil {
					t.Fatalf("run %d failed: %v\n%s", i, err, result.stdout)
				}
			}
			if got := result.count("import-diff"); got != 1 {
				t.Errorf("import-diff ran %d times, want 1; invocations: %v", got, result.rbdArgs)
			}
			if got := result.count("snap create"); got > 1 {
				t.Errorf("snap create ran %d times, want at most 1; invocations: %v", got, result.rbdArgs)
			}
			if got := result.count("snap rm"); got > 1 {
				t.Errorf("snap rm ran %d times, want at most 1; invocations: %v", got, result.rbdArgs)
			}
		})
	}
}

func TestImportScriptChainsMultipleParts(t *testing.T) {
	// The import of a part creates the snapshot the next part starts from, so
	// a part that was skipped must still leave the image in a state the next
	// part can roll back to.
	const middleSnap = "uid-offset-100"

	result := runScriptRuns(t,
		map[string]string{},
		// The first part has already been applied, so its snapshot is there
		// before the runs below.
		map[string]string{"STUB_SNAPS": snapName1 + " " + middleSnap},
		[]scriptRun{
			{
				script: EmbedJobImportScript,
				env: map[string]string{
					"FROM_SNAP_NAME": snapName1,
					"TO_SNAP_NAME":   middleSnap,
				},
			},
			// The second part starts from the snapshot the first part created.
			{
				script: EmbedJobImportScript,
				env: map[string]string{
					"FROM_SNAP_NAME": middleSnap,
					"TO_SNAP_NAME":   snapName2,
				},
			},
		},
	)

	for i, err := range result.errs {
		if err != nil {
			t.Fatalf("run %d failed: %v\n%s", i, err, result.stdout)
		}
	}
	if got := result.count("import-diff"); got != 1 {
		t.Errorf("import-diff ran %d times, want 1; invocations: %v", got, result.rbdArgs)
	}
	// The skipped part left the image head in an unknown state, so the next
	// part must roll it back before it applies its diff.
	rollbackIndex := result.indexOf("snap rollback pool/image@" + middleSnap)
	importIndex := result.indexOf("import-diff")
	if rollbackIndex < 0 || importIndex < 0 || rollbackIndex > importIndex {
		t.Errorf("the next part must roll back before it imports; invocations: %v", result.rbdArgs)
	}
	if got := result.metadata(t)["mantle.clean-snap"]; got != snapName2 {
		t.Errorf("mantle.clean-snap = %q, want %q", got, snapName2)
	}
}
