package controller

import (
	"testing"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMakeImportJobToSnapName(t *testing.T) {
	backup := &mantlev1.MantleBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "backup1",
			Annotations: map[string]string{annotRemoteUID: "remote-uid"},
		},
	}
	const transferPartSize = int64(100)

	tests := []struct {
		name     string
		partNum  int
		numParts int
		want     string
	}{
		{
			name:     "the only part creates the snapshot of the backup",
			partNum:  0,
			numParts: 1,
			want:     "backup1",
		},
		{
			name:     "a part other than the last one creates the middle snapshot",
			partNum:  0,
			numParts: 3,
			want:     MakeMiddleSnapshotName(backup, 100),
		},
		{
			name:     "the middle snapshot is the source of the next part",
			partNum:  1,
			numParts: 3,
			want:     MakeMiddleSnapshotName(backup, 200),
		},
		{
			name:     "the last part creates the snapshot of the backup",
			partNum:  2,
			numParts: 3,
			want:     "backup1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MakeImportJobToSnapName(backup, tt.partNum, tt.numParts, transferPartSize)
			if got != tt.want {
				t.Errorf("MakeImportJobToSnapName(_, %d, %d, %d) = %q, want %q",
					tt.partNum, tt.numParts, transferPartSize, got, tt.want)
			}
		})
	}
}
