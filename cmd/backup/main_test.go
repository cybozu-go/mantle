package backup

import (
	"context"
	"testing"

	mantlev1 "github.com/cybozu-go/mantle/api/v1"
	"github.com/cybozu-go/mantle/internal/controller"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestCreateMantleBackup(t *testing.T) {
	t.Setenv("JOB_NAME", "some-job-12345678")

	testCases := map[string]struct {
		mbcLabels           map[string]string
		transferCompression string
		wantLabelValue      string
		wantLabelSet        bool
	}{
		"MBC has the priority label": {
			mbcLabels:      map[string]string{controller.LabelBackupPriority: "high"},
			wantLabelValue: "high",
			wantLabelSet:   true,
		},
		"MBC has transfer compression in spec": {
			transferCompression: "zstd",
		},
		"MBC does not have the priority label": {
			mbcLabels:    map[string]string{},
			wantLabelSet: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			cli := fake.NewClientBuilder().WithScheme(scheme).Build()

			mbc := &mantlev1.MantleBackupConfig{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-mbc",
					Namespace: "test-ns",
					Labels:    tc.mbcLabels,
				},
				Spec: mantlev1.MantleBackupConfigSpec{
					PVC:                 "test-pvc",
					Expire:              "1d",
					TransferCompression: tc.transferCompression,
				},
			}

			mbcNamespace = "test-ns"
			err := createMantleBackup(context.Background(), cli, mbc)
			require.NoError(t, err)

			var mbList mantlev1.MantleBackupList
			err = cli.List(context.Background(), &mbList, client.InNamespace("test-ns"))
			require.NoError(t, err)
			require.Len(t, mbList.Items, 1)
			mb := mbList.Items[0]

			value, ok := mb.GetLabels()[controller.LabelBackupPriority]
			require.Equal(t, tc.wantLabelSet, ok)
			if ok {
				require.Equal(t, tc.wantLabelValue, value)
			}

			require.Equal(t, tc.transferCompression, mb.Spec.TransferCompression)
		})
	}
}
