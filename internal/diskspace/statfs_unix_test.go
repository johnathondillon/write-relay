//go:build linux || darwin

package diskspace

import (
	"math"
	"path/filepath"
	"testing"
)

func TestAvailableBytes(t *testing.T) {
	if _, err := AvailableBytes(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if _, err := AvailableBytes(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("missing path must fail")
	}
	for _, test := range []struct {
		blocks  uint64
		size    int64
		want    uint64
		invalid bool
	}{
		{2, 4096, 8192, false}, {0, 4096, 0, false}, {1, 0, 0, true}, {1, -1, 0, true}, {math.MaxUint64, 2, 0, true},
	} {
		got, err := availableBytes(test.blocks, test.size)
		if got != test.want || (err != nil) != test.invalid {
			t.Fatal(test, got, err)
		}
	}
}
