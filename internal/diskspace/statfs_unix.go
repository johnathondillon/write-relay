//go:build linux || darwin

package diskspace

import (
	"errors"
	"math"

	"golang.org/x/sys/unix"
)

// AvailableBytes uses unprivileged available blocks, not total free blocks.
// The existing spool file selects its filesystem, including bind mounts.
func AvailableBytes(path string) (uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return 0, err
	}
	return availableBytes(stat.Bavail, int64(stat.Bsize))
}

func availableBytes(blocks uint64, blockSize int64) (uint64, error) {
	if blockSize <= 0 || blocks > math.MaxUint64/uint64(blockSize) {
		return 0, errors.New("invalid filesystem space measurement")
	}
	return blocks * uint64(blockSize), nil
}
