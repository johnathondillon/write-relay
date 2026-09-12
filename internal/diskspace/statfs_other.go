//go:build !linux && !darwin

package diskspace

import "errors"

func AvailableBytes(string) (uint64, error) {
	return 0, errors.New("disk-space protection requires Linux or macOS")
}
