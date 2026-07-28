package postgres

import (
	"errors"
	"testing"
)

func TestValidateServerVersion(t *testing.T) {
	for _, version := range []int{140000, 150012, 160001, 170005, 180000} {
		if err := validateServerVersion(version); err != nil {
			t.Fatalf("version %d rejected: %v", version, err)
		}
	}
	for _, version := range []int{130020, 190000} {
		if err := validateServerVersion(version); !errors.Is(err, ErrUnsupportedVersion) {
			t.Fatalf("version %d error = %v", version, err)
		}
	}
}
