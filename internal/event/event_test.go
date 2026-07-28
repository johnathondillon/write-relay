package event

import (
	"errors"
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantErr error
	}{
		{
			name:    "minimal",
			payload: `{"specversion":"1.0","id":"evt-1","source":"urn:test","type":"order.paid"}`,
		},
		{
			name:    "extensions and timestamp",
			payload: `{"specversion":"1.0","id":"evt-1","source":"urn:test","type":"order.paid","time":"2026-07-27T20:24:00Z","datacontenttype":"application/json","extension":42}`,
		},
		{name: "array root", payload: `[]`, wantErr: ErrMalformed},
		{name: "wrong version", payload: `{"specversion":"0.3","id":"x","source":"s","type":"t"}`, wantErr: ErrMalformed},
		{name: "missing id", payload: `{"specversion":"1.0","source":"s","type":"t"}`, wantErr: ErrMalformed},
		{name: "non-string source", payload: `{"specversion":"1.0","id":"x","source":2,"type":"t"}`, wantErr: ErrMalformed},
		{name: "empty type", payload: `{"specversion":"1.0","id":"x","source":"s","type":""}`, wantErr: ErrMalformed},
		{name: "bad time", payload: `{"specversion":"1.0","id":"x","source":"s","type":"t","time":"yesterday"}`, wantErr: ErrMalformed},
		{name: "empty content type", payload: `{"specversion":"1.0","id":"x","source":"s","type":"t","datacontenttype":""}`, wantErr: ErrMalformed},
		{name: "trailing value", payload: `{"specversion":"1.0","id":"x","source":"s","type":"t"} {}`, wantErr: ErrMalformed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata, err := Validate([]byte(tt.payload), 262144)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Validate() error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && metadata.ID != "evt-1" {
				t.Fatalf("unexpected metadata: %#v", metadata)
			}
		})
	}
}

func TestValidateOversizedUsesRawByteLength(t *testing.T) {
	payload := `{"specversion":"1.0","id":"x","source":"s","type":"t","data":"` + strings.Repeat("é", 10) + `"}`
	_, err := Validate([]byte(payload), len(payload)-1)
	if !errors.Is(err, ErrOversized) {
		t.Fatalf("expected oversized error, got %v", err)
	}
}
