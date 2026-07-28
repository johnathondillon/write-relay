package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

var (
	ErrMalformed = errors.New("malformed event")
	ErrOversized = errors.New("event exceeds configured size limit")
)

type Metadata struct {
	SpecVersion     string
	ID              string
	Source          string
	Type            string
	Subject         string
	Time            *time.Time
	DataContentType string
}

func Validate(payload []byte, maxBytes int) (Metadata, error) {
	if maxBytes < 1 {
		return Metadata{}, fmt.Errorf("%w: invalid maximum size %d", ErrOversized, maxBytes)
	}
	if len(payload) > maxBytes {
		return Metadata{}, fmt.Errorf("%w: received %d bytes, limit is %d", ErrOversized, len(payload), maxBytes)
	}

	var object map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(payload))
	if err := decoder.Decode(&object); err != nil {
		return Metadata{}, fmt.Errorf("%w: invalid JSON: %v", ErrMalformed, err)
	}
	if object == nil {
		return Metadata{}, fmt.Errorf("%w: root value must be an object", ErrMalformed)
	}
	if err := ensureEOF(decoder); err != nil {
		return Metadata{}, fmt.Errorf("%w: %v", ErrMalformed, err)
	}

	var metadata Metadata
	var err error
	if metadata.SpecVersion, err = requiredString(object, "specversion"); err != nil {
		return Metadata{}, err
	}
	if metadata.SpecVersion != "1.0" {
		return Metadata{}, fmt.Errorf("%w: specversion must be %q", ErrMalformed, "1.0")
	}
	if metadata.ID, err = requiredString(object, "id"); err != nil {
		return Metadata{}, err
	}
	if metadata.Source, err = requiredString(object, "source"); err != nil {
		return Metadata{}, err
	}
	if metadata.Type, err = requiredString(object, "type"); err != nil {
		return Metadata{}, err
	}
	if metadata.Subject, err = optionalString(object, "subject", true); err != nil {
		return Metadata{}, err
	}
	if metadata.DataContentType, err = optionalString(object, "datacontenttype", false); err != nil {
		return Metadata{}, err
	}
	if raw, ok := object["time"]; ok {
		value, err := decodeString(raw, "time")
		if err != nil || value == "" {
			return Metadata{}, fmt.Errorf("%w: time must be an RFC 3339 string", ErrMalformed)
		}
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return Metadata{}, fmt.Errorf("%w: time must be RFC 3339: %v", ErrMalformed, err)
		}
		metadata.Time = &parsed
	}
	return metadata, nil
}

func ensureEOF(decoder *json.Decoder) error {
	var extra json.RawMessage
	err := decoder.Decode(&extra)
	if err == io.EOF {
		return nil
	}
	if err == nil {
		return errors.New("multiple JSON values are not allowed")
	}
	return fmt.Errorf("invalid trailing content: %v", err)
}

func requiredString(object map[string]json.RawMessage, name string) (string, error) {
	raw, ok := object[name]
	if !ok {
		return "", fmt.Errorf("%w: %s is required", ErrMalformed, name)
	}
	value, err := decodeString(raw, name)
	if err != nil {
		return "", err
	}
	if value == "" {
		return "", fmt.Errorf("%w: %s must be a non-empty string", ErrMalformed, name)
	}
	return value, nil
}

func optionalString(object map[string]json.RawMessage, name string, allowEmpty bool) (string, error) {
	raw, ok := object[name]
	if !ok {
		return "", nil
	}
	value, err := decodeString(raw, name)
	if err != nil {
		return "", err
	}
	if !allowEmpty && value == "" {
		return "", fmt.Errorf("%w: %s must be a non-empty string", ErrMalformed, name)
	}
	return value, nil
}

func decodeString(raw json.RawMessage, name string) (string, error) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("%w: %s must be a string", ErrMalformed, name)
	}
	return value, nil
}
