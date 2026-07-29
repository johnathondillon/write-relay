package delivery

import (
	"bytes"
	"testing"
)

func TestStdoutSinkWritesOneEventPerLine(t *testing.T) {
	var output bytes.Buffer
	sink, registration := NewStdoutSink("development", &output)
	result := sink.Send(t.Context(), Delivery{Payload: []byte(`{"id":"one"}`)})
	if !result.Success {
		t.Fatalf("result = %#v", result)
	}
	if output.String() != "{\"id\":\"one\"}\n" {
		t.Fatalf("output = %q", output.String())
	}
	if registration.Name != "development" || registration.Type != "stdout" {
		t.Fatalf("registration = %#v", registration)
	}
}
