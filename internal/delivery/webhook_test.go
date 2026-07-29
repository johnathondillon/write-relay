package delivery

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johnathondillon/write-relay/internal/config"
)

func TestWebhookSendsRawEventWithStableIdentityAndSecrets(t *testing.T) {
	t.Setenv("TEST_WEBHOOK_AUTH", "Bearer test-token")
	t.Setenv("TEST_WEBHOOK_SECRET", "signing-secret")
	payload := []byte(`{"specversion":"1.0","id":"one","source":"urn:test","type":"created"}`)
	var firstKey string
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls++
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
		}
		if string(body) != string(payload) {
			t.Errorf("body = %q", body)
		}
		if request.Header.Get("Content-Type") != "application/cloudevents+json" {
			t.Errorf("content type = %q", request.Header.Get("Content-Type"))
		}
		if request.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("authorization = %q", request.Header.Get("Authorization"))
		}
		if request.Header.Get("X-WriteRelay-Signature") == "" ||
			request.Header.Get("X-WriteRelay-Timestamp") == "" {
			t.Error("missing webhook signature headers")
		}
		mac := hmac.New(sha256.New, []byte("signing-secret"))
		_, _ = io.WriteString(mac, request.Header.Get("X-WriteRelay-Timestamp"))
		_, _ = io.WriteString(mac, ".")
		_, _ = mac.Write(payload)
		wantSignature := "v1=" + hex.EncodeToString(mac.Sum(nil))
		if request.Header.Get("X-WriteRelay-Signature") != wantSignature {
			t.Errorf("signature = %q, want %q",
				request.Header.Get("X-WriteRelay-Signature"), wantSignature)
		}
		key := request.Header.Get("Idempotency-Key")
		if firstKey == "" {
			firstKey = key
		} else if key != firstKey {
			t.Errorf("idempotency key changed: %q != %q", key, firstKey)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	sender, registration, err := NewWebhookSender(config.SinkConfig{
		Name:             "orders",
		Type:             "webhook",
		URL:              server.URL,
		AuthorizationEnv: "TEST_WEBHOOK_AUTH",
		SigningSecretEnv: "TEST_WEBHOOK_SECRET",
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if registration.Name != "orders" || registration.ConfigSHA256 == [32]byte{} {
		t.Fatalf("unexpected registration: %#v", registration)
	}
	item := Delivery{
		SinkName: "orders", Source: "urn:test", ID: "one", Payload: payload,
	}
	for attempt := 0; attempt < 2; attempt++ {
		item.Attempts = attempt
		result := sender.Send(t.Context(), item)
		if !result.Success || result.StatusCode != http.StatusNoContent {
			t.Fatalf("attempt %d: %#v", attempt+1, result)
		}
	}
	if calls != 2 || firstKey == "" {
		t.Fatalf("calls=%d key=%q", calls, firstKey)
	}
}

func TestWebhookClassifiesResponsesAndDoesNotFollowRedirects(t *testing.T) {
	var redirected atomic.Int32
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirected.Add(1)
	}))
	defer redirectTarget.Close()

	tests := []struct {
		name      string
		status    int
		retryable bool
	}{
		{name: "bad_request", status: http.StatusBadRequest, retryable: false},
		{name: "timeout", status: http.StatusRequestTimeout, retryable: true},
		{name: "too_many_requests", status: http.StatusTooManyRequests, retryable: true},
		{name: "server_error", status: http.StatusServiceUnavailable, retryable: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
			}))
			defer server.Close()
			sender, _, err := NewWebhookSender(config.SinkConfig{
				Name: "test", Type: "webhook", URL: server.URL,
			}, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			result := sender.Send(t.Context(), Delivery{SinkName: "test", Payload: []byte(`{}`)})
			if result.Success || result.Retryable != test.retryable || result.StatusCode != test.status {
				t.Fatalf("result = %#v", result)
			}
		})
	}

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		http.Redirect(w, request, redirectTarget.URL, http.StatusFound)
	}))
	defer redirector.Close()
	sender, _, err := NewWebhookSender(config.SinkConfig{
		Name: "test", Type: "webhook", URL: redirector.URL,
	}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result := sender.Send(t.Context(), Delivery{SinkName: "test", Payload: []byte(`{}`)})
	if result.Success || result.Retryable || result.StatusCode != http.StatusFound {
		t.Fatalf("redirect result = %#v", result)
	}
	if redirected.Load() != 0 {
		t.Fatal("webhook redirect was followed")
	}
}

func TestWebhookTimeoutIsRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	sender, _, err := NewWebhookSender(config.SinkConfig{
		Name: "test", Type: "webhook", URL: server.URL,
	}, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	result := sender.Send(t.Context(), Delivery{SinkName: "test", Payload: []byte(`{}`)})
	if result.Success || !result.Retryable || result.Failure != "webhook request timed out" {
		t.Fatalf("result = %#v", result)
	}
}

func TestParseRetryAfter(t *testing.T) {
	if got := parseRetryAfter("12"); got != 12*time.Second {
		t.Fatalf("delta retry-after = %s", got)
	}
	future := time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat)
	got := parseRetryAfter(future)
	if got < 28*time.Second || got > 31*time.Second {
		t.Fatalf("date retry-after = %s", got)
	}
	if got := parseRetryAfter("invalid"); got != 0 {
		t.Fatalf("invalid retry-after = %s", got)
	}
}

func TestWebhookRejectsAuthorizationHeaderInjection(t *testing.T) {
	t.Setenv("TEST_BAD_AUTH", "Bearer safe\r\nX-Injected: true")
	_, _, err := NewWebhookSender(config.SinkConfig{
		Name: "test", Type: "webhook", URL: "https://example.test",
		AuthorizationEnv: "TEST_BAD_AUTH",
	}, time.Second)
	if err == nil {
		t.Fatal("expected invalid authorization header error")
	}
}
