package delivery

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/johnathondillon/write-relay/internal/config"
	"github.com/johnathondillon/write-relay/internal/failure"
)

type WebhookSender struct {
	endpoint      string
	authorization string
	signingSecret []byte
	timeout       time.Duration
	client        *http.Client
	hooks         failure.Hooks
}

func NewWebhookSender(sink config.SinkConfig, timeout time.Duration) (*WebhookSender, SinkRegistration, error) {
	return NewWebhookSenderWithHooks(sink, timeout, failure.Hooks{})
}

// NewWebhookSenderWithHooks exists for deterministic process-crash tests.
// Production composition calls NewWebhookSender with inert hooks.
func NewWebhookSenderWithHooks(
	sink config.SinkConfig,
	timeout time.Duration,
	hooks failure.Hooks,
) (*WebhookSender, SinkRegistration, error) {
	authorization, err := optionalSecret(sink.AuthorizationEnv)
	if err != nil {
		return nil, SinkRegistration{}, err
	}
	if strings.ContainsAny(authorization, "\r\n") {
		return nil, SinkRegistration{}, fmt.Errorf(
			"environment variable %s contains an invalid authorization header value",
			sink.AuthorizationEnv,
		)
	}
	signingSecret, err := optionalSecret(sink.SigningSecretEnv)
	if err != nil {
		return nil, SinkRegistration{}, err
	}
	fingerprint := sha256.Sum256([]byte(sink.Type + "\x00" + sink.URL))
	sender := &WebhookSender{
		endpoint:      sink.URL,
		authorization: authorization,
		signingSecret: []byte(signingSecret),
		timeout:       timeout,
		hooks:         hooks,
		client: &http.Client{
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
	return sender, SinkRegistration{
		Name: sink.Name, Type: sink.Type, ConfigSHA256: fingerprint,
	}, nil
}

func optionalSecret(name string) (string, error) {
	if name == "" {
		return "", nil
	}
	value, ok := os.LookupEnv(name)
	if !ok || value == "" {
		return "", fmt.Errorf("environment variable %s is not set", name)
	}
	return value, nil
}

func (s *WebhookSender) Send(ctx context.Context, item Delivery) AttemptResult {
	requestCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(
		requestCtx, http.MethodPost, s.endpoint, bytes.NewReader(item.Payload),
	)
	if err != nil {
		return AttemptResult{Failure: "construct webhook request", Retryable: false}
	}
	attempt := item.Attempts + 1
	request.Header.Set("Content-Type", "application/cloudevents+json")
	request.Header.Set("User-Agent", "WriteRelay/1")
	request.Header.Set("Idempotency-Key", deliveryKey(item))
	request.Header.Set("X-WriteRelay-Attempt", strconv.Itoa(attempt))
	if s.authorization != "" {
		request.Header.Set("Authorization", s.authorization)
	}
	if len(s.signingSecret) != 0 {
		timestamp := strconv.FormatInt(time.Now().UTC().Unix(), 10)
		mac := hmac.New(sha256.New, s.signingSecret)
		_, _ = io.WriteString(mac, timestamp)
		_, _ = io.WriteString(mac, ".")
		_, _ = mac.Write(item.Payload)
		request.Header.Set("X-WriteRelay-Timestamp", timestamp)
		request.Header.Set("X-WriteRelay-Signature", "v1="+hex.EncodeToString(mac.Sum(nil)))
	}

	s.hooks.CallBeforeSinkRequest()
	response, err := s.client.Do(request)
	if err != nil {
		if requestCtx.Err() != nil {
			return AttemptResult{Retryable: true, Failure: "webhook request timed out"}
		}
		return AttemptResult{Retryable: true, Failure: "webhook network request failed"}
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 32*1024))

	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return AttemptResult{Success: true, StatusCode: response.StatusCode}
	}
	retryable := response.StatusCode == http.StatusRequestTimeout ||
		response.StatusCode == http.StatusTooEarly ||
		response.StatusCode == http.StatusTooManyRequests ||
		response.StatusCode >= 500
	return AttemptResult{
		Retryable: retryable, RetryAfter: parseRetryAfter(response.Header.Get("Retry-After")),
		StatusCode: response.StatusCode,
		Failure:    fmt.Sprintf("webhook returned HTTP %d", response.StatusCode),
	}
}

func parseRetryAfter(value string) time.Duration {
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds > 0 {
			if seconds > int64((1<<63-1)/time.Second) {
				return time.Duration(1<<63 - 1)
			}
			return time.Duration(seconds) * time.Second
		}
		return 0
	}
	at, err := http.ParseTime(value)
	if err != nil {
		return 0
	}
	delay := time.Until(at)
	if delay < 0 {
		return 0
	}
	return delay
}

func deliveryKey(item Delivery) string {
	digest := sha256.Sum256([]byte(
		item.SinkName + "\x00" + item.Source + "\x00" + item.ID,
	))
	return hex.EncodeToString(digest[:])
}
