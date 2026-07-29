package delivery

import (
	"context"
	"crypto/sha256"
	"io"
	"sync"
)

type StdoutSink struct {
	writer io.Writer
	mu     sync.Mutex
}

func NewStdoutSink(name string, writer io.Writer) (*StdoutSink, SinkRegistration) {
	return &StdoutSink{writer: writer}, SinkRegistration{
		Name: name, Type: "stdout",
		ConfigSHA256: sha256.Sum256([]byte("stdout")),
	}
}

func (s *StdoutSink) Send(_ context.Context, item Delivery) AttemptResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.writer.Write(append(append([]byte(nil), item.Payload...), '\n')); err != nil {
		return AttemptResult{Retryable: true, Failure: "write stdout delivery failed"}
	}
	return AttemptResult{Success: true}
}
