package natasks

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	jsErrCodeJetStreamNotAvailable jetstream.ErrorCode = 10008
	jsErrCodeStreamOffline         jetstream.ErrorCode = 10118

	managementRetryAttempts = 8
	managementRetryDelay    = 250 * time.Millisecond
	managementRetryMaxDelay = 2 * time.Second
)

func retryJetStreamManagement(fn func() error) error {
	delay := managementRetryDelay

	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil || !isTemporaryJetStreamError(err) || attempt >= managementRetryAttempts {
			return err
		}

		time.Sleep(delay)
		delay *= 2
		if delay > managementRetryMaxDelay {
			delay = managementRetryMaxDelay
		}
	}
}

func isTemporaryJetStreamError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, jetstream.ErrNoStreamResponse) ||
		errors.Is(err, jetstream.ErrConsumerLeadershipChanged) {
		return true
	}

	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode == jsErrCodeStreamOffline ||
			apiErr.ErrorCode == jsErrCodeJetStreamNotAvailable
	}

	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "raft: not leader") ||
		strings.Contains(msg, "leadership change") ||
		strings.Contains(msg, "leadership-change")
}
