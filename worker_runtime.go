package natasks

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func (w *Worker) startProgressLoop(msg jetstream.Msg) func() {
	done := make(chan struct{})

	go func() {
		ticker := time.NewTicker(w.cfg.progressInterval)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if err := msg.InProgress(); err != nil {
					w.logger.Warn("natasks: mark message in progress", "error", err)
					return
				}
			}
		}
	}()

	return func() {
		close(done)
	}
}

func (w *Worker) consumerForRun() (jetstream.Consumer, error) {
	if w.consumer != nil {
		return w.consumer, nil
	}

	return w.refreshConsumerForRun()
}

func (w *Worker) refreshConsumerForRun() (jetstream.Consumer, error) {
	consumer, err := w.js.Consumer(context.Background(), w.cfg.streamName, w.consumerName())
	if err != nil {
		ctx, cancel := managementContext()
		defer cancel()
		consumer, err = w.js.Consumer(ctx, w.cfg.streamName, w.consumerName())
	}
	if err != nil {
		return nil, err
	}

	w.consumer = consumer
	return consumer, nil
}

func (w *Worker) recoverConsumerForRun() (jetstream.Consumer, error) {
	if err := retryJetStreamManagement(func() error {
		return ensureStream(w.js, w.cfg.config)
	}); err != nil {
		return nil, err
	}

	var consumer jetstream.Consumer
	if err := retryJetStreamManagement(func() error {
		var err error
		consumer, err = w.ensureConsumer()
		return err
	}); err != nil {
		return nil, err
	}

	w.consumer = consumer
	return consumer, nil
}

func (w *Worker) fetchBatch(ctx context.Context, consumer jetstream.Consumer) ([]jetstream.Msg, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, w.cfg.fetchTimeout)
	defer cancel()

	batch, err := consumer.Fetch(w.fetchSize(), jetstream.FetchContext(fetchCtx))
	if err != nil {
		return nil, err
	}

	var msgs []jetstream.Msg
	for msg := range batch.Messages() {
		msgs = append(msgs, msg)
	}

	return msgs, batch.Error()
}

func (w *Worker) fetchSize() int {
	if w.cfg.concurrency > w.cfg.fetchBatch {
		return w.cfg.concurrency
	}

	return w.cfg.fetchBatch
}

func (w *Worker) handleFetchResult(ctx context.Context, err error) (nextConsumer jetstream.Consumer, stop bool, runErr error) {
	switch {
	case err == nil:
		return nil, false, nil
	case w.isShutdownFetchError(ctx, err):
		return nil, true, nil
	case w.isIdleFetchError(ctx, err):
		w.waitAfterIdleFetch()
		return nil, false, nil
	case isTemporaryJetStreamError(err):
		return w.handleTemporaryJetStreamFetchError(ctx, err)
	case w.isRecoverableConnectionFetchError(err):
		return w.recoverAfterReconnect(ctx, err)
	default:
		return nil, true, fmt.Errorf("natasks: fetch messages: %w", err)
	}
}

func (w *Worker) handleTemporaryJetStreamFetchError(ctx context.Context, err error) (nextConsumer jetstream.Consumer, stop bool, runErr error) {
	status := connectionStatus(w.jetStreamConn())
	w.log().Warn("natasks: fetch interrupted by temporary jetstream error", "error", err, "status", status.String())

	switch status {
	case nats.CONNECTING, nats.DISCONNECTED, nats.RECONNECTING:
		return w.recoverAfterReconnect(ctx, err)
	case nats.CONNECTED:
		if reconnectErr := w.forceReconnect(ctx); reconnectErr != nil {
			if errors.Is(reconnectErr, context.Canceled) || errors.Is(reconnectErr, context.DeadlineExceeded) {
				return nil, true, nil
			}
			w.log().Warn("natasks: force nats reconnect after temporary jetstream error", "error", reconnectErr)
		}
	}

	w.waitAfterIdleFetch()
	return nil, false, nil
}

func (w *Worker) recoverAfterReconnect(ctx context.Context, cause error) (nextConsumer jetstream.Consumer, stop bool, runErr error) {
	status := connectionStatus(w.jetStreamConn())
	w.log().Warn("natasks: fetch interrupted, waiting for nats reconnect", "error", cause, "status", status.String())

	if waitErr := w.waitForReconnect(ctx); waitErr != nil {
		if errors.Is(waitErr, context.Canceled) || errors.Is(waitErr, context.DeadlineExceeded) {
			return nil, true, nil
		}
		return nil, true, waitErr
	}

	consumer, recoverErr := w.recoverConsumerForRun()
	if recoverErr != nil {
		w.log().Warn("natasks: recover worker consumer after reconnect", "error", recoverErr)
		w.waitAfterIdleFetch()
		return nil, false, nil
	}

	w.log().Info("natasks: nats connection restored, resuming worker")
	return consumer, false, nil
}

func (w *Worker) forceReconnect(ctx context.Context) error {
	nc := w.jetStreamConn()
	if nc == nil {
		return nil
	}

	if err := nc.ForceReconnect(); err != nil {
		return err
	}

	return w.waitForReconnect(ctx)
}

func (w *Worker) waitAfterIdleFetch() {
	time.Sleep(w.cfg.idleWait)
}

func (w *Worker) isIdleFetchError(ctx context.Context, err error) bool {
	if errors.Is(err, nats.ErrTimeout) {
		return true
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ctx.Err() == nil
	}

	return false
}

func (w *Worker) isShutdownFetchError(ctx context.Context, err error) bool {
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	return ctx.Err() != nil
}

func (w *Worker) isRecoverableConnectionFetchError(err error) bool {
	if err == nil {
		return false
	}

	switch connectionStatus(w.jetStreamConn()) {
	case nats.CONNECTING, nats.DISCONNECTED, nats.RECONNECTING:
		return true
	default:
		return false
	}
}

func (w *Worker) isRecoverableFetchError(err error) bool {
	return isTemporaryJetStreamError(err) || w.isRecoverableConnectionFetchError(err)
}

func (w *Worker) waitForReconnect(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		switch connectionStatus(w.jetStreamConn()) {
		case nats.CONNECTED:
			return nil
		case nats.CLOSED:
			if err := connectionLastError(w.jetStreamConn()); err != nil {
				return fmt.Errorf("natasks: nats connection closed: %w", err)
			}
			return fmt.Errorf("natasks: nats connection closed")
		}

		w.waitAfterIdleFetch()
	}
}

func (w *Worker) terminateMessage(msg jetstream.Msg, reason string, cause error) error {
	if err := msg.Term(); err != nil {
		return fmt.Errorf("natasks: terminate %s message: %w", reason, err)
	}

	return cause
}
