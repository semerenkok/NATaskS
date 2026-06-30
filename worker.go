package natasks

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

var ErrHandlerNotFound = errors.New("natasks: handler not found")

// Handler processes an incoming task.
type Handler func(context.Context, *Task) error

// Worker consumes tasks from a queue and dispatches them to registered handlers.
type Worker struct {
	js       jetstream.JetStream
	consumer jetstream.Consumer
	cfg      workerConfig
	queue    string
	logger   *slog.Logger
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewWorker constructs a worker for a single queue and ensures the required
// stream and consumer exist.
func NewWorker(js jetstream.JetStream, queue string, opts ...WorkerOption) (*Worker, error) {
	cfg, err := collectWorkerConfig(opts)
	if err != nil {
		return nil, err
	}

	if js == nil {
		return nil, fmt.Errorf("natasks: nil jetstream context")
	}

	if err := validateQueueName(queue); err != nil {
		return nil, err
	}

	if err := retryJetStreamManagement(func() error {
		return ensureStream(js, cfg.config)
	}); err != nil {
		return nil, err
	}

	w := &Worker{
		js:       js,
		cfg:      cfg,
		queue:    normalizeQueue(queue),
		logger:   slog.Default(),
		handlers: make(map[string]Handler),
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

	return w, nil
}

// Handle registers a handler for a task name.
func (w *Worker) Handle(name string, handler Handler) {
	if w == nil {
		return
	}

	name = strings.TrimSpace(name)

	if name == "" {
		panic("natasks: handler name is required")
	}

	if handler == nil {
		panic("natasks: handler is nil")
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	w.handlers[name] = chainProcessMiddleware(handler, w.cfg.processMiddleware)
}

// WithLogger replaces the worker logger.
func (w *Worker) WithLogger(logger *slog.Logger) *Worker {
	if w == nil || logger == nil {
		return w
	}

	w.logger = logger
	return w
}

func (w *Worker) log() *slog.Logger {
	if w == nil || w.logger == nil {
		return slog.Default()
	}

	return w.logger
}

// Run starts the fetch loop and blocks until ctx is canceled or the underlying
// NATS connection is permanently closed. Temporary disconnects are treated as
// recoverable: the worker waits for the connection to return, ensures the
// stream and consumer exist again, and then resumes fetching.
func (w *Worker) Run(ctx context.Context) error {
	if w == nil {
		return fmt.Errorf("natasks: nil worker")
	}

	if ctx == nil {
		ctx = context.Background()
	}

	// Graceful shutdown should stop fetching new messages, but allow the
	// already-started handler to finish. Values from the run context are kept.
	handleCtx := context.WithoutCancel(ctx)

	consumer, err := w.consumerForRun()
	if err != nil {
		runErr := fmt.Errorf("natasks: get worker consumer: %w", err)
		w.log().Error("natasks: worker stopped due to nats", "queue", w.queue, "consumer", w.consumerName(), "error", runErr, "status", connectionStatus(w.jetStreamConn()).String())
		return runErr
	}

	w.log().Info("natasks: worker started", "queue", w.queue, "consumer", w.consumerName())

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		msgs, err := w.fetchBatch(ctx, consumer)
		if err != nil || len(msgs) == 0 {
			nextConsumer, stop, runErr := w.handleFetchResult(ctx, err)
			if nextConsumer != nil {
				consumer = nextConsumer
			}
			if stop {
				if runErr != nil {
					w.log().Error("natasks: worker stopped due to nats", "queue", w.queue, "consumer", w.consumerName(), "error", runErr, "status", connectionStatus(w.jetStreamConn()).String())
				}
				return runErr
			}

			continue
		}

		w.processBatch(handleCtx, msgs)
	}
}

func (w *Worker) handleMessage(ctx context.Context, msg jetstream.Msg) error {
	ctx = w.extractMessageContext(ctx, msg)
	ctx, cancel := w.processingContext(ctx)
	defer cancel()

	task, handler, err := w.messageTask(msg)
	if err != nil {
		return err
	}

	startedAt := time.Now()
	w.log().Debug("natasks: task processing started",
		"queue", w.queue,
		"task", task.Name(),
		"message_id", task.MessageID(),
	)

	stopProgress := w.startProgressLoop(msg)
	defer stopProgress()

	if err := w.runHandler(ctx, handler, task); err != nil {
		w.log().Debug("natasks: task processing finished",
			"queue", w.queue,
			"task", task.Name(),
			"message_id", task.MessageID(),
			"duration", time.Since(startedAt),
			"error", err,
		)
		return w.retryOrDeadLetter(msg, task, err)
	}

	err = w.ackMessage(msg)
	w.log().Debug("natasks: task processing finished",
		"queue", w.queue,
		"task", task.Name(),
		"message_id", task.MessageID(),
		"duration", time.Since(startedAt),
		"error", err,
	)
	return err
}

func (w *Worker) processBatch(ctx context.Context, msgs []jetstream.Msg) {
	if len(msgs) == 0 {
		return
	}

	limit := w.cfg.concurrency
	if limit <= 1 || len(msgs) == 1 {
		for _, msg := range msgs {
			if err := w.handleMessage(ctx, msg); err != nil {
				w.logger.Error("natasks: handle task", "error", err)
			}
		}
		return
	}

	if limit > len(msgs) {
		limit = len(msgs)
	}

	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup

	for _, msg := range msgs {
		sem <- struct{}{}
		wg.Add(1)

		go func(msg jetstream.Msg) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := w.handleMessage(ctx, msg); err != nil {
				w.logger.Error("natasks: handle task", "error", err)
			}
		}(msg)
	}

	wg.Wait()
}

func (w *Worker) extractMessageContext(ctx context.Context, msg jetstream.Msg) context.Context {
	if w.cfg.propagator == nil {
		return ctx
	}

	return w.cfg.propagator.Extract(ctx, natsHeaderCarrier(msg.Headers()))
}

func (w *Worker) messageTask(msg jetstream.Msg) (*Task, Handler, error) {
	taskName := msg.Headers().Get(headerTaskName)
	if taskName == "" {
		return nil, nil, w.terminateMessage(msg, "malformed", fmt.Errorf("natasks: message missing task name header"))
	}

	handler, ok := w.handler(taskName)
	if !ok {
		return nil, nil, w.terminateMessage(msg, "unhandled", fmt.Errorf("%w: %s", ErrHandlerNotFound, taskName))
	}

	task, err := taskFromMessage(msg)
	if err != nil {
		return nil, nil, w.terminateMessage(msg, "invalid task", err)
	}

	return task, handler, nil
}

func (w *Worker) ackMessage(msg jetstream.Msg) error {
	if err := msg.Ack(); err != nil {
		return fmt.Errorf("natasks: ack message: %w", err)
	}

	return nil
}

func (w *Worker) processingContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if w.cfg.taskTimeout <= 0 {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, w.cfg.taskTimeout)
}

func (w *Worker) runHandler(ctx context.Context, handler Handler, task *Task) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("natasks: panic recovered: %v\n%s", recovered, debug.Stack())
		}
	}()

	return handler(ctx, task)
}
