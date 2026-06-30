package natasks

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Client dispatches tasks into JetStream.
type Client struct {
	js             jetstream.JetStream
	cfg            config
	dispatch       DispatchFunc
	logger         *slog.Logger
	scheduleMu     sync.Mutex
	schedulesReady bool
}

type dispatchScheduleContextKey struct{}

// NewClient constructs a dispatch client and ensures the stream exists.
func NewClient(js jetstream.JetStream, opts ...Option) (*Client, error) {
	cfg, err := collectConfig(opts)
	if err != nil {
		return nil, err
	}

	if js == nil {
		return nil, fmt.Errorf("natasks: nil jetstream context")
	}

	if err := retryJetStreamManagement(func() error {
		return ensureStream(js, cfg)
	}); err != nil {
		return nil, err
	}

	client := &Client{
		js:     js,
		cfg:    cfg,
		logger: slog.Default(),
	}
	client.dispatch = chainDispatchMiddleware(client.dispatchNow, cfg.dispatchMiddleware)

	return client, nil
}

// WithLogger replaces the client logger.
func (c *Client) WithLogger(logger *slog.Logger) *Client {
	if c == nil || logger == nil {
		return c
	}

	c.logger = logger
	return c
}

func (c *Client) log() *slog.Logger {
	if c == nil || c.logger == nil {
		return slog.Default()
	}

	return c.logger
}

// Dispatch publishes a task to the queue.
func (c *Client) Dispatch(ctx context.Context, task *Task, queue string) error {
	return c.dispatchTask(ctx, task, queue)
}

// DispatchIn publishes a task that should become visible after the given delay.
func (c *Client) DispatchIn(ctx context.Context, task *Task, queue string, delay time.Duration) error {
	if delay <= 0 {
		return c.dispatchTask(ctx, task, queue)
	}

	return c.DispatchAt(ctx, task, queue, time.Now().Add(delay))
}

// DispatchAt publishes a task that should become visible at the given time.
func (c *Client) DispatchAt(ctx context.Context, task *Task, queue string, at time.Time) error {
	at = at.UTC()
	if !at.After(time.Now().UTC()) {
		return c.dispatchTask(ctx, task, queue)
	}

	if err := c.ensureSchedulesEnabled(); err != nil {
		return err
	}

	return c.dispatchTask(withDispatchScheduleAt(ctx, at), task, queue)
}

func (c *Client) dispatchTask(ctx context.Context, task *Task, queue string) error {
	if c == nil {
		return fmt.Errorf("natasks: nil client")
	}

	if task == nil {
		return fmt.Errorf("natasks: nil task")
	}

	if err := validateQueueName(queue); err != nil {
		return err
	}

	if ctx == nil {
		ctx = context.Background()
	}

	return c.dispatch(ctx, task, queue)
}

func (c *Client) newMessage(ctx context.Context, queue string, task *Task) *nats.Msg {
	queue = normalizeQueue(queue)
	targetSubject := queueSubject(c.cfg.subjectPrefix, queue)
	msg := buildTaskMessage(targetSubject, queue, task)

	if scheduledAt, ok := dispatchScheduleAt(ctx); ok {
		c.applyScheduleHeaders(msg, scheduledAt, targetSubject)
	}

	return msg
}

func (c *Client) applyScheduleHeaders(msg *nats.Msg, at time.Time, targetSubject string) {
	msg.Subject = scheduleSubject(c.cfg.subjectPrefix)
	msg.Header.Set(headerSchedule, formatScheduleAt(at))
	msg.Header.Set(headerScheduleTarget, targetSubject)
}

func (c *Client) dispatchNow(ctx context.Context, task *Task, queue string) error {
	msg := c.newMessage(ctx, queue, task)
	if c.cfg.propagator != nil {
		c.cfg.propagator.Inject(ctx, natsHeaderCarrier(msg.Header))
	}

	if _, err := c.js.PublishMsg(ctx, msg); err != nil {
		c.log().Debug("natasks: task publish failed",
			"queue", queue,
			"task", task.Name(),
			"message_id", task.MessageID(),
			"subject", msg.Subject,
			"error", err,
		)
		return fmt.Errorf("natasks: publish task: %w", err)
	}

	c.log().Debug("natasks: task published",
		"queue", queue,
		"task", task.Name(),
		"message_id", task.MessageID(),
		"subject", msg.Subject,
	)

	return nil
}

func (c *Client) ensureSchedulesEnabled() error {
	if c == nil {
		return fmt.Errorf("natasks: nil client")
	}

	c.scheduleMu.Lock()
	defer c.scheduleMu.Unlock()

	if c.schedulesReady {
		return nil
	}

	if err := retryJetStreamManagement(func() error {
		return ensureStream(c.js, c.cfg)
	}); err != nil {
		return err
	}

	c.schedulesReady = true
	return nil
}

func withDispatchScheduleAt(ctx context.Context, at time.Time) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	return context.WithValue(ctx, dispatchScheduleContextKey{}, at.UTC())
}

func dispatchScheduleAt(ctx context.Context) (time.Time, bool) {
	if ctx == nil {
		return time.Time{}, false
	}

	at, ok := ctx.Value(dispatchScheduleContextKey{}).(time.Time)
	return at, ok
}

func formatScheduleAt(at time.Time) string {
	return "@at " + at.UTC().Format(time.RFC3339Nano)
}
