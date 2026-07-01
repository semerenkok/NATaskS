package natasks

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCollectConfigAppliesOptions(t *testing.T) {
	cfg, err := collectConfig([]Option{
		WithStreamName("APP"),
		nil,
		WithSubjectPrefix("app.tasks"),
		WithStreamAutoCreate(false),
	})
	require.NoError(t, err)
	require.Equal(t, "APP", cfg.streamName)
	require.Equal(t, "app.tasks", cfg.subjectPrefix)
	require.False(t, cfg.autoCreateStream)
}

func TestConfigValidate(t *testing.T) {
	testCases := []struct {
		name string
		cfg  config
	}{
		{name: "empty stream", cfg: config{streamName: "", subjectPrefix: "tasks"}},
		{name: "empty prefix", cfg: config{streamName: "TASKS", subjectPrefix: ""}},
		{name: "wildcard prefix", cfg: config{streamName: "TASKS", subjectPrefix: "tasks.*"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, tc.cfg.validate())
		})
	}
}

func TestCollectWorkerConfigAppliesOptions(t *testing.T) {
	cfg, err := collectWorkerConfig([]WorkerOption{
		WithStreamName("APP"),
		WithSubjectPrefix("app.tasks"),
		WithStreamAutoCreate(false),
		WithConsumerPrefix("worker"),
		WithDurable("jobs"),
		WithConcurrency(4),
		WithFetchBatch(10),
		WithFetchTimeout(2 * time.Second),
		WithTaskTimeout(3 * time.Second),
		WithIdleWait(20 * time.Millisecond),
		WithAckWait(4 * time.Second),
		WithProgressInterval(time.Second),
		WithMaxAckPending(77),
		WithMaxRetries(5),
		WithRetryBackoff(time.Second, 2*time.Second),
		WithDLQSuffix(".dlq"),
		nil,
	})
	require.NoError(t, err)
	require.Equal(t, "APP", cfg.streamName)
	require.Equal(t, "app.tasks", cfg.subjectPrefix)
	require.False(t, cfg.autoCreateStream)
	require.Equal(t, "worker", cfg.consumerPrefix)
	require.Equal(t, "jobs", cfg.durable)
	require.Equal(t, 4, cfg.concurrency)
	require.Equal(t, 10, cfg.fetchBatch)
	require.Equal(t, 2*time.Second, cfg.fetchTimeout)
	require.Equal(t, 3*time.Second, cfg.taskTimeout)
	require.Equal(t, 20*time.Millisecond, cfg.idleWait)
	require.Equal(t, 4*time.Second, cfg.ackWait)
	require.Equal(t, time.Second, cfg.progressInterval)
	require.Equal(t, 77, cfg.maxAckPending)
	require.Equal(t, 5, cfg.maxRetries)
	require.Equal(t, []time.Duration{time.Second, 2 * time.Second}, cfg.retryBackoff)
	require.Equal(t, ".dlq", cfg.dlqSuffix)
}

func TestWorkerConfigValidate(t *testing.T) {
	testCases := []struct {
		name string
		mut  func(*workerConfig)
	}{
		{name: "concurrency", mut: func(cfg *workerConfig) { cfg.concurrency = 0 }},
		{name: "fetch batch", mut: func(cfg *workerConfig) { cfg.fetchBatch = 0 }},
		{name: "fetch timeout", mut: func(cfg *workerConfig) { cfg.fetchTimeout = 0 }},
		{name: "task timeout", mut: func(cfg *workerConfig) { cfg.taskTimeout = -time.Second }},
		{name: "idle wait", mut: func(cfg *workerConfig) { cfg.idleWait = -time.Second }},
		{name: "ack wait", mut: func(cfg *workerConfig) { cfg.ackWait = 0 }},
		{name: "progress interval", mut: func(cfg *workerConfig) { cfg.progressInterval = 0 }},
		{name: "progress interval gte ack wait", mut: func(cfg *workerConfig) { cfg.progressInterval = cfg.ackWait }},
		{name: "max ack pending", mut: func(cfg *workerConfig) { cfg.maxAckPending = 0 }},
		{name: "max retries", mut: func(cfg *workerConfig) { cfg.maxRetries = -2 }},
		{name: "dlq suffix", mut: func(cfg *workerConfig) { cfg.dlqSuffix = " " }},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := defaultWorkerConfig()
			tc.mut(&cfg)
			require.Error(t, cfg.validate())
		})
	}
}
