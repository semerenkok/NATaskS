package natasks

import (
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func TestStreamConfig(t *testing.T) {
	cfg := streamConfig(config{
		streamName:    "APP",
		subjectPrefix: "app.tasks",
	})

	require.Equal(t, "APP", cfg.Name)
	require.Equal(t, []string{"app.tasks.*"}, cfg.Subjects)
	require.Equal(t, jetstream.WorkQueuePolicy, cfg.Retention)
	require.Equal(t, jetstream.FileStorage, cfg.Storage)
	require.Equal(t, jetstream.DiscardOld, cfg.Discard)
	require.True(t, cfg.AllowMsgSchedules)
}

func TestMissingStreamErrorPreservesCause(t *testing.T) {
	err := missingStreamError(config{streamName: "APP"}, jetstream.ErrStreamNotFound)

	require.ErrorIs(t, err, jetstream.ErrStreamNotFound)
	require.True(t, errors.Is(err, jetstream.ErrStreamNotFound))
	require.Contains(t, err.Error(), `stream "APP" does not exist`)
	require.Contains(t, err.Error(), "auto-create is disabled")
}

func TestHasStreamSubject(t *testing.T) {
	require.True(t, hasStreamSubject([]string{"app.tasks.*"}, "app.tasks"))
	require.False(t, hasStreamSubject([]string{"other.*"}, "app.tasks"))
}
