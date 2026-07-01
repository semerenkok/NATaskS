package natasks

import (
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

func ensureStream(js jetstream.JetStream, cfg config) error {
	ctx, cancel := managementContext()
	defer cancel()

	stream, err := js.Stream(ctx, cfg.streamName)
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			if !cfg.autoCreateStream {
				return missingStreamError(cfg, err)
			}

			return createStream(ctx, js, cfg)
		}

		return fmt.Errorf("natasks: get stream info: %w", err)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		return fmt.Errorf("natasks: get stream info: %w", err)
	}

	if !hasStreamSubject(info.Config.Subjects, cfg.subjectPrefix) {
		return fmt.Errorf("natasks: stream %q exists without subject %q", cfg.streamName, streamSubject(cfg.subjectPrefix))
	}

	if info.Config.AllowMsgSchedules {
		return nil
	}

	return updateStreamSchedules(ctx, js, info.Config)
}

func streamConfig(cfg config) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:              cfg.streamName,
		Subjects:          streamSubjects(cfg.subjectPrefix),
		Retention:         jetstream.WorkQueuePolicy,
		Storage:           jetstream.FileStorage,
		Discard:           jetstream.DiscardOld,
		AllowMsgSchedules: true,
	}
}

func missingStreamError(cfg config, err error) error {
	return fmt.Errorf("natasks: stream %q does not exist and auto-create is disabled: %w", cfg.streamName, err)
}

func createStream(ctx context.Context, js jetstream.JetStream, cfg config) error {
	if _, err := js.CreateStream(ctx, streamConfig(cfg)); err != nil {
		return fmt.Errorf("natasks: add stream: %w", err)
	}

	return nil
}

func updateStreamSchedules(ctx context.Context, js jetstream.JetStream, cfg jetstream.StreamConfig) error {
	cfg.AllowMsgSchedules = true

	if _, err := js.UpdateStream(ctx, cfg); err != nil {
		return fmt.Errorf("natasks: update stream: %w", err)
	}

	return nil
}

func hasStreamSubject(subjects []string, prefix string) bool {
	expected := streamSubject(prefix)
	for _, subject := range subjects {
		if subject == expected {
			return true
		}
	}

	return false
}
