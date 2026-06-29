package natasks

import (
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

func TestRetryJetStreamManagementRetriesTemporaryErrors(t *testing.T) {
	attempts := 0

	err := retryJetStreamManagement(func() error {
		attempts++
		if attempts < 3 {
			return &jetstream.APIError{
				Code:        503,
				ErrorCode:   jsErrCodeStreamOffline,
				Description: "stream is offline",
			}
		}

		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 3, attempts)
}

func TestRetryJetStreamManagementDoesNotRetryPermanentErrors(t *testing.T) {
	wantErr := errors.New("permanent")
	attempts := 0

	err := retryJetStreamManagement(func() error {
		attempts++
		return wantErr
	})

	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 1, attempts)
}
