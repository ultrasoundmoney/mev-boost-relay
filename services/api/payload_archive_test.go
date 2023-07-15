package api

import (
	"testing"
	"time"

	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/flashbots/go-boost-utils/types"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// mockNatsService also satisfies the INatsService interface.
type mockNatsService struct {
	mock.Mock
}

func (m *mockNatsService) Publish(subject string, data []byte, opts nats.PubOpt) error {
	args := m.Called(subject, data, opts)
	return args.Error(0)
}

func (m *mockNatsService) LastError() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockNatsService) OnShutdown() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockNatsService) UpsertStream(config *nats.StreamConfig) (*nats.StreamInfo, error) {
	args := m.Called(config)
	return nil, args.Error(0)
}

func TestPublishPayload(t *testing.T) {
	// Create a mock NatsService
	natsService := new(mockNatsService)

	streamConfig := nats.StreamConfig{
		MaxAge:    time.Hour,
		MaxBytes:  4_000_000,
		MaxMsgs:   120_000,
		Name:      "payload-archive",
		Retention: nats.WorkQueuePolicy,
		Subjects:  []string{"payload-archive"},
	}

	natsService.On("UpsertStream", &streamConfig).Return(nil)

	// Create the PayloadArchive with the mock NatsService
	payloadArchive, err := NewPayloadArchive(natsService)
	require.NoError(t, err)

	// Prepare a payload to publish
	payload := capella.ExecutionPayload{
		ParentHash:    phase0.Hash32{0x01},
		FeeRecipient:  bellatrix.ExecutionAddress{0x02},
		StateRoot:     types.Root{0x03},
		ReceiptsRoot:  types.Root{0x04},
		LogsBloom:     types.Bloom{0x05},
		BlockNumber:   5001,
		GasLimit:      5002,
		GasUsed:       5003,
		Timestamp:     5004,
		ExtraData:     []byte{0x07},
		BaseFeePerGas: types.IntToU256(123),
		BlockHash:     phase0.Hash32{0x09},
		Transactions:  []bellatrix.Transaction{},
	}

	// Serialize the payload to JSON
	msg, err := payload.MarshalJSON()
	require.NoError(t, err)

	// Expect a call to Publish with the correct subject and data, and program it to return no error
	natsService.On("Publish", "payload-archive", msg, nats.AckWait(0)).Return(nil)
	// natsService.On("Flush").Return(nil)

	// Expect a call to LastError and program it to return no error
	natsService.On("LastError").Return(nil)

	// Call the method we're testing
	err = payloadArchive.PublishPayload(0, &payload)

	// Assert that there was no error
	require.NoError(t, err)

	// Assert that the expectations were met
	natsService.AssertExpectations(t)
}
