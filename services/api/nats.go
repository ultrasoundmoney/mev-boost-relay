package api

import (
	"github.com/nats-io/nats.go"
)

// INatsService manages the connection to the NATS server and provides only the
// functionality used by its consuming code. Uses an interface for easy
// testing.
type INatsService interface {
	LastError() error
	Publish(subject string, data []byte, opts nats.PubOpt) error
	UpsertStream(config *nats.StreamConfig) (*nats.StreamInfo, error)
}

type NatsService struct {
	nc *nats.Conn
	js nats.JetStreamContext
}

func NewNatsService(natsURI string) (*NatsService, error) {
	nc, err := nats.Connect(natsURI)
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	ns := &NatsService{
		nc: nc,
		js: js,
	}

	return ns, nil
}

func (n *NatsService) Publish(subject string, data []byte, opts nats.PubOpt) error {
	_, err := n.js.Publish(subject, data, opts)
	return err
}

func (n *NatsService) LastError() error {
	return n.nc.LastError()
}

// UpsertStream creates a stream if it doesn't exist, or updates it if it does.
func (n *NatsService) UpsertStream(config *nats.StreamConfig) (*nats.StreamInfo, error) {
	var streamInfo *nats.StreamInfo
	var err error

	streamInfo, err = n.js.AddStream(config)

	// Check if error is stream exists error
	if err != nil && err.Error() == "409" {
		// Stream exists, so update it
		streamInfo, err = n.js.UpdateStream(config)
		if err != nil {
			return nil, err
		}
	}

	return streamInfo, nil
}
