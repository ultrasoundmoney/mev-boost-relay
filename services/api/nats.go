package api

import (
	"github.com/nats-io/nats.go"
)

// INatsService manages the connection to the NATS server and provides only the
// functionality used by its consuming code. Uses an interface for easy
// testing.
type INatsService interface {
	AddStream(config *nats.StreamConfig) (*nats.StreamInfo, error)
	LastError() error
	Publish(subject string, data []byte, opts nats.PubOpt) error
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

func (n *NatsService) AddStream(config *nats.StreamConfig) (*nats.StreamInfo, error) {
	return n.js.AddStream(config)
}
