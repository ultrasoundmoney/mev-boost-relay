package api

import (
	"encoding/json"
	"time"

	"github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/nats-io/nats.go"
)

// PayloadArchive stores execution payloads for later analysis while minimally
// slowing down the bidding process.
type PayloadArchive struct {
	ns      INatsService
	pubOpts nats.PubOpt
}

type ExecutionPayloadArchiveBundle struct {
	Slot    uint64
	Payload *capella.ExecutionPayload
}

func NewPayloadArchive(ns INatsService) (*PayloadArchive, error) {
	//nolint:exhaustruct
	_, err := ns.UpsertStream(&nats.StreamConfig{
		MaxAge:   time.Hour,
		MaxBytes: 4_000_000,
		MaxMsgs:  120_000,
		Name:     "payload-archive",
		// After messages are consumed, they can be dropped from the stream.
		Retention: nats.WorkQueuePolicy,
		Subjects:  []string{"payload-archive"},
	})
	if err != nil {
		return nil, err
	}

	return &PayloadArchive{
		ns: ns,
		// We don't care to know if the message was acknowledged by a consumer
		// or not, only to publish it onto the stream.
		pubOpts: nats.AckWait(0),
	}, nil
}

func (p *PayloadArchive) PublishPayload(slot uint64, payload *capella.ExecutionPayload) error {
	msg, err := json.Marshal(&ExecutionPayloadArchiveBundle{
		Slot:    slot,
		Payload: payload,
	})

	if err != nil {
		return err
	}

	if err := p.ns.Publish("payload-archive", msg, p.pubOpts); err != nil {
		return err
	}

	if err := p.ns.LastError(); err != nil {
		return err
	}

	return nil
}
