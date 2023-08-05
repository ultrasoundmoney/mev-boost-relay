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
	Slot    uint64                    `json:"slot"`
	Payload *capella.ExecutionPayload `json:"payload"`
}

func NewPayloadArchive(ns INatsService) (*PayloadArchive, error) {
	//nolint:exhaustruct
	_, err := ns.UpsertStream(&nats.StreamConfig{
		MaxAge:   time.Hour,
		MaxBytes: 240 * 1024 * 1024 * 1024, // 480_000 * 0.5 MiB max per message. 240 GB in bytes.
		MaxMsgs:  480_000,                  // max 1600 payloads per slot * 5 slots per minute * 60 minutes per hour = 480_000.
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
