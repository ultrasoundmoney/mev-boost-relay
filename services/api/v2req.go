package api

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	v1 "github.com/attestantio/go-builder-client/api/v1"
	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/pkg/errors"
)

// SubmitBlockRequest is the v2 request from the builder to submit a block.
type SubmitBlockRequestV2 struct {
	Message                *v1.BidTrace
	ExecutionPayloadHeader *capella.ExecutionPayloadHeader
	Signature              phase0.BLSSignature `ssz-size:"96"`
	Transactions           []bellatrix.Transaction
	Withdrawals            []capella.Withdrawal
}

// String returns a string version of the structure.
func (s *SubmitBlockRequestV2) String() string {
	data, err := json.Marshal(s)
	if err != nil {
		return fmt.Sprintf("ERR: %v", err)
	}
	return string(data)
}

// submitBlockRequestJSON is the spec representation of the struct.
type submitBlockRequestV2JSON struct {
	Message                *v1.BidTrace                    `json:"message"`
	ExecutionPayloadHeader *capella.ExecutionPayloadHeader `json:"execution_payload_header"`
	Signature              string                          `json:"signature"`
	Transactions           []string                        `json:"transactions"`
	Withdrawals            []capella.Withdrawal            `json:"withdrawals"`
}

// MarshalJSON implements json.Marshaler.
func (s *SubmitBlockRequestV2) MarshalJSON() ([]byte, error) {
	transactions := make([]string, len(s.Transactions))
	for i := range s.Transactions {
		transactions[i] = fmt.Sprintf("%#x", s.Transactions[i])
	}
	return json.Marshal(&submitBlockRequestV2JSON{
		Message:                s.Message,
		ExecutionPayloadHeader: s.ExecutionPayloadHeader,
		Signature:              fmt.Sprintf("%#x", s.Signature),
		Transactions:           transactions,
		Withdrawals:            s.Withdrawals,
	})
}

// UnmarshalJSON implements json.Unmarshaler.
func (s *SubmitBlockRequestV2) UnmarshalJSON(input []byte) error {
	var data submitBlockRequestV2JSON
	if err := json.Unmarshal(input, &data); err != nil {
		return errors.Wrap(err, "invalid JSON")
	}
	return s.unpack(&data)
}

func (s *SubmitBlockRequestV2) unpack(data *submitBlockRequestV2JSON) error {
	// field: Message
	if data.Message == nil {
		return errors.New("message missing")
	}
	s.Message = data.Message

	// field: ExecutionPayloadHeader
	if data.ExecutionPayloadHeader == nil {
		return errors.New("execution payload header missing")
	}
	s.ExecutionPayloadHeader = data.ExecutionPayloadHeader

	// field: Signature
	if data.Signature == "" {
		return errors.New("signature missing")
	}
	signature, err := hex.DecodeString(strings.TrimPrefix(data.Signature, "0x"))
	if err != nil {
		return errors.Wrap(err, "invalid signature")
	}
	if len(signature) != phase0.SignatureLength {
		return errors.New("incorrect length for signature")
	}
	copy(s.Signature[:], signature)

	// field: Transactions
	if data.Transactions == nil {
		return errors.New("transactions missing")
	}
	transactions := make([]bellatrix.Transaction, len(data.Transactions))
	for i := range data.Transactions {
		if data.Transactions[i] == "" {
			return errors.New("transaction missing")
		}
		tmp, err := hex.DecodeString(strings.TrimPrefix(data.Transactions[i], "0x"))
		if err != nil {
			return errors.Wrap(err, "invalid value for transaction")
		}
		transactions[i] = bellatrix.Transaction(tmp)
	}
	s.Transactions = transactions

	// field: Withdrawals
	if data.Withdrawals == nil {
		return errors.New("withdrawals missing")
	}
	s.Withdrawals = data.Withdrawals

	return nil
}
