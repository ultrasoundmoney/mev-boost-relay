package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	builderCapella "github.com/attestantio/go-builder-client/api/capella"
	builderDeneb "github.com/attestantio/go-builder-client/api/deneb"
	apiv1 "github.com/attestantio/go-builder-client/api/v1"
	"github.com/attestantio/go-builder-client/spec"
	consensusspec "github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/attestantio/go-eth2-client/spec/deneb"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/flashbots/go-boost-utils/bls"
	"github.com/flashbots/go-boost-utils/utils"
	"github.com/flashbots/mev-boost-relay/beaconclient"
	"github.com/flashbots/mev-boost-relay/common"
	"github.com/flashbots/mev-boost-relay/database"
	"github.com/flashbots/mev-boost-relay/datastore"
	"github.com/holiman/uint256"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

const (
	testGasLimit        = uint64(30000000)
	testSlot            = uint64(42)
	testParentHash      = "0xbd3291854dc822b7ec585925cda0e18f06af28fa2886e15f52d52dd4b6f94ed6"
	testWithdrawalsRoot = "0x7f6d156912a4cb1e74ee37e492ad883f7f7ac856d987b3228b517e490aa0189e"
	testPrevRandao      = "0x9962816e9d0a39fd4c80935338a741dc916d1545694e41eb5a505e1a3098f9e4"
	testBuilderPubkey   = "0xfa1ed37c3553d0ce1e9349b2c5063cf6e394d231c8d3e0df75e9462257c081543086109ffddaacc0aa76f33dc9661c83"
)

var (
	testAddress  = bellatrix.ExecutionAddress([20]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19})
	testAddress2 = bellatrix.ExecutionAddress([20]byte{1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19})
)

type testBackend struct {
	t         require.TestingT
	relay     *RelayAPI
	datastore *datastore.Datastore
	redis     *datastore.RedisCache
}

func newTestBackend(t require.TestingT, numBeaconNodes int) *testBackend {
	redisClient, err := miniredis.Run()
	require.NoError(t, err)

	redisCache, err := datastore.NewRedisCache("", redisClient.Addr(), "")
	require.NoError(t, err)

	db := database.MockDB{}

	ds, err := datastore.NewDatastore(redisCache, nil, db)
	require.NoError(t, err)

	sk, _, err := bls.GenerateNewKeypair()
	require.NoError(t, err)

	mainnetDetails, err := common.NewEthNetworkDetails(common.EthNetworkMainnet)
	require.NoError(t, err)

	opts := RelayAPIOpts{
		Log:             common.TestLog,
		ListenAddr:      "localhost:12345",
		BeaconClient:    &beaconclient.MultiBeaconClient{},
		Datastore:       ds,
		Redis:           redisCache,
		DB:              db,
		EthNetDetails:   *mainnetDetails,
		SecretKey:       sk,
		ProposerAPI:     true,
		BlockBuilderAPI: true,
		DataAPI:         true,
		InternalAPI:     true,
	}

	relay, err := NewRelayAPI(opts)
	require.NoError(t, err)

	relay.genesisInfo = &beaconclient.GetGenesisResponse{
		Data: beaconclient.GetGenesisResponseData{
			GenesisTime: 1606824023,
		},
	}

	backend := testBackend{
		t:         t,
		relay:     relay,
		datastore: ds,
		redis:     redisCache,
	}
	return &backend
}

func (be *testBackend) requestBytes(method, path string, payload []byte, headers map[string]string) *httptest.ResponseRecorder {
	var req *http.Request
	var err error

	req, err = http.NewRequest(method, path, bytes.NewReader(payload))
	require.NoError(be.t, err)

	// Set headers
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	// lfg
	rr := httptest.NewRecorder()
	be.relay.getRouter().ServeHTTP(rr, req)
	return rr
}

func (be *testBackend) request(method, path string, payload any) *httptest.ResponseRecorder {
	var req *http.Request
	var err error

	if payload == nil {
		req, err = http.NewRequest(method, path, bytes.NewReader(nil))
	} else {
		payloadBytes, err2 := json.Marshal(payload)
		require.NoError(be.t, err2)
		req, err = http.NewRequest(method, path, bytes.NewReader(payloadBytes))
	}
	require.NoError(be.t, err)

	// lfg
	rr := httptest.NewRecorder()
	be.relay.getRouter().ServeHTTP(rr, req)
	return rr
}

func (be *testBackend) requestWithUA(method, path, userAgent string, payload any) *httptest.ResponseRecorder {
	var req *http.Request
	var err error

	if payload == nil {
		req, err = http.NewRequest(method, path, bytes.NewReader(nil))
	} else {
		payloadBytes, err2 := json.Marshal(payload)
		require.NoError(be.t, err2)
		req, err = http.NewRequest(method, path, bytes.NewReader(payloadBytes))
	}
	req.Header.Set("User-Agent", userAgent)

	require.NoError(be.t, err)
	rr := httptest.NewRecorder()
	be.relay.getRouter().ServeHTTP(rr, req)
	return rr
}

func TestWebserver(t *testing.T) {
	t.Run("errors when webserver is already existing", func(t *testing.T) {
		backend := newTestBackend(t, 1)
		backend.relay.srvStarted.Store(true)
		err := backend.relay.StartServer()
		require.Error(t, err)
	})
}

func TestWebserverRootHandler(t *testing.T) {
	backend := newTestBackend(t, 1)
	rr := backend.request(http.MethodGet, "/", nil)
	require.Equal(t, http.StatusOK, rr.Code)
}

func TestStatus(t *testing.T) {
	backend := newTestBackend(t, 1)
	path := "/eth/v1/builder/status"
	rr := backend.request(http.MethodGet, path, common.ValidPayloadRegisterValidator)
	require.Equal(t, http.StatusOK, rr.Code)
}

func TestLivez(t *testing.T) {
	backend := newTestBackend(t, 1)
	path := "/livez"
	rr := backend.request(http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "{\"message\":\"live\"}\n", rr.Body.String())
}

func TestRegisterValidator(t *testing.T) {
	path := "/eth/v1/builder/validators"

	t.Run("not a known validator", func(t *testing.T) {
		backend := newTestBackend(t, 1)

		rr := backend.request(http.MethodPost, path, []apiv1.SignedValidatorRegistration{common.ValidPayloadRegisterValidator})
		require.Equal(t, http.StatusBadRequest, rr.Code)
	})
}

func TestGetHeader(t *testing.T) {
	// Setup backend with headSlot and genesisTime
	backend := newTestBackend(t, 1)
	backend.relay.genesisInfo = &beaconclient.GetGenesisResponse{
		Data: beaconclient.GetGenesisResponseData{
			GenesisTime: uint64(time.Now().UTC().Unix()),
		},
	}

	// request params
	slot := uint64(2)
	backend.relay.headSlot.Store(slot)
	parentHash := "0x13e606c7b3d1faad7e83503ce3dedce4c6bb89b0c28ffb240d713c7b110b9747"
	proposerPubkey := "0x6ae5932d1e248d987d51b58665b81848814202d7b23b343d20f2a167d12f07dcb01ca41c42fdd60b7fca9c4b90890792"
	builderPubkey := "0xfa1ed37c3553d0ce1e9349b2c5063cf6e394d231c8d3e0df75e9462257c081543086109ffddaacc0aa76f33dc9661c83"
	bidValue := uint256.NewInt(99)
	trace := &common.BidTraceV2{
		BidTrace: apiv1.BidTrace{
			Value: bidValue,
		},
	}

	// request path
	path := fmt.Sprintf("/eth/v1/builder/header/%d/%s/%s", slot, parentHash, proposerPubkey)

	// Create a bid
	opts := common.CreateTestBlockSubmissionOpts{
		Slot:           slot,
		ParentHash:     parentHash,
		ProposerPubkey: proposerPubkey,
	}
	payload, getPayloadResp, getHeaderResp := common.CreateTestBlockSubmission(t, builderPubkey, bidValue, &opts)
	_, err := backend.redis.SaveBidAndUpdateTopBid(context.Background(), backend.redis.NewPipeline(), trace, payload, getPayloadResp, getHeaderResp, time.Now(), false, nil)
	require.NoError(t, err)

	// Check 1: regular request works and returns a bid
	rr := backend.request(http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, rr.Code)
	resp := spec.VersionedSignedBuilderBid{}
	err = json.Unmarshal(rr.Body.Bytes(), &resp)
	require.NoError(t, err)
	value, err := resp.Value()
	require.NoError(t, err)
	require.Equal(t, bidValue.String(), value.String())

	// Check 2: Request returns 204 if sending a filtered user agent
	rr = backend.requestWithUA(http.MethodGet, path, "mev-boost/v1.5.0 Go-http-client/1.1", nil)
	require.Equal(t, http.StatusNoContent, rr.Code)
}

func TestBuilderApiGetValidators(t *testing.T) {
	path := "/relay/v1/builder/validators"

	backend := newTestBackend(t, 1)
	duties := []common.BuilderGetValidatorsResponseEntry{
		{
			Slot:  1,
			Entry: &common.ValidPayloadRegisterValidator,
		},
	}
	responseBytes, err := json.Marshal(duties)
	require.NoError(t, err)
	backend.relay.proposerDutiesResponse = &responseBytes

	rr := backend.request(http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, rr.Code)

	resp := []common.BuilderGetValidatorsResponseEntry{}
	err = json.Unmarshal(rr.Body.Bytes(), &resp)
	require.NoError(t, err)
	require.Equal(t, 1, len(resp))
	require.Equal(t, uint64(1), resp[0].Slot)
	require.Equal(t, common.ValidPayloadRegisterValidator, *resp[0].Entry)
}

func TestDataApiGetDataProposerPayloadDelivered(t *testing.T) {
	path := "/relay/v1/data/bidtraces/proposer_payload_delivered"

	t.Run("Accept valid block_hash", func(t *testing.T) {
		backend := newTestBackend(t, 1)

		validBlockHash := "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		rr := backend.request(http.MethodGet, path+"?block_hash="+validBlockHash, nil)
		require.Equal(t, http.StatusOK, rr.Code)
	})

	t.Run("Reject invalid block_hash", func(t *testing.T) {
		backend := newTestBackend(t, 1)

		invalidBlockHashes := []string{
			// One character too long.
			"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab",
			// One character too short.
			"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			// Missing the 0x prefix.
			"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			// Has an invalid hex character ('z' at the end).
			"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaz",
		}

		for _, invalidBlockHash := range invalidBlockHashes {
			rr := backend.request(http.MethodGet, path+"?block_hash="+invalidBlockHash, nil)
			require.Equal(t, http.StatusBadRequest, rr.Code)
			require.Contains(t, rr.Body.String(), "invalid block_hash argument")
		}
	})
}

func TestBuilderSubmitBlockSSZ(t *testing.T) {
	requestPayloadJSONBytes := common.LoadGzippedBytes(t, "../../testdata/submitBlockPayloadCapella_Goerli.json.gz")

	req := new(common.VersionedSubmitBlockRequest)
	req.Capella = new(builderCapella.SubmitBlockRequest)
	err := json.Unmarshal(requestPayloadJSONBytes, req.Capella)
	require.NoError(t, err)

	reqSSZ, err := req.Capella.MarshalSSZ()
	require.NoError(t, err)
	require.Equal(t, 352239, len(reqSSZ))

	test := new(builderCapella.SubmitBlockRequest)
	err = test.UnmarshalSSZ(reqSSZ)
	require.NoError(t, err)
}

func TestBuilderSubmitBlock(t *testing.T) {
	path := "/relay/v1/builder/blocks"
	backend := newTestBackend(t, 1)

	headSlot := uint64(32)
	submissionSlot := headSlot + 1
	submissionTimestamp := 1606824419

	// Payload attributes
	payloadJSONFilename := "../../testdata/submitBlockPayloadCapella_Goerli.json.gz"
	parentHash := "0xbd3291854dc822b7ec585925cda0e18f06af28fa2886e15f52d52dd4b6f94ed6"
	feeRec, err := utils.HexToAddress("0x5cc0dde14e7256340cc820415a6022a7d1c93a35")
	require.NoError(t, err)
	withdrawalsRoot, err := utils.HexToHash("0xb15ed76298ff84a586b1d875df08b6676c98dfe9c7cd73fab88450348d8e70c8")
	require.NoError(t, err)
	prevRandao := "0x9962816e9d0a39fd4c80935338a741dc916d1545694e41eb5a505e1a3098f9e4"

	// Setup the test relay backend
	backend.relay.headSlot.Store(headSlot)
	backend.relay.capellaEpoch = 0
	backend.relay.denebEpoch = 1
	backend.relay.proposerDutiesMap = make(map[uint64]*common.BuilderGetValidatorsResponseEntry)
	backend.relay.proposerDutiesMap[headSlot+1] = &common.BuilderGetValidatorsResponseEntry{
		Slot: headSlot,
		Entry: &apiv1.SignedValidatorRegistration{
			Message: &apiv1.ValidatorRegistration{
				FeeRecipient: feeRec,
			},
		},
	}
	backend.relay.payloadAttributes = make(map[string]payloadAttributesHelper)
	backend.relay.payloadAttributes[parentHash] = payloadAttributesHelper{
		slot:       submissionSlot,
		parentHash: parentHash,
		payloadAttributes: beaconclient.PayloadAttributes{
			PrevRandao: prevRandao,
		},
		withdrawalsRoot: phase0.Root(withdrawalsRoot),
	}

	// Prepare the request payload
	req := new(common.VersionedSubmitBlockRequest)
	req.Capella = new(builderCapella.SubmitBlockRequest)
	requestPayloadJSONBytes := common.LoadGzippedBytes(t, payloadJSONFilename)
	require.NoError(t, err)
	err = json.Unmarshal(requestPayloadJSONBytes, req.Capella)
	require.NoError(t, err)

	// Update
	req.Capella.Message.Slot = submissionSlot
	req.Capella.ExecutionPayload.Timestamp = uint64(submissionTimestamp)

	// Send JSON encoded request
	reqJSONBytes, err := req.Capella.MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, 704810, len(reqJSONBytes))
	reqJSONBytes2, err := json.Marshal(req.Capella)
	require.NoError(t, err)
	require.Equal(t, reqJSONBytes, reqJSONBytes2)
	rr := backend.requestBytes(http.MethodPost, path, reqJSONBytes, nil)
	require.Contains(t, rr.Body.String(), "invalid signature")
	require.Equal(t, http.StatusBadRequest, rr.Code)

	// Send SSZ encoded request
	reqSSZBytes, err := req.Capella.MarshalSSZ()
	require.NoError(t, err)
	require.Equal(t, 352239, len(reqSSZBytes))
	rr = backend.requestBytes(http.MethodPost, path, reqSSZBytes, map[string]string{
		"Content-Type": "application/octet-stream",
	})
	require.Contains(t, rr.Body.String(), "invalid signature")
	require.Equal(t, http.StatusBadRequest, rr.Code)

	// Send JSON+GZIP encoded request
	headers := map[string]string{
		"Content-Encoding": "gzip",
	}
	jsonGzip := gzipBytes(t, reqJSONBytes)
	require.Equal(t, 207788, len(jsonGzip))
	rr = backend.requestBytes(http.MethodPost, path, jsonGzip, headers)
	require.Contains(t, rr.Body.String(), "invalid signature")
	require.Equal(t, http.StatusBadRequest, rr.Code)

	// Send SSZ+GZIP encoded request
	headers = map[string]string{
		"Content-Type":     "application/octet-stream",
		"Content-Encoding": "gzip",
	}

	sszGzip := gzipBytes(t, reqSSZBytes)
	require.Equal(t, 195923, len(sszGzip))
	rr = backend.requestBytes(http.MethodPost, path, sszGzip, headers)
	require.Contains(t, rr.Body.String(), "invalid signature")
	require.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestCheckSubmissionFeeRecipient(t *testing.T) {
	cases := []struct {
		description    string
		slotDuty       *common.BuilderGetValidatorsResponseEntry
		payload        *common.VersionedSubmitBlockRequest
		expectOk       bool
		expectGasLimit uint64
	}{
		{
			description: "success",
			slotDuty: &common.BuilderGetValidatorsResponseEntry{
				Entry: &apiv1.SignedValidatorRegistration{
					Message: &apiv1.ValidatorRegistration{
						FeeRecipient: testAddress,
						GasLimit:     testGasLimit,
					},
				},
			},
			payload: &common.VersionedSubmitBlockRequest{
				VersionedSubmitBlockRequest: spec.VersionedSubmitBlockRequest{
					Version: consensusspec.DataVersionCapella,
					Capella: &builderCapella.SubmitBlockRequest{
						Message: &apiv1.BidTrace{
							Slot:                 testSlot,
							ProposerFeeRecipient: testAddress,
						},
						ExecutionPayload: &capella.ExecutionPayload{},
					},
				},
			},
			expectOk:       true,
			expectGasLimit: testGasLimit,
		},
		{
			description: "failure_nil_slot_duty",
			slotDuty:    nil,
			payload: &common.VersionedSubmitBlockRequest{
				VersionedSubmitBlockRequest: spec.VersionedSubmitBlockRequest{
					Version: consensusspec.DataVersionCapella,
					Capella: &builderCapella.SubmitBlockRequest{
						Message: &apiv1.BidTrace{
							Slot: testSlot,
						},
						ExecutionPayload: &capella.ExecutionPayload{},
					},
				},
			},
			expectOk:       false,
			expectGasLimit: 0,
		},
		{
			description: "failure_diff_fee_recipient",
			slotDuty: &common.BuilderGetValidatorsResponseEntry{
				Entry: &apiv1.SignedValidatorRegistration{
					Message: &apiv1.ValidatorRegistration{
						FeeRecipient: testAddress,
						GasLimit:     testGasLimit,
					},
				},
			},
			payload: &common.VersionedSubmitBlockRequest{
				VersionedSubmitBlockRequest: spec.VersionedSubmitBlockRequest{
					Version: consensusspec.DataVersionCapella,
					Capella: &builderCapella.SubmitBlockRequest{
						Message: &apiv1.BidTrace{
							Slot:                 testSlot,
							ProposerFeeRecipient: testAddress2,
						},
						ExecutionPayload: &capella.ExecutionPayload{},
					},
				},
			},
			expectOk:       false,
			expectGasLimit: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			_, _, backend := startTestBackend(t)
			backend.relay.proposerDutiesLock.RLock()
			slot, err := tc.payload.Slot()
			require.NoError(t, err)
			backend.relay.proposerDutiesMap[slot] = tc.slotDuty
			backend.relay.proposerDutiesLock.RUnlock()

			w := httptest.NewRecorder()
			logger := logrus.New()
			log := logrus.NewEntry(logger)
			submission, err := common.GetBlockSubmissionInfo(tc.payload)
			require.NoError(t, err)
			gasLimit, ok := backend.relay.checkSubmissionFeeRecipient(w, log, submission)
			require.Equal(t, tc.expectGasLimit, gasLimit)
			require.Equal(t, tc.expectOk, ok)
		})
	}
}

func TestCheckSubmissionPayloadAttrs(t *testing.T) {
	withdrawalsRoot, err := utils.HexToHash(testWithdrawalsRoot)
	require.NoError(t, err)
	prevRandao, err := utils.HexToHash(testPrevRandao)
	require.NoError(t, err)
	parentHash, err := utils.HexToHash(testParentHash)
	require.NoError(t, err)

	cases := []struct {
		description string
		attrs       payloadAttributesHelper
		payload     *common.VersionedSubmitBlockRequest
		expectOk    bool
	}{
		{
			description: "success",
			attrs: payloadAttributesHelper{
				slot:            testSlot,
				parentHash:      testParentHash,
				withdrawalsRoot: phase0.Root(withdrawalsRoot),
				payloadAttributes: beaconclient.PayloadAttributes{
					PrevRandao: testPrevRandao,
				},
			},
			payload: &common.VersionedSubmitBlockRequest{
				VersionedSubmitBlockRequest: spec.VersionedSubmitBlockRequest{
					Version: consensusspec.DataVersionCapella,
					Capella: &builderCapella.SubmitBlockRequest{
						ExecutionPayload: &capella.ExecutionPayload{
							PrevRandao: prevRandao,
							Withdrawals: []*capella.Withdrawal{
								{
									Index: 989694,
								},
							},
						},
						Message: &apiv1.BidTrace{
							Slot:       testSlot,
							ParentHash: parentHash,
						},
					},
				},
			},
			expectOk: true,
		},
		{
			description: "failure_attrs_not_known",
			attrs: payloadAttributesHelper{
				slot: testSlot,
			},
			payload: &common.VersionedSubmitBlockRequest{
				VersionedSubmitBlockRequest: spec.VersionedSubmitBlockRequest{
					Version: consensusspec.DataVersionCapella,
					Capella: &builderCapella.SubmitBlockRequest{
						Message: &apiv1.BidTrace{
							Slot: testSlot + 1, // submission for a future slot
						},
						ExecutionPayload: &capella.ExecutionPayload{},
					},
				},
			},
			expectOk: false,
		},
		{
			description: "failure_wrong_prev_randao",
			attrs: payloadAttributesHelper{
				slot: testSlot,
				payloadAttributes: beaconclient.PayloadAttributes{
					PrevRandao: testPrevRandao,
				},
			},
			payload: &common.VersionedSubmitBlockRequest{
				VersionedSubmitBlockRequest: spec.VersionedSubmitBlockRequest{
					Version: consensusspec.DataVersionCapella,
					Capella: &builderCapella.SubmitBlockRequest{
						Message: &apiv1.BidTrace{
							Slot:       testSlot,
							ParentHash: parentHash,
						},
						ExecutionPayload: &capella.ExecutionPayload{
							PrevRandao: [32]byte(parentHash), // use a different hash to cause an error
						},
					},
				},
			},
			expectOk: false,
		},
		{
			description: "failure_nil_withdrawals",
			attrs: payloadAttributesHelper{
				slot: testSlot,
				payloadAttributes: beaconclient.PayloadAttributes{
					PrevRandao: testPrevRandao,
				},
			},
			payload: &common.VersionedSubmitBlockRequest{
				VersionedSubmitBlockRequest: spec.VersionedSubmitBlockRequest{
					Version: consensusspec.DataVersionCapella,
					Capella: &builderCapella.SubmitBlockRequest{
						Message: &apiv1.BidTrace{
							Slot:       testSlot,
							ParentHash: parentHash,
						},
						ExecutionPayload: &capella.ExecutionPayload{
							PrevRandao:  [32]byte(prevRandao),
							Withdrawals: nil, // set to nil to cause an error
						},
					},
				},
			},
			expectOk: false,
		},
		{
			description: "failure_wrong_withdrawal_root",
			attrs: payloadAttributesHelper{
				slot:            testSlot,
				parentHash:      testParentHash,
				withdrawalsRoot: phase0.Root(prevRandao), // use different root to cause an error
				payloadAttributes: beaconclient.PayloadAttributes{
					PrevRandao: testPrevRandao,
				},
			},
			payload: &common.VersionedSubmitBlockRequest{
				VersionedSubmitBlockRequest: spec.VersionedSubmitBlockRequest{
					Version: consensusspec.DataVersionCapella,
					Capella: &builderCapella.SubmitBlockRequest{
						ExecutionPayload: &capella.ExecutionPayload{
							PrevRandao: [32]byte(prevRandao),
							Withdrawals: []*capella.Withdrawal{
								{
									Index: 989694,
								},
							},
						},
						Message: &apiv1.BidTrace{
							Slot:       testSlot,
							ParentHash: parentHash,
						},
					},
				},
			},
			expectOk: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			_, _, backend := startTestBackend(t)
			backend.relay.payloadAttributesLock.RLock()
			backend.relay.payloadAttributes[testParentHash] = tc.attrs
			backend.relay.payloadAttributesLock.RUnlock()

			w := httptest.NewRecorder()
			logger := logrus.New()
			log := logrus.NewEntry(logger)
			submission, err := common.GetBlockSubmissionInfo(tc.payload)
			require.NoError(t, err)
			ok := backend.relay.checkSubmissionPayloadAttrs(w, log, submission)
			require.Equal(t, tc.expectOk, ok)
		})
	}
}

func TestCheckSubmissionSlotDetails(t *testing.T) {
	cases := []struct {
		description string
		payload     *common.VersionedSubmitBlockRequest
		expectOk    bool
	}{
		{
			description: "success",
			payload: &common.VersionedSubmitBlockRequest{
				VersionedSubmitBlockRequest: spec.VersionedSubmitBlockRequest{
					Version: consensusspec.DataVersionCapella,
					Capella: &builderCapella.SubmitBlockRequest{
						ExecutionPayload: &capella.ExecutionPayload{
							Timestamp: testSlot * common.SecondsPerSlot,
						},
						Message: &apiv1.BidTrace{
							Slot: testSlot,
						},
					},
				},
			},
			expectOk: true,
		},
		{
			description: "non_capella_slot",
			payload: &common.VersionedSubmitBlockRequest{
				VersionedSubmitBlockRequest: spec.VersionedSubmitBlockRequest{
					Version: consensusspec.DataVersionCapella,
					Capella: &builderCapella.SubmitBlockRequest{
						ExecutionPayload: &capella.ExecutionPayload{
							Timestamp: testSlot * common.SecondsPerSlot,
						},
						Message: &apiv1.BidTrace{
							Slot: testSlot + 32,
						},
					},
				},
			},
			expectCont: false,
		},
		{
			description: "non_deneb_slot",
			payload: &common.VersionedSubmitBlockRequest{
				VersionedSubmitBlockRequest: spec.VersionedSubmitBlockRequest{
					Version: consensusspec.DataVersionDeneb,
					Deneb: &builderDeneb.SubmitBlockRequest{
						ExecutionPayload: &deneb.ExecutionPayload{
							Timestamp: testSlot * common.SecondsPerSlot,
						},
						Message: &apiv1.BidTrace{
							Slot: testSlot,
						},
					},
				},
			},
			expectCont: false,
		},
		{
			description: "failure_past_slot",
			payload: &common.VersionedSubmitBlockRequest{
				VersionedSubmitBlockRequest: spec.VersionedSubmitBlockRequest{
					Version: consensusspec.DataVersionCapella,
					Capella: &builderCapella.SubmitBlockRequest{
						Message: &apiv1.BidTrace{
							Slot: testSlot - 1, // use old slot to cause error
						},
						ExecutionPayload: &capella.ExecutionPayload{},
					},
				},
			},
			expectOk: false,
		},
		{
			description: "failure_wrong_timestamp",
			payload: &common.VersionedSubmitBlockRequest{
				VersionedSubmitBlockRequest: spec.VersionedSubmitBlockRequest{
					Version: consensusspec.DataVersionCapella,
					Capella: &builderCapella.SubmitBlockRequest{
						ExecutionPayload: &capella.ExecutionPayload{
							Timestamp: testSlot*common.SecondsPerSlot - 1, // use wrong timestamp to cause error
						},
						Message: &apiv1.BidTrace{
							Slot: testSlot,
						},
					},
				},
			},
			expectOk: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			_, _, backend := startTestBackend(t)
			backend.relay.capellaEpoch = 1
			backend.relay.denebEpoch = 2
			headSlot := testSlot - 1
			w := httptest.NewRecorder()
			logger := logrus.New()
			log := logrus.NewEntry(logger)
			submission, err := common.GetBlockSubmissionInfo(tc.payload)
			require.NoError(t, err)
			ok := backend.relay.checkSubmissionSlotDetails(w, log, headSlot, tc.payload, submission)
			require.Equal(t, tc.expectOk, ok)
		})
	}
}

func TestCheckBuilderEntry(t *testing.T) {
	builderPubkey, err := utils.HexToPubkey(testBuilderPubkey)
	require.NoError(t, err)
	diffPubkey := builderPubkey
	diffPubkey[0] = 0xff
	cases := []struct {
		description string
		entry       *blockBuilderCacheEntry
		pk          phase0.BLSPubKey
		expectOk    bool
	}{
		{
			description: "success",
			entry: &blockBuilderCacheEntry{
				status: common.BuilderStatus{
					IsHighPrio: true,
				},
			},
			pk:       builderPubkey,
			expectOk: true,
		},
		{
			description: "failure_blacklisted",
			entry: &blockBuilderCacheEntry{
				status: common.BuilderStatus{
					IsBlacklisted: true, // set blacklisted to true to cause failure
				},
			},
			pk:       builderPubkey,
			expectOk: false,
		},
		{
			description: "failure_low_prio",
			entry: &blockBuilderCacheEntry{
				status: common.BuilderStatus{
					IsHighPrio: false, // set low-prio to cause failure
				},
			},
			pk:       builderPubkey,
			expectOk: false,
		},
		{
			description: "failure_nil_entry_low_prio",
			entry:       nil,
			pk:          diffPubkey, // set to different pubkey, so no entry is found
			expectOk:    false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			_, _, backend := startTestBackend(t)
			backend.relay.blockBuildersCache[tc.pk.String()] = tc.entry
			backend.relay.ffDisableLowPrioBuilders = true
			w := httptest.NewRecorder()
			logger := logrus.New()
			log := logrus.NewEntry(logger)
			_, ok := backend.relay.checkBuilderEntry(w, log, builderPubkey)
			require.Equal(t, tc.expectOk, ok)
		})
	}
}

func TestCheckFloorBidValue(t *testing.T) {
	cases := []struct {
		description          string
		payload              *common.VersionedSubmitBlockRequest
		cancellationsEnabled bool
		floorValue           string
		expectOk             bool
	}{
		{
			description: "success",
			payload: &common.VersionedSubmitBlockRequest{
				spec.VersionedSubmitBlockRequest{
					Version: consensusspec.DataVersionCapella,
					Capella: &builderCapella.SubmitBlockRequest{
						Message: &apiv1.BidTrace{
							Slot:  testSlot,
							Value: uint256.NewInt(1),
						},
					},
				},
			},
			expectOk: true,
		},
		{
			description: "failure_slot_already_delivered",
			payload: &common.VersionedSubmitBlockRequest{
				spec.VersionedSubmitBlockRequest{
					Version: consensusspec.DataVersionCapella,
					Capella: &builderCapella.SubmitBlockRequest{
						Message: &apiv1.BidTrace{
							Slot: 0,
						},
					},
				},
			},
			expectOk: false,
		},
		{
			description: "failure_cancellations_below_floor",
			payload: &common.VersionedSubmitBlockRequest{
				spec.VersionedSubmitBlockRequest{
					Version: consensusspec.DataVersionCapella,
					Capella: &builderCapella.SubmitBlockRequest{
						Message: &apiv1.BidTrace{
							Slot:  testSlot,
							Value: uint256.NewInt(1),
						},
					},
				},
			},
			expectOk:             false,
			cancellationsEnabled: true,
			floorValue:           "2",
		},
		{
			description: "failure_no_cancellations_at_floor",
			payload: &common.VersionedSubmitBlockRequest{
				spec.VersionedSubmitBlockRequest{
					Version: consensusspec.DataVersionCapella,
					Capella: &builderCapella.SubmitBlockRequest{
						Message: &apiv1.BidTrace{
							Slot:  testSlot,
							Value: uint256.NewInt(0),
						},
					},
				},
			},
			expectOk: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			_, _, backend := startTestBackend(t)
			submission, err := common.GetBlockSubmissionInfo(tc.payload)
			require.NoError(t, err)
			err = backend.redis.SetFloorBidValue(submission.Slot, submission.ParentHash.String(), submission.Proposer.String(), tc.floorValue)
			require.Nil(t, err)

			w := httptest.NewRecorder()
			logger := logrus.New()
			log := logrus.NewEntry(logger)
			tx := backend.redis.NewTxPipeline()
			simResultC := make(chan *blockSimResult, 1)
			bfOpts := bidFloorOpts{
				w:                    w,
				tx:                   tx,
				log:                  log,
				cancellationsEnabled: tc.cancellationsEnabled,
				simResultC:           simResultC,
				payload:              tc.payload,
			}
			floor, ok := backend.relay.checkFloorBidValue(bfOpts)
			require.Equal(t, tc.expectOk, ok)
			if ok {
				require.NotNil(t, floor)
				require.NotNil(t, log)
			}
		})
	}
}

func TestUpdateRedis(t *testing.T) {
	cases := []struct {
		description          string
		cancellationsEnabled bool
		floorValue           string
		payload              *common.VersionedSubmitBlockRequest
		expectOk             bool
	}{
		{
			description: "success",
			floorValue:  "10",
			payload: &common.VersionedSubmitBlockRequest{
				spec.VersionedSubmitBlockRequest{
					Version: consensusspec.DataVersionCapella,
					Capella: &builderCapella.SubmitBlockRequest{
						Message: &apiv1.BidTrace{
							Slot:  testSlot,
							Value: uint256.NewInt(1),
						},
						ExecutionPayload: &capella.ExecutionPayload{},
					},
				},
			},
			expectOk: true,
		},
		{
			description: "failure_no_payload",
			floorValue:  "10",
			payload:     nil,
			expectOk:    false,
		},
		{
			description: "failure_encode_failure_too_long_extra_data",
			floorValue:  "10",
			payload: &common.VersionedSubmitBlockRequest{
				spec.VersionedSubmitBlockRequest{
					Version: consensusspec.DataVersionCapella,
					Capella: &builderCapella.SubmitBlockRequest{
						Message: &apiv1.BidTrace{
							Slot:  testSlot,
							Value: uint256.NewInt(1),
						},
						ExecutionPayload: &capella.ExecutionPayload{
							ExtraData: make([]byte, 33), // Max extra data length is 32.
						},
					},
				},
			},
			expectOk: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			_, _, backend := startTestBackend(t)
			w := httptest.NewRecorder()
			logger := logrus.New()
			log := logrus.NewEntry(logger)
			tx := backend.redis.NewTxPipeline()

			floorValue := new(big.Int)
			floorValue, ok := floorValue.SetString(tc.floorValue, 10)
			require.True(t, ok)
			rOpts := redisUpdateBidOpts{
				w:                    w,
				tx:                   tx,
				log:                  log,
				cancellationsEnabled: tc.cancellationsEnabled,
				floorBidValue:        floorValue,
				payload:              tc.payload,
			}
			updateResp, getPayloadResp, ok := backend.relay.updateRedisBid(rOpts)
			require.Equal(t, tc.expectOk, ok)
			if ok {
				require.NotNil(t, updateResp)
				require.NotNil(t, getPayloadResp)
			}
		})
	}
}

func TestCheckProposerSignature(t *testing.T) {
	t.Run("Unsupported version", func(t *testing.T) {
		_, _, backend := startTestBackend(t)
		payload := new(common.VersionedSignedBlindedBlockRequest)
		payload.Version = consensusspec.DataVersionBellatrix
		ok, err := backend.relay.checkProposerSignature(payload, []byte{})
		require.Error(t, err, "unsupported consensus data version")
		require.False(t, ok)
	})

	t.Run("Valid Capella Signature", func(t *testing.T) {
		jsonBytes := common.LoadGzippedBytes(t, "../../testdata/signedBlindedBeaconBlock_Goerli.json.gz")
		payload := new(common.VersionedSignedBlindedBlockRequest)
		err := json.Unmarshal(jsonBytes, payload)
		require.NoError(t, err)
		// start backend with goerli network
		_, _, backend := startTestBackend(t)
		goerli, err := common.NewEthNetworkDetails(common.EthNetworkGoerli)
		require.NoError(t, err)
		backend.relay.opts.EthNetDetails = *goerli
		// check signature
		pubkey, err := utils.HexToPubkey("0xa8afcb5313602f936864b30600f568e04069e596ceed9b55e2a1c872c959ddcb90589636469c15d97e7565344d9ed4ad")
		require.NoError(t, err)
		ok, err := backend.relay.checkProposerSignature(payload, pubkey[:])
		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("Invalid Capella Signature", func(t *testing.T) {
		jsonBytes := common.LoadGzippedBytes(t, "../../testdata/signedBlindedBeaconBlock_Goerli.json.gz")
		payload := new(common.VersionedSignedBlindedBlockRequest)
		err := json.Unmarshal(jsonBytes, payload)
		require.NoError(t, err)
		// change signature
		signature, err := utils.HexToSignature(
			"0x942d85822e86a182b0a535361b379015a03e5ce4416863d3baa46b42eef06f070462742b79fbc77c0802699ba6d2ab00" +
				"11740dad6bfcf05b1f15c5a11687ae2aa6a08c03ad1ff749d7a48e953d13b5d7c2bd1da4cfcf30ba6d918b587d6525f0",
		)
		require.NoError(t, err)
		payload.Capella.Signature = signature
		// start backend with goerli network
		_, _, backend := startTestBackend(t)
		goerli, err := common.NewEthNetworkDetails(common.EthNetworkGoerli)
		require.NoError(t, err)
		backend.relay.opts.EthNetDetails = *goerli
		// check signature
		pubkey, err := utils.HexToPubkey("0xa8afcb5313602f936864b30600f568e04069e596ceed9b55e2a1c872c959ddcb90589636469c15d97e7565344d9ed4ad")
		require.NoError(t, err)
		ok, err := backend.relay.checkProposerSignature(payload, pubkey[:])
		require.NoError(t, err)
		require.False(t, ok)
	})
}

func gzipBytes(t *testing.T, b []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(b)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}
