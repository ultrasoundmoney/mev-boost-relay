package datastore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	builderApi "github.com/attestantio/go-builder-client/api"
	builderApiDeneb "github.com/attestantio/go-builder-client/api/deneb"
	"github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/flashbots/mev-boost-relay/common"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var ErrFailedToParsePayload = errors.New("failed to parse payload")

func getPayloadContents(slot uint64, proposerPubkey, blockHash, host, basePath, authToken string, timeout time.Duration) (*builderApi.VersionedSubmitBlindedBlockResponse, error) {
	client := &http.Client{Timeout: timeout}

	queryParams := url.Values{}
	queryParams.Add("slot", fmt.Sprintf("%d", slot))
	queryParams.Add("proposer_pubkey", proposerPubkey)
	queryParams.Add("block_hash", blockHash)

	fullURL := fmt.Sprintf("%s/%s/payload_contents?%s", host, basePath, queryParams.Encode())
	req, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "auction host %s: failed to create payload contents request", host)
	}

	// Add auth token if provided
	if authToken != "" {
		req.Header.Add("x-auth-token", authToken)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "auction host %s: failed to fetch payload contents", host)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, errors.Wrapf(ErrExecutionPayloadNotFound, "auction host %s", host)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrapf(err, "auction host %s: failed to read payload contents response body", host)
	}

	// Try to parse pectra contents
	electraPayloadContents := new(builderApiDeneb.ExecutionPayloadAndBlobsBundle)
	err = electraPayloadContents.UnmarshalSSZ(body)

	if err == nil {
		return &builderApi.VersionedSubmitBlindedBlockResponse{
			Version: spec.DataVersionElectra,
			Electra: electraPayloadContents,
		}, nil
	}

	// Try to parse deneb contents
	denebPayloadContents := new(builderApiDeneb.ExecutionPayloadAndBlobsBundle)
	err = denebPayloadContents.UnmarshalSSZ(body)

	if err == nil {
		return &builderApi.VersionedSubmitBlindedBlockResponse{
			Version: spec.DataVersionDeneb,
			Deneb:   denebPayloadContents,
		}, nil
	}

	// Try to parse capella payload
	capellaPayload := new(capella.ExecutionPayload)
	err = capellaPayload.UnmarshalSSZ(body)

	if err == nil {
		return &builderApi.VersionedSubmitBlindedBlockResponse{
			Version: spec.DataVersionCapella,
			Capella: capellaPayload,
		}, nil
	}

	return nil, errors.Wrapf(ErrFailedToParsePayload, "auction host %s", host)
}

func (ds *Datastore) LocalPayloadContents(slot uint64, proposerPubkey, blockHash string) (*builderApi.VersionedSubmitBlindedBlockResponse, error) {
	return getPayloadContents(slot, proposerPubkey, blockHash, ds.localAuctionHost, "internal", "", 0)
}

func (ds *Datastore) RemotePayloadContents(log *logrus.Entry, slot uint64, proposerPubkey, blockHash string) (*builderApi.VersionedSubmitBlindedBlockResponse, error) {
	// Fan out to all remote auction hosts concurrently and return the first success.
	type res struct {
		payload *builderApi.VersionedSubmitBlindedBlockResponse
		err     error
	}

	if len(ds.remoteAuctionHosts) == 0 {
		return nil, ErrExecutionPayloadNotFound
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	results := make(chan res, len(ds.remoteAuctionHosts))
	var wg sync.WaitGroup
	for _, host := range ds.remoteAuctionHosts {
		h := host
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Short timeout per host to avoid blocking.
			payload, err := getPayloadContents(slot, proposerPubkey, blockHash, h, "private", ds.auctionAuthToken, 2*time.Second)
			if err != nil {
				if log != nil {
					log.WithError(err).WithField("remoteAuctionHost", h).Info("remote payload query error")
				}
			} else if payload != nil {
				if log != nil {
					log.WithField("remoteAuctionHost", h).Info("remote payload found")
				}
			} else {
				if log != nil {
					log.WithField("remoteAuctionHost", h).Info("remote payload unknown response")
				}
			}
			select {
			case results <- res{payload: payload, err: err}:
			case <-ctx.Done():
			}
		}()
	}

	// Close results when all workers are done.
	go func() {
		wg.Wait()
		close(results)
	}()

	var lastErr error
	for r := range results {
		if r.payload != nil {
			cancel()
			return r.payload, nil
		}
		if r.err != nil {
			lastErr = r.err
		}
	}
	if lastErr == nil {
		lastErr = ErrExecutionPayloadNotFound
	}
	return nil, lastErr
}

func getBidTrace(slot uint64, proposerPubkey, blockHash, auctionHost, basePath, authToken string) (*common.BidTraceV2WithBlobFields, error) {
	client := &http.Client{}

	queryParams := url.Values{}
	queryParams.Add("slot", fmt.Sprintf("%d", slot))
	queryParams.Add("proposer_pubkey", proposerPubkey)
	queryParams.Add("block_hash", blockHash)

	fullURL := fmt.Sprintf("%s/%s/bid_trace?%s", auctionHost, basePath, queryParams.Encode())
	req, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "auction host %s: failed to create bid trace request", auctionHost)
	}

	if authToken != "" {
		req.Header.Add("x-auth-token", authToken)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "auction host %s: failed to fetch bid trace", auctionHost)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, errors.Wrapf(ErrBidTraceNotFound, "auction host %s", auctionHost)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrapf(err, "auction host %s: failed to read bid trace response body", auctionHost)
	}

	bidtrace := new(common.BidTraceV2WithBlobFields)
	err = json.Unmarshal(body, &bidtrace)
	if err != nil {
		return nil, errors.Wrapf(err, "auction host %s: failed to decode bid trace JSON", auctionHost)
	}

	return bidtrace, nil
}

func (ds *Datastore) LocalBidTrace(slot uint64, proposerPubkey, blockHash string) (*common.BidTraceV2WithBlobFields, error) {
	return getBidTrace(slot, proposerPubkey, blockHash, ds.localAuctionHost, "internal", "")
}

func (ds *Datastore) RemoteBidTrace(log *logrus.Entry, slot uint64, proposerPubkey, blockHash string) (*common.BidTraceV2WithBlobFields, error) {
	// Query all remote hosts concurrently and return the first found.
	type res struct {
		bt  *common.BidTraceV2WithBlobFields
		err error
	}

	if len(ds.remoteAuctionHosts) == 0 {
		return nil, ErrBidTraceNotFound
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	results := make(chan res, len(ds.remoteAuctionHosts))
	var wg sync.WaitGroup
	for _, host := range ds.remoteAuctionHosts {
		h := host
		wg.Add(1)
		go func() {
			defer wg.Done()
			bt, err := getBidTrace(slot, proposerPubkey, blockHash, h, "private", ds.auctionAuthToken)
			if err != nil {
				if log != nil {
					log.WithError(err).WithField("remoteAuctionHost", h).Info("remote bidTrace query error")
				}
			} else if bt != nil {
				if log != nil {
					log.WithField("remoteAuctionHost", h).Info("remote bidTrace found")
				}
			} else {
				if log != nil {
					log.WithField("remoteAuctionHost", h).Info("remote bidTrace unknown response")
				}
			}
			select {
			case results <- res{bt: bt, err: err}:
			case <-ctx.Done():
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var lastErr error
	for r := range results {
		if r.bt != nil {
			cancel()
			return r.bt, nil
		}
		if r.err != nil {
			lastErr = r.err
		}
	}
	if lastErr == nil {
		lastErr = ErrBidTraceNotFound
	}
	return nil, lastErr
}
