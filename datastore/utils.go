package datastore

import (
	"context"
	"math/big"
)

// BuilderBids supports redis.SaveBidAndUpdateTopBid
type BuilderBids struct {
	bidValues map[string]*big.Int
}

func NewBuilderBidsFromRedis(r *RedisCache, slot uint64, parentHash, proposerPubkey string) (*BuilderBids, error) {
	keyBidValues := r.keyBlockBuilderLatestBidsValue(slot, parentHash, proposerPubkey)
	bidValueMap, err := r.client.HGetAll(context.Background(), keyBidValues).Result()
	if err != nil {
		return nil, err
	}
	return NewBuilderBids(bidValueMap), nil
}

func NewBuilderBids(bidValueMap map[string]string) *BuilderBids {
	b := BuilderBids{
		bidValues: make(map[string]*big.Int),
	}
	for builderPubkey, bidValue := range bidValueMap {
		b.bidValues[builderPubkey] = new(big.Int)
		b.bidValues[builderPubkey].SetString(bidValue, 10)
	}
	return &b
}

func (b *BuilderBids) getTopBid() (string, *big.Int) {
	topBidBuilderPubkey := ""
	topBidValue := big.NewInt(0)
	for builderPubkey, bidValue := range b.bidValues {
		if bidValue.Cmp(topBidValue) > 0 {
			topBidValue = bidValue
			topBidBuilderPubkey = builderPubkey
		}
	}
	return topBidBuilderPubkey, topBidValue
}
