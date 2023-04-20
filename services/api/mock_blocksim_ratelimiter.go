package api

import (
	"context"

	"github.com/flashbots/mev-boost-relay/common"
)

type MockBlockSimulationRateLimiter struct {
	simulationError error
}

func (m *MockBlockSimulationRateLimiter) send(context context.Context, payload *common.BuilderBlockValidationRequest, isHighPrio bool) error {
	return m.simulationError
}

func (m *MockBlockSimulationRateLimiter) currentCounter() int64 {
	return 0
}
