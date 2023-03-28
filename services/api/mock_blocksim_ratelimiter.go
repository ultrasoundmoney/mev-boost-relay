package api

import (
	"context"
)

type MockBlockSimulationRateLimiter struct {
	errs []error
	idx  int
}

func (m *MockBlockSimulationRateLimiter) send(context context.Context, payload *BuilderBlockValidationRequest, isHighPrio bool) error {
	err := m.errs[m.idx]
	m.idx += 1
	return err
}

func (m *MockBlockSimulationRateLimiter) currentCounter() int64 {
	return 0
}
