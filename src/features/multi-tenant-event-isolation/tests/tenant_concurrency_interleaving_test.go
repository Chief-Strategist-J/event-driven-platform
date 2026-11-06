package tests

import (
	"fmt"
	"sync"
	"testing"

	"event-driven-platform/src/features/multi-tenant-event-isolation/contracts"
	"event-driven-platform/src/features/multi-tenant-event-isolation/domain/rules"
)

func TestTenantConcurrency_InterleavedLanesZeroBleed(t *testing.T) {
	const workerCount = 10
	const iterationsPerWorker = 100

	var wg sync.WaitGroup
	errCh := make(chan error, workerCount*iterationsPerWorker)

	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		workerID := w
		go func() {
			defer wg.Done()
			for i := 0; i < iterationsPerWorker; i++ {
				tenantID := fmt.Sprintf("tenant_lane_%d", (workerID+i)%5)
				allowedRegions := []string{"us-east-1", "eu-west-1"}

				env := contracts.VerifyEnvelopeInput{
					HeaderTenant:  tenantID,
					KeyPrefix:     tenantID,
					PayloadTenant: tenantID,
					CurrentRegion: "us-east-1",
				}

				if err := rules.ValidateEnvelopeConsistency(env); err != nil {
					errCh <- fmt.Errorf("worker %d iter %d: envelope mismatch: %w", workerID, i, err)
					return
				}

				if err := rules.ValidateResidency(allowedRegions, "us-east-1"); err != nil {
					errCh <- fmt.Errorf("worker %d iter %d: residency failure: %w", workerID, i, err)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrency interleaving error: %v", err)
	}
}
