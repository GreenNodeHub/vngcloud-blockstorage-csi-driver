package internal

import (
	lctx "context"
)

// Semaphore caps how many CreateVolume operations this process runs against
// vServer at once.
//
// Why it exists: the TS-B experiment of 30/08/2026 (same 100-PVC storm, only
// csi-provisioner worker-threads changed 100 -> 10) showed vServer chokes on
// CONCURRENT volume creates, not on request rate - 10 workers finished 31%
// faster with 92% fewer vServer 500s. The driver's rate limiter cannot express
// this: a token bucket paces how fast requests LEAVE, while ~100 slow creates
// can still be waiting on vServer at once. Capping in-flight creates here
// keeps the protection independent of how the CO happens to be configured.
//
// Deliberately a blocking gate, not fail-fast like the rate limiter's shed:
// a waiting handler costs one parked goroutine and produces zero retry churn,
// and Acquire ends with the request context, so the sidecar's own timeout
// still bounds the wait.
type Semaphore struct {
	slots chan struct{}
}

func NewSemaphore(pcap int) *Semaphore {
	return &Semaphore{slots: make(chan struct{}, pcap)}
}

// Acquire takes a slot, waiting until one frees up or pctx ends.
func (s *Semaphore) Acquire(pctx lctx.Context) error {
	select {
	case s.slots <- struct{}{}:
		return nil
	case <-pctx.Done():
		return pctx.Err()
	}
}

// Release frees a slot taken by a successful Acquire.
func (s *Semaphore) Release() {
	<-s.slots
}
