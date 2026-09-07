package cloud

import (
	lsync "sync"
	ltime "time"

	lserr "github.com/vngcloud/vngcloud-blockstorage-csi-driver/pkg/cloud/errors"
)

// metaCache memoises a catalog lookup - the zone list, the volume types of a
// zone - for a short TTL.
//
// These describe a topology that changes on the timescale of a region rollout,
// but the CreateVolume path re-fetched them on every single call: the TS-B load
// test on 29/08/2026 measured 709 ListZones for 190 PVCs, roughly 3.7 per
// volume. That traffic is what pushed the driver past the vServer quota, and a
// 429 costs far more than a stale zone list ever could: the adaptive limiter
// halves the rate, and climbing back from the floor takes 95s.
//
// Failures are never cached - one unlucky lookup must not become a lasting
// outage - and concurrent callers of a cold cache share a single fetch, which
// is the whole point at worker-threads=100.
// metaCacheTTL: short enough that a genuine topology change is picked up within
// a couple of minutes without anyone restarting the driver, long enough that a
// provisioning storm makes each lookup once rather than once per volume.
const metaCacheTTL = 5 * ltime.Minute

type metaCache[T any] struct {
	mu      lsync.Mutex
	ttl     ltime.Duration
	value   T
	loaded  bool
	expires ltime.Time
	loading *metaCacheCall[T]

	// permanent drops the expiry entirely - see newPermanentMetaCache.
	permanent bool
}

// metaCacheCall is one in-flight fetch that late callers wait on.
type metaCacheCall[T any] struct {
	done  chan struct{}
	value T
	err   lserr.IError
}

func newMetaCache[T any](pttl ltime.Duration) *metaCache[T] {
	return &metaCache[T]{ttl: pttl}
}

// newPermanentMetaCache memoises a value that cannot change for the life of
// the process, keeping the single-flight behaviour and the "never cache a
// failure" rule.
//
// Re-running the loader is not a cheap refresh here: get returns the loader's
// error, so an expiry lapsing while vServer is unreachable would fail calls
// that a value already in hand could have served.
func newPermanentMetaCache[T any]() *metaCache[T] {
	return &metaCache[T]{permanent: true}
}

// fresh reports whether the cached value may still be served. Callers hold mu.
func (s *metaCache[T]) fresh() bool {
	return s.loaded && (s.permanent || ltime.Now().Before(s.expires))
}

func (s *metaCache[T]) get(pload func() (T, lserr.IError)) (T, lserr.IError) {
	s.mu.Lock()

	if s.fresh() {
		value := s.value
		s.mu.Unlock()

		return value, nil
	}

	// Someone is already fetching: wait for that result instead of issuing a
	// request of our own.
	if call := s.loading; call != nil {
		s.mu.Unlock()
		<-call.done

		return call.value, call.err
	}

	call := &metaCacheCall[T]{done: make(chan struct{})}
	s.loading = call
	s.mu.Unlock()

	call.value, call.err = pload()

	s.mu.Lock()
	s.loading = nil
	if call.err == nil {
		s.value = call.value
		s.loaded = true
		s.expires = ltime.Now().Add(s.ttl)
	}
	s.mu.Unlock()

	close(call.done)

	return call.value, call.err
}

// keyedMetaCache is a metaCache per lookup key, for catalog calls that take
// arguments - the volume type of a given zone, say.
type keyedMetaCache[T any] struct {
	mu      lsync.Mutex
	ttl     ltime.Duration
	entries map[string]*metaCache[T]
}

func newKeyedMetaCache[T any](pttl ltime.Duration) *keyedMetaCache[T] {
	return &keyedMetaCache[T]{ttl: pttl, entries: make(map[string]*metaCache[T])}
}

func (s *keyedMetaCache[T]) get(pkey string, pload func() (T, lserr.IError)) (T, lserr.IError) {
	s.mu.Lock()
	entry := s.entries[pkey]
	if entry == nil {
		entry = newMetaCache[T](s.ttl)
		s.entries[pkey] = entry
	}
	s.mu.Unlock()

	return entry.get(pload)
}
