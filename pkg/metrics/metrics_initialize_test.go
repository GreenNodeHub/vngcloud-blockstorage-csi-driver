package metrics

import (
	lsync "sync"
	ltesting "testing"
)

// A histogram child cannot be created by observing without putting a value in
// a bucket, so InitializeHistogram uses GetMetricWith. If it ever went back to
// Observe(0), every pre-created series would ship a fake zero sample and every
// latency quantile would be wrong from the first scrape.
func TestInitializeHistogramCreatesTheSeriesWithoutObserving(t *ltesting.T) {
	r := InitializeRecorder()

	const name = "vks_csi_test_init_histogram"
	labels := map[string]string{"route": "volumes"}

	r.InitializeHistogram(name, "test histogram help", labels, []float64{1, 2})

	mf := gatherFamily(t, r, name)
	if mf == nil {
		t.Fatalf("%s was not published", name)
	}
	if len(mf.GetMetric()) != 1 {
		t.Fatalf("%s has %d series, want 1", name, len(mf.GetMetric()))
	}
	if got := mf.GetMetric()[0].GetHistogram().GetSampleCount(); got != 0 {
		t.Errorf("pre-created histogram has %d samples, want 0", got)
	}
	if got := mf.GetMetric()[0].GetHistogram().GetSampleSum(); got != 0 {
		t.Errorf("pre-created histogram sums to %v, want 0", got)
	}
}

// The point of pre-creating a counter is that it reads 0 rather than being
// absent. Add(0), not Inc().
func TestInitializeCounterCreatesTheSeriesAtZero(t *ltesting.T) {
	r := InitializeRecorder()

	const name = "vks_csi_test_init_counter_total"
	labels := map[string]string{"op": "detach", "reason": "IaaSUnreachable"}

	r.InitializeCounter(name, "test counter help", labels)

	mf := gatherFamily(t, r, name)
	if mf == nil {
		t.Fatalf("%s was not published", name)
	}
	if got := mf.GetMetric()[0].GetCounter().GetValue(); got != 0 {
		t.Errorf("pre-created counter = %v, want 0", got)
	}

	// And it must not reset a series that has since counted something.
	r.IncreaseCount(name, "test counter help", labels)
	r.InitializeCounter(name, "test counter help", labels)

	mf = gatherFamily(t, r, name)
	if got := mf.GetMetric()[0].GetCounter().GetValue(); got != 1 {
		t.Errorf("counter = %v after re-initialising, want the recorded 1", got)
	}
}

// AddGauge exists because the in-flight gauge is moved concurrently by every
// RPC handler. SetGauge could not express that without the caller keeping its
// own count and racing on it.
func TestAddGaugeIsCorrectUnderConcurrency(t *ltesting.T) {
	r := InitializeRecorder()

	const (
		name = "vks_csi_test_add_gauge"
		n    = 50
	)
	labels := map[string]string{"op": "CreateVolume"}

	var wg lsync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.AddGauge(name, "test gauge help", 1, labels)
		}()
	}
	wg.Wait()

	if got := gaugeValue(t, r, name, labels); got != n {
		t.Fatalf("gauge = %v after %d concurrent increments, want %d", got, n, n)
	}

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.AddGauge(name, "test gauge help", -1, labels)
		}()
	}
	wg.Wait()

	if got := gaugeValue(t, r, name, labels); got != 0 {
		t.Fatalf("gauge = %v after releasing every increment, want 0", got)
	}
}

// go_* and process_* are not registered by NewKubeRegistry, so their presence
// proves the explicit registration ran.
func TestRegisterRuntimeCollectorsPublishesGoAndProcessMetrics(t *ltesting.T) {
	r := InitializeRecorder()
	r.RegisterRuntimeCollectors()

	for _, name := range []string{"go_goroutines", "process_resident_memory_bytes"} {
		if gatherFamily(t, r, name) == nil {
			t.Errorf("%s is not published", name)
		}
	}
}
