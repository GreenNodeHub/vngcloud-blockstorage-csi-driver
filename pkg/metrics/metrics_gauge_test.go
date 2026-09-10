package metrics

import (
	lsync "sync"
	ltesting "testing"

	ldto "github.com/prometheus/client_model/go"
)

func TestSetGaugeThenDelete(t *ltesting.T) {
	r := InitializeRecorder()
	labels := map[string]string{"volume_id": "vol-1", "node_id": "ins-1"}

	r.SetGauge("vks_csi_test_pending_seconds", "test gauge help", 42, labels)
	if got := gaugeValue(t, r, "vks_csi_test_pending_seconds", labels); got != 42 {
		t.Fatalf("gauge = %v, want 42", got)
	}

	// Overwriting the same series must replace, not accumulate.
	r.SetGauge("vks_csi_test_pending_seconds", "test gauge help", 100, labels)
	if got := gaugeValue(t, r, "vks_csi_test_pending_seconds", labels); got != 100 {
		t.Fatalf("gauge after re-set = %v, want 100", got)
	}

	// Deleting must remove the series, otherwise a recovered volume keeps
	// reporting a stale "stuck for N seconds" forever.
	r.DeleteGauge("vks_csi_test_pending_seconds", labels)
	if gaugeExists(t, r, "vks_csi_test_pending_seconds", labels) {
		t.Fatal("series still present after DeleteGauge")
	}
}

// SetGauge on an unknown metric name must register it once and not panic when
// called again.
func TestSetGaugeIsIdempotentOnRegistration(t *ltesting.T) {
	r := InitializeRecorder()
	labels := map[string]string{"volume_id": "vol-1", "node_id": "ins-1"}

	r.SetGauge("vks_csi_test_twice", "test gauge help", 1, labels)
	r.SetGauge("vks_csi_test_twice", "test gauge help", 2, labels)

	if got := gaugeValue(t, r, "vks_csi_test_twice", labels); got != 2 {
		t.Fatalf("gauge = %v, want 2", got)
	}
}

// Several goroutines calling SetGauge concurrently for a metric name that
// does not exist yet must not race on first-use registration and must not
// panic with AlreadyRegisteredError from a double MustRegister. Run with
// -race.
func TestSetGaugeConcurrentFirstUse(t *ltesting.T) {
	r := InitializeRecorder()
	const name = "vks_csi_test_concurrent_first_use"
	const n = 50
	labels := map[string]string{"volume_id": "vol-1", "node_id": "ins-1"}

	var wg lsync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			r.SetGauge(name, "test gauge help", float64(i), labels)
		}(i)
	}
	wg.Wait()

	got := gaugeValue(t, r, name, labels)
	if got < 0 || got >= n {
		t.Fatalf("gauge = %v, want a value written by one of the %d goroutines (in [0, %d))", got, n, n)
	}
}

func gaugeValue(t *ltesting.T, r *metricRecorder, pname string, plabels map[string]string) float64 {
	t.Helper()

	mf := gatherFamily(t, r, pname)
	if mf == nil {
		t.Fatalf("metric family %q not published", pname)
	}
	for _, m := range mf.GetMetric() {
		if labelsMatch(m, plabels) {
			return m.GetGauge().GetValue()
		}
	}
	t.Fatalf("no series in %q matching %v", pname, plabels)

	return 0
}

func gaugeExists(t *ltesting.T, r *metricRecorder, pname string, plabels map[string]string) bool {
	t.Helper()

	mf := gatherFamily(t, r, pname)
	if mf == nil {
		return false
	}
	for _, m := range mf.GetMetric() {
		if labelsMatch(m, plabels) {
			return true
		}
	}

	return false
}

func gatherFamily(t *ltesting.T, r *metricRecorder, pname string) *ldto.MetricFamily {
	t.Helper()

	families, err := r.registry.Gather()
	if err != nil {
		t.Fatalf("registry.Gather() error = %v", err)
	}
	for _, f := range families {
		if f.GetName() == pname {
			return f
		}
	}

	return nil
}

func labelsMatch(pm *ldto.Metric, plabels map[string]string) bool {
	if len(pm.GetLabel()) != len(plabels) {
		return false
	}
	for _, l := range pm.GetLabel() {
		if plabels[l.GetName()] != l.GetValue() {
			return false
		}
	}

	return true
}
