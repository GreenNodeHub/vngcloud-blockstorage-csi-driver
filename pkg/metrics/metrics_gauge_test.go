package metrics

import (
	ltesting "testing"

	ldto "github.com/prometheus/client_model/go"
)

func TestSetGaugeThenDelete(t *ltesting.T) {
	r := InitializeRecorder()
	labels := map[string]string{"volume_id": "vol-1", "node_id": "ins-1"}

	r.SetGauge("vcontainer_csi_test_pending_seconds", 42, labels)
	if got := gaugeValue(t, r, "vcontainer_csi_test_pending_seconds", labels); got != 42 {
		t.Fatalf("gauge = %v, want 42", got)
	}

	// Overwriting the same series must replace, not accumulate.
	r.SetGauge("vcontainer_csi_test_pending_seconds", 100, labels)
	if got := gaugeValue(t, r, "vcontainer_csi_test_pending_seconds", labels); got != 100 {
		t.Fatalf("gauge after re-set = %v, want 100", got)
	}

	// Deleting must remove the series, otherwise a recovered volume keeps
	// reporting a stale "stuck for N seconds" forever.
	r.DeleteGauge("vcontainer_csi_test_pending_seconds", labels)
	if gaugeExists(t, r, "vcontainer_csi_test_pending_seconds", labels) {
		t.Fatal("series still present after DeleteGauge")
	}
}

// SetGauge on an unknown metric name must register it once and not panic when
// called again.
func TestSetGaugeIsIdempotentOnRegistration(t *ltesting.T) {
	r := InitializeRecorder()
	labels := map[string]string{"volume_id": "vol-1", "node_id": "ins-1"}

	r.SetGauge("vcontainer_csi_test_twice", 1, labels)
	r.SetGauge("vcontainer_csi_test_twice", 2, labels)

	if got := gaugeValue(t, r, "vcontainer_csi_test_twice", labels); got != 2 {
		t.Fatalf("gauge = %v, want 2", got)
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
