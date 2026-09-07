package metrics

import (
	ltesting "testing"
)

// Every metric this recorder registers publishes a HELP string, and that string
// is what an operator reads in a dashboard or an alert. Until this change the
// counters and histograms all published the literal
//
//	ebs_csi_aws_com metric
//
// which was copied from aws-ebs-csi-driver and says nothing - and names the
// wrong cloud in a VngCloud driver. Read off the live dev cluster on
// 07/09/2026, both operator-facing counters this feature added looked like:
//
//	# HELP vcontainer_csi_detach_breaker_trips_total [ALPHA] ebs_csi_aws_com metric
//	# HELP vcontainer_csi_iaas_errors_total [ALPHA] ebs_csi_aws_com metric
//
// The gauge had a real help string, but only because SetGauge hard-coded the
// detach-pending wording for ANY gauge name - so the second gauge to exist
// would have been mislabelled with the first one's description. Both problems
// have the same fix: the caller states the help, because only the caller knows
// what the metric means.
func TestRegisteredMetricsCarryTheCallersHelpString(t *ltesting.T) {
	r := InitializeRecorder()

	const (
		counterName = "vcontainer_csi_test_help_counter_total"
		counterHelp = "test counter help that only this call site could know"
		gaugeName   = "vcontainer_csi_test_help_gauge"
		gaugeHelp   = "test gauge help that only this call site could know"
		histName    = "vcontainer_csi_test_help_histogram"
		histHelp    = "test histogram help that only this call site could know"
	)

	r.IncreaseCount(counterName, counterHelp, map[string]string{"op": "detach"})
	r.SetGauge(gaugeName, gaugeHelp, 1, map[string]string{"volume_id": "vol-1"})
	r.ObserveHistogram(histName, histHelp, 0.5, map[string]string{"op": "create"}, []float64{1})

	for _, tc := range []struct{ name, want string }{
		{counterName, counterHelp},
		{gaugeName, gaugeHelp},
		{histName, histHelp},
	} {
		mf := gatherFamily(t, r, tc.name)
		if mf == nil {
			t.Fatalf("metric family %q not published", tc.name)
		}
		got := mf.GetHelp()
		// component-base prefixes "[ALPHA] " for an alpha metric, so match on
		// containment rather than equality.
		if !contains(got, tc.want) {
			t.Errorf("%s help = %q, want it to contain %q", tc.name, got, tc.want)
		}
		if contains(got, "ebs_csi_aws_com") {
			t.Errorf("%s help = %q, still carries the aws-ebs placeholder", tc.name, got)
		}
	}
}

func contains(phaystack, pneedle string) bool {
	if len(pneedle) > len(phaystack) {
		return false
	}
	for i := 0; i+len(pneedle) <= len(phaystack); i++ {
		if phaystack[i:i+len(pneedle)] == pneedle {
			return true
		}
	}

	return false
}
