package metrics

import (
	"net/http"
	"sync"
	"time"

	lprom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	ldto "github.com/prometheus/client_model/go"
	"k8s.io/component-base/metrics"
	"k8s.io/klog/v2"
)

var (
	r    *metricRecorder // singleton instance of metricRecorder
	once sync.Once
)

type metricRecorder struct {
	registry metrics.KubeRegistry
	metrics  map[string]interface{}
	mutex    sync.Mutex
}

// Recorder returns the singleton instance of metricRecorder.
// nil is returned if the recorder is not initialized.
func Recorder() *metricRecorder {
	return r
}

// InitializeRecorder initializes a new metricRecorder instance if it hasn't been initialized.
func InitializeRecorder() *metricRecorder {
	once.Do(func() {
		r = &metricRecorder{
			registry: metrics.NewKubeRegistry(),
			metrics:  make(map[string]interface{}),
		}
	})
	return r
}

// IncreaseCount increases the counter metric by 1, registering it on first use
// with the help string the caller supplies.
//
// help is a parameter rather than a default because only the caller knows what
// the metric means. These methods used to register every counter and histogram
// with the literal "ebs_csi_aws_com metric", copied from aws-ebs-csi-driver, and
// the dev cluster duly published that as the description of both operator-facing
// counters this driver added - naming the wrong cloud and explaining nothing.
func (m *metricRecorder) IncreaseCount(name, help string, labels map[string]string) {
	if m == nil {
		return // recorder is not initialized
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	metric, ok := m.metrics[name]
	if !ok {
		klog.V(4).InfoS("Metric not found, registering", "name", name, "labels", labels)
		m.registerCounterVec(name, help, getLabelNames(labels))
		metric = m.metrics[name]
	}

	metric.(*metrics.CounterVec).With(metrics.Labels(labels)).Inc()
}

// ObserveHistogram records the given value in the histogram metric, registering
// it on first use with the caller's help string. See IncreaseCount on why help
// is a parameter.
func (m *metricRecorder) ObserveHistogram(name, help string, value float64, labels map[string]string, buckets []float64) {
	if m == nil {
		return // recorder is not initialized
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	metric, ok := m.metrics[name]
	if !ok {
		klog.V(4).InfoS("Metric not found, registering", "name", name, "labels", labels, "buckets", buckets)
		m.registerHistogramVec(name, help, getLabelNames(labels), buckets)
		metric = m.metrics[name]
	}

	metric.(*metrics.HistogramVec).With(metrics.Labels(labels)).Observe(value)
}

// SetGauge publishes an absolute value for one label set, registering the
// metric on first use with the caller's help string. Unlike the counters here,
// a gauge must be able to go down and to disappear - see DeleteGauge.
//
// help used to be hard-coded to the detach-pending wording for ANY gauge name,
// which happened to be right because exactly one gauge existed. The second one
// would have been published under the first one's description.
func (m *metricRecorder) SetGauge(name, help string, value float64, labels map[string]string) {
	if m == nil {
		return // recorder is not initialized
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	metric, ok := m.metrics[name]
	if !ok {
		klog.V(4).InfoS("Metric not found, registering", "name", name, "labels", labels)
		m.registerGaugeVec(name, help, getLabelNames(labels))
		metric = m.metrics[name]
	}

	metric.(*metrics.GaugeVec).With(metrics.Labels(labels)).Set(value)
}

// DeleteGauge drops one series. Needed because a recovered volume must stop
// reporting "stuck for N seconds" - a stale series would alert forever.
func (m *metricRecorder) DeleteGauge(name string, labels map[string]string) {
	if m == nil {
		return // recorder is not initialized
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	metric, ok := m.metrics[name]
	if !ok {
		return
	}

	metric.(*metrics.GaugeVec).Delete(metrics.Labels(labels))
}

// AddGauge moves a gauge by a delta, registering it on first use. Needed for
// the in-flight gauge: two concurrent RPCs must each be counted, which Set
// cannot express without the caller keeping its own counter and racing on it.
func (m *metricRecorder) AddGauge(name, help string, delta float64, labels map[string]string) {
	if m == nil {
		return // recorder is not initialized
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	metric, ok := m.metrics[name]
	if !ok {
		klog.V(4).InfoS("Metric not found, registering", "name", name, "labels", labels)
		m.registerGaugeVec(name, help, getLabelNames(labels))
		metric = m.metrics[name]
	}

	metric.(*metrics.GaugeVec).With(metrics.Labels(labels)).Add(delta)
}

// The Initialize* methods create a series at its zero value without recording
// an observation. See driver.InitializeStartupMetrics for why that matters:
// a counter that does not exist reads as "no data" rather than 0, and an alert
// rule cannot fire on a series that is absent.
//
// They are idempotent, and safe to call for a metric that already exists - the
// underlying With() returns the existing child.

func (m *metricRecorder) InitializeCounter(name, help string, labels map[string]string) {
	if m == nil {
		return // recorder is not initialized
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	metric, ok := m.metrics[name]
	if !ok {
		m.registerCounterVec(name, help, getLabelNames(labels))
		metric = m.metrics[name]
	}

	// Add(0) rather than Inc(): this creates the child series and leaves it at
	// zero, which is the whole point.
	metric.(*metrics.CounterVec).With(metrics.Labels(labels)).Add(0)
}

func (m *metricRecorder) InitializeGauge(name, help string, labels map[string]string) {
	if m == nil {
		return // recorder is not initialized
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	metric, ok := m.metrics[name]
	if !ok {
		m.registerGaugeVec(name, help, getLabelNames(labels))
		metric = m.metrics[name]
	}

	metric.(*metrics.GaugeVec).With(metrics.Labels(labels)).Add(0)
}

func (m *metricRecorder) InitializeHistogram(name, help string, labels map[string]string, buckets []float64) {
	if m == nil {
		return // recorder is not initialized
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, ok := m.metrics[name]; !ok {
		m.registerHistogramVec(name, help, getLabelNames(labels), buckets)
	}

	// A HistogramVec child cannot be created without observing, and observing
	// would put a fake value in the bucket. GetMetricWith creates the child
	// with zero observations, which is exactly what is wanted here.
	if hv, ok := m.metrics[name].(*metrics.HistogramVec); ok {
		_, _ = hv.GetMetricWith(lprom.Labels(labels))
	}
}

// RegisterRuntimeCollectors publishes go_* and process_*.
//
// NewKubeRegistry builds a bare prometheus.NewRegistry(), which - unlike
// legacyregistry - registers no runtime collectors, so without this call the
// endpoint carries no goroutine count and no RSS. Both have been the deciding
// evidence in past incidents on other components in this fleet (a goroutine
// leak from an early-returned range, and a per-reconcile cache allocation),
// and neither is visible from CSI-level metrics alone.
func (m *metricRecorder) RegisterRuntimeCollectors() {
	if m == nil {
		return // recorder is not initialized
	}

	m.registry.RawMustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
}

// Gather returns what this recorder currently publishes.
//
// A thin passthrough to the registry - the same call the HTTP handler makes -
// exported so that code outside this package can assert on what it published
// without reaching into the registry field. Not on any hot path.
func (m *metricRecorder) Gather() ([]*ldto.MetricFamily, error) {
	if m == nil {
		return nil, nil // recorder is not initialized
	}

	return m.registry.Gather()
}

// InitializeMetricsHandler starts a new HTTP server to expose the metrics.
func (m *metricRecorder) InitializeMetricsHandler(address, path string) {
	if m == nil {
		klog.InfoS("InitializeMetricsHandler: metric recorder is not initialized")
		return
	}

	mux := http.NewServeMux()
	mux.Handle(path, metrics.HandlerFor(
		m.registry,
		metrics.HandlerOpts{
			ErrorHandling: metrics.ContinueOnError,
		}))

	server := &http.Server{
		Addr:        address,
		Handler:     mux,
		ReadTimeout: 3 * time.Second,
	}

	go func() {
		klog.InfoS("Metric server listening", "address", address, "path", path)

		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			klog.ErrorS(err, "Failed to start metric server", "address", address, "path", path)
			klog.FlushAndExit(klog.ExitFlushTimeout, 1)
		}
	}()
}

func (m *metricRecorder) registerHistogramVec(name, help string, labels []string, buckets []float64) {
	if _, exists := m.metrics[name]; exists {
		return
	}
	histogram := createHistogramVec(name, help, labels, buckets)
	m.metrics[name] = histogram
	m.registry.MustRegister(histogram)
}

func (m *metricRecorder) registerCounterVec(name, help string, labels []string) {
	if _, exists := m.metrics[name]; exists {
		return
	}
	counter := createCounterVec(name, help, labels)
	m.metrics[name] = counter
	m.registry.MustRegister(counter)
}

func (m *metricRecorder) registerGaugeVec(name, help string, labels []string) {
	if _, exists := m.metrics[name]; exists {
		return
	}
	gauge := createGaugeVec(name, help, labels)
	m.metrics[name] = gauge
	m.registry.MustRegister(gauge)
}

func createGaugeVec(name, help string, labels []string) *metrics.GaugeVec {
	return metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Name:           name,
			Help:           help,
			StabilityLevel: metrics.ALPHA,
		},
		labels,
	)
}

func createHistogramVec(name, help string, labels []string, buckets []float64) *metrics.HistogramVec {
	opts := &metrics.HistogramOpts{
		Name:           name,
		Help:           help,
		StabilityLevel: metrics.ALPHA,
		Buckets:        buckets,
	}
	return metrics.NewHistogramVec(opts, labels)
}

func createCounterVec(name, help string, labels []string) *metrics.CounterVec {
	return metrics.NewCounterVec(
		&metrics.CounterOpts{
			Name:           name,
			Help:           help,
			StabilityLevel: metrics.ALPHA,
		},
		labels,
	)
}

func getLabelNames(labels map[string]string) []string {
	names := make([]string, 0, len(labels))
	for n := range labels {
		names = append(names, n)
	}
	return names
}
