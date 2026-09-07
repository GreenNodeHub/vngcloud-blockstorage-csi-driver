package metrics

import (
	"net/http"
	"sync"
	"time"

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

type VContainerMetrics struct {
	Duration *metrics.HistogramVec
	Total    *metrics.CounterVec
	Errors   *metrics.CounterVec
}

// MetricContext indicates the context for OpenStack metrics.
type MetricContext struct {
	Start      time.Time
	Attributes []string
	Metrics    *VContainerMetrics
}

// Observe records the request latency and counts the errors.
func (s *MetricContext) Observe(om *VContainerMetrics, err error) error {
	if om == nil {
		// mc.RequestMetrics not set, ignore this request
		return nil
	}

	om.Duration.WithLabelValues(s.Attributes...).Observe(
		time.Since(s.Start).Seconds())
	om.Total.WithLabelValues(s.Attributes...).Inc()
	if err != nil {
		om.Errors.WithLabelValues(s.Attributes...).Inc()
	}
	return err
}

// ObserveRequest records the request latency and counts the errors.
func (s *MetricContext) ObserveRequest(err error) error {
	return s.Observe(APIRequestMetrics, err)
}

// ObserveReconcile records the request reconciliation duration
func (s *MetricContext) ObserveReconcile(err error) error {
	return s.Observe(vccmReconcileMetrics, err)
}

// NewMetricContext creates a new MetricContext.
func NewMetricContext(resource string, request string) *MetricContext {
	return &MetricContext{
		Start:      time.Now(),
		Attributes: []string{resource + "_" + request},
	}
}

func RegisterMetrics(component string) {
	doRegisterAPIMetrics()
	if component == "occm" {
		doRegisterOccmMetrics()
	}
}
