package metrics

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Collector interface {
	WritePrometheus(*strings.Builder)
}

type Handler struct {
	service    string
	startedAt  time.Time
	collectors []Collector
}

func NewHandler(service string, collectors ...Collector) *Handler {
	return &Handler{
		service:    service,
		startedAt:  time.Now(),
		collectors: collectors,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	var body strings.Builder

	WriteMetricHelp(&body, "logagg_service_info", "Constant 1-valued service info metric.", "gauge")
	WriteMetricLine(&body, "logagg_service_info", map[string]string{"service": h.service}, "1")

	WriteMetricHelp(&body, "logagg_service_uptime_seconds", "Service uptime in seconds.", "gauge")
	WriteMetricLine(&body, "logagg_service_uptime_seconds", map[string]string{"service": h.service}, FormatFloat(time.Since(h.startedAt).Seconds()))

	for _, collector := range h.collectors {
		if collector != nil {
			collector.WritePrometheus(&body)
		}
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write([]byte(body.String()))
}

type HTTPCollector struct {
	service  string
	inflight atomic.Int64

	mu       sync.Mutex
	requests map[httpMetricKey]httpMetricValue
}

type httpMetricKey struct {
	Route  string
	Method string
	Code   int
}

type httpMetricValue struct {
	Count       uint64
	DurationSum float64
}

func NewHTTPCollector(service string) *HTTPCollector {
	return &HTTPCollector{
		service:  service,
		requests: map[httpMetricKey]httpMetricValue{},
	}
}

func (c *HTTPCollector) Middleware(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		c.inflight.Add(1)
		defer c.inflight.Add(-1)

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		c.observe(route, r.Method, rec.status, time.Since(start))
	})
}

func (c *HTTPCollector) observe(route, method string, code int, duration time.Duration) {
	route = strings.TrimSpace(route)
	if route == "" {
		route = "unknown"
	}

	key := httpMetricKey{
		Route:  route,
		Method: method,
		Code:   code,
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	value := c.requests[key]
	value.Count++
	value.DurationSum += duration.Seconds()
	c.requests[key] = value
}

func (c *HTTPCollector) WritePrometheus(body *strings.Builder) {
	if c == nil {
		return
	}

	WriteMetricHelp(body, "logagg_http_inflight_requests", "Current in-flight HTTP requests.", "gauge")
	WriteMetricLine(body, "logagg_http_inflight_requests", map[string]string{"service": c.service}, strconv.FormatInt(c.inflight.Load(), 10))

	WriteMetricHelp(body, "logagg_http_requests_total", "Total HTTP requests by route, method, and status code.", "counter")
	WriteMetricHelp(body, "logagg_http_request_duration_seconds_sum", "Cumulative HTTP request duration in seconds.", "counter")
	WriteMetricHelp(body, "logagg_http_request_duration_seconds_count", "Total observed HTTP requests for duration aggregation.", "counter")

	c.mu.Lock()
	defer c.mu.Unlock()

	for key, value := range c.requests {
		labels := map[string]string{
			"service": c.service,
			"route":   key.Route,
			"method":  key.Method,
			"code":    strconv.Itoa(key.Code),
		}
		WriteMetricLine(body, "logagg_http_requests_total", labels, strconv.FormatUint(value.Count, 10))
		WriteMetricLine(body, "logagg_http_request_duration_seconds_sum", labels, FormatFloat(value.DurationSum))
		WriteMetricLine(body, "logagg_http_request_duration_seconds_count", labels, strconv.FormatUint(value.Count, 10))
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func StartServer(ctx context.Context, addr string, handler http.Handler, onError func(error)) {
	if strings.TrimSpace(addr) == "" || handler == nil {
		return
	}

	server := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed && onError != nil {
			onError(err)
		}
	}()
}

func WriteMetricHelp(body *strings.Builder, name, help, metricType string) {
	body.WriteString("# HELP ")
	body.WriteString(name)
	body.WriteByte(' ')
	body.WriteString(help)
	body.WriteByte('\n')
	body.WriteString("# TYPE ")
	body.WriteString(name)
	body.WriteByte(' ')
	body.WriteString(metricType)
	body.WriteByte('\n')
}

func WriteMetricLine(body *strings.Builder, name string, labels map[string]string, value string) {
	body.WriteString(name)
	if len(labels) > 0 {
		body.WriteByte('{')
		first := true
		for key, labelValue := range labels {
			if !first {
				body.WriteByte(',')
			}
			first = false
			body.WriteString(key)
			body.WriteString(`="`)
			body.WriteString(EscapeLabelValue(labelValue))
			body.WriteByte('"')
		}
		body.WriteByte('}')
	}
	body.WriteByte(' ')
	body.WriteString(value)
	body.WriteByte('\n')
}

func EscapeLabelValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}

func FormatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}
