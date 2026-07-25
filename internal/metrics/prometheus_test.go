package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPCollectorPreservesCountersAndEmitsHistogramBuckets(t *testing.T) {
	collector := NewHTTPCollector("query-api")
	handler := collector.Middleware("/v1/logs", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/logs", nil))

	var output strings.Builder
	collector.WritePrometheus(&output)
	body := output.String()
	for _, metric := range []string{
		"logagg_http_requests_total",
		"logagg_http_request_duration_seconds_bucket",
		"logagg_http_request_duration_seconds_sum",
		"logagg_http_request_duration_seconds_count",
		`code="503"`,
		`le="+Inf"`,
	} {
		if !strings.Contains(body, metric) {
			t.Fatalf("expected %q in metrics output:\n%s", metric, body)
		}
	}
}
