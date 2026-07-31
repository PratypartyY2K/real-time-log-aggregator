package main

import (
	"testing"
	"time"
)

func TestBuildBatchesContainsCorrelatedCauseAndDistractors(t *testing.T) {
	const traceID = "rag-payment-test"
	batches := buildBatches(time.Date(2026, 7, 30, 14, 32, 0, 0, time.UTC), traceID)
	var incidentLogs, noiseLogs int
	for _, batch := range batches {
		for _, record := range batch.Logs {
			if record.Fields["trace_id"] == traceID {
				incidentLogs++
			} else {
				noiseLogs++
			}
		}
	}
	if len(batches) != 5 || incidentLogs < 8 || noiseLogs < 3 {
		t.Fatalf("got batches=%d incident_logs=%d noise_logs=%d", len(batches), incidentLogs, noiseLogs)
	}
}
