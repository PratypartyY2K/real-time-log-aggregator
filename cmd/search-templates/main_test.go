package main

import (
	"math"
	"testing"
)

func TestCosineRanksSameDirectionHighest(t *testing.T) {
	if same, other := cosine([]float64{1, 0}, []float64{2, 0}), cosine([]float64{1, 0}, []float64{0, 1}); math.Abs(same-1) > 1e-9 || other != 0 {
		t.Fatalf("same=%f other=%f", same, other)
	}
}
