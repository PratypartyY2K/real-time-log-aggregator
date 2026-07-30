package main

import (
	"testing"
)

func TestVectorLiteral(t *testing.T) {
	if got := vectorLiteral([]float64{0.5, -1, 2.25}); got != "[0.5,-1,2.25]" {
		t.Fatalf("vectorLiteral() = %q", got)
	}
}
