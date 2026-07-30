package processor

import "testing"

func TestExtractTemplateRemovesVariableValues(t *testing.T) {
	first, firstID := extractTemplate("database query 550e8400-e29b-41d4-a716-446655440000 timed out after 5000 ms from 10.0.0.8")
	second, secondID := extractTemplate("database query 6ba7b810-9dad-11d1-80b4-00c04fd430c8 timed out after 8000 ms from 10.0.0.9")
	if first != "database query <uuid> timed out after <num> ms from <ip>" || first != second || firstID != secondID {
		t.Fatalf("templates differ: (%q, %q) (%q, %q)", first, firstID, second, secondID)
	}
}
