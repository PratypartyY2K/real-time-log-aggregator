package processor

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
)

var templateReplacements = []struct {
	pattern     *regexp.Regexp
	replacement string
}{
	{regexp.MustCompile(`(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`), "<uuid>"},
	{regexp.MustCompile(`\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b`), "<ip>"},
	{regexp.MustCompile(`(?i)\b(?:0x)?[0-9a-f]{12,}\b`), "<hex>"},
	{regexp.MustCompile(`\b[0-9]+(?:\.[0-9]+)?\b`), "<num>"},
}

func extractTemplate(message string) (string, string) {
	template := strings.TrimSpace(message)
	for _, item := range templateReplacements {
		template = item.pattern.ReplaceAllString(template, item.replacement)
	}
	template = strings.Join(strings.Fields(template), " ")
	sum := sha256.Sum256([]byte(template))
	return template, hex.EncodeToString(sum[:16])
}
