package ingestion

import (
	"strings"
	"time"
)

// ParseEventTime parses an event timestamp leniently.
// Accepts RFC 3339 (with timezone) and ISO 8601 without timezone (assumes UTC).
// The OL GE integration (openlineage-integration-common 1.39.0) emits timestamps
// via Python's datetime.now().isoformat() which omits timezone info.
func ParseEventTime(s string) time.Time {
	s = strings.TrimSpace(s)

	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t
	}

	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}

	// ISO 8601 without timezone — assume UTC
	if t, err := time.Parse("2006-01-02T15:04:05.999999999", s); err == nil {
		return t.UTC()
	}

	if t, err := time.Parse("2006-01-02T15:04:05", s); err == nil {
		return t.UTC()
	}

	return time.Time{}
}
