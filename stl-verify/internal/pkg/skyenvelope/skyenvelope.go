// Package skyenvelope holds the response-envelope mechanics shared by Sky's
// hosts: pagination guarding and optional-field folding. Split so the envelope
// stays one contract across hosts while page limits and each vendor's network
// vocabulary stay per-client. Now the only reader of these feeds — the Python
// API serves them from the tables this service writes rather than fetching them
// per request, so its own copy of this envelope is gone.
package skyenvelope

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Pagination is the envelope's pagination block. Total is nil when the host
// omits it or serves it null.
type Pagination struct {
	Total *int `json:"total"`
}

// RequireFullPage rejects a page that may be truncated, which would read as
// rows that do not exist. With a usable total, a short page means the set
// outgrew the limit; without one, a page at the limit cannot be told from a
// cut-off one, so it is refused rather than served as a silent partial set.
func RequireFullPage(p *Pagination, received, limit int, requestURL string) error {
	if p != nil && p.Total != nil {
		if *p.Total > received {
			return fmt.Errorf(
				"sky reported %d rows but returned %d; the page limit is too low: %s", *p.Total, received, requestURL)
		}
		return nil
	}
	if received >= limit {
		return fmt.Errorf(
			"sky returned a full page of %d rows with no usable total; the set may be truncated: %s", received, requestURL)
	}
	return nil
}

// OptionalText folds an omitted or blank field to nil, so consumers see one
// spelling of absence.
func OptionalText(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

// OptionalNumber folds an omitted or blank numeric field to nil, keeping the
// figure as upstream's literal string otherwise.
func OptionalNumber(value json.Number) *string {
	raw := strings.TrimSpace(value.String())
	if raw == "" {
		return nil
	}
	return &raw
}
