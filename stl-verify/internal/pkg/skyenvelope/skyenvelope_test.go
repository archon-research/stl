package skyenvelope

import (
	"encoding/json"
	"testing"
)

func TestRequireFullPage(t *testing.T) {
	total := func(n int) *Pagination { return &Pagination{Total: &n} }

	for _, tc := range []struct {
		name     string
		p        *Pagination
		received int
		wantErr  bool
	}{
		{"total above received is a truncated page", total(40), 11, true},
		{"total equal to received is complete", total(11), 11, false},
		{"no pagination and a short page is complete", nil, 11, false},
		{"no pagination and a full page may be truncated", nil, 500, true},
		{"null total and a full page may be truncated", &Pagination{}, 500, true},
		{"null total and a short page is complete", &Pagination{}, 11, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := RequireFullPage(tc.p, tc.received, 500, "https://sky.test/route")
			if (err != nil) != tc.wantErr {
				t.Errorf("RequireFullPage() = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestOptionalTextFoldsOmittedAndBlankToNil(t *testing.T) {
	if got := OptionalText(""); got != nil {
		t.Errorf("OptionalText(\"\") = %q, want nil", *got)
	}
	if got := OptionalText("  "); got != nil {
		t.Errorf("OptionalText(blank) = %q, want nil", *got)
	}
	if got := OptionalText(" USDS "); got == nil || *got != "USDS" {
		t.Errorf("OptionalText = %v, want USDS", got)
	}
}

func TestOptionalNumberKeepsTheLiteralFigure(t *testing.T) {
	if got := OptionalNumber(json.Number("")); got != nil {
		t.Errorf("OptionalNumber(empty) = %q, want nil", *got)
	}
	if got := OptionalNumber(json.Number("0.3705")); got == nil || *got != "0.3705" {
		t.Errorf("OptionalNumber = %v, want the unrounded literal", got)
	}
}
