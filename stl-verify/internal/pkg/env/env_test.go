package env

import (
	"testing"
	"time"
)

func TestGetInt(t *testing.T) {
	const key = "STL_TEST_GET_INT"

	tests := []struct {
		name    string
		set     bool
		value   string
		def     int
		want    int
		wantErr bool
	}{
		{name: "unset returns default", set: false, def: 42, want: 42},
		{name: "empty string returns default", set: true, value: "", def: 7, want: 7},
		{name: "valid integer is parsed", set: true, value: "100", def: 10, want: 100},
		{name: "negative is parsed", set: true, value: "-3", def: 0, want: -3},
		{name: "non-numeric returns error", set: true, value: "abc", def: 10, wantErr: true},
		{name: "float returns error", set: true, value: "1.5", def: 10, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv(key, tc.value)
			}

			got, err := GetInt(key, tc.def)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (got=%d)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("GetInt = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestGetDuration(t *testing.T) {
	const key = "STL_TEST_GET_DURATION"

	tests := []struct {
		name    string
		set     bool
		value   string
		def     time.Duration
		want    time.Duration
		wantErr bool
	}{
		{name: "unset returns default", set: false, def: 30 * time.Second, want: 30 * time.Second},
		{name: "empty string returns default", set: true, value: "", def: 5 * time.Second, want: 5 * time.Second},
		{name: "seconds is parsed", set: true, value: "5s", def: 30 * time.Second, want: 5 * time.Second},
		{name: "milliseconds is parsed", set: true, value: "250ms", def: time.Second, want: 250 * time.Millisecond},
		{name: "minutes is parsed", set: true, value: "2m", def: time.Second, want: 2 * time.Minute},
		{name: "bare integer returns error", set: true, value: "5", def: time.Second, wantErr: true},
		{name: "garbage returns error", set: true, value: "not-a-duration", def: time.Second, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv(key, tc.value)
			}

			got, err := GetDuration(key, tc.def)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (got=%s)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("GetDuration = %s, want %s", got, tc.want)
			}
		})
	}
}
