package env

import (
	"testing"
	"time"
)

func TestGetInt(t *testing.T) {
	const key = "STL_TEST_GET_INT"

	tests := []struct {
		name    string
		value   string
		def     int
		want    int
		wantErr bool
	}{
		{name: "unset returns default", def: 42, want: 42},
		{name: "valid integer is parsed", value: "100", def: 10, want: 100},
		{name: "negative is parsed", value: "-3", def: 0, want: -3},
		{name: "non-numeric returns error", value: "abc", def: 10, wantErr: true},
		{name: "float returns error", value: "1.5", def: 10, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.value != "" {
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

func TestGetInt64(t *testing.T) {
	const key = "STL_TEST_GET_INT64"

	tests := []struct {
		name    string
		value   string
		def     int64
		want    int64
		wantErr bool
	}{
		{name: "unset returns default", def: 42, want: 42},
		{name: "block number is parsed", value: "25946281", def: 0, want: 25946281},
		{name: "negative is parsed", value: "-3", def: 0, want: -3},
		{name: "non-numeric returns error", value: "abc", def: 10, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.value != "" {
				t.Setenv(key, tc.value)
			}

			got, err := GetInt64(key, tc.def)
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
				t.Errorf("GetInt64 = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestGetDuration(t *testing.T) {
	const key = "STL_TEST_GET_DURATION"

	tests := []struct {
		name    string
		value   string
		def     time.Duration
		want    time.Duration
		wantErr bool
	}{
		{name: "unset returns default", def: 30 * time.Second, want: 30 * time.Second},
		{name: "seconds is parsed", value: "5s", def: 30 * time.Second, want: 5 * time.Second},
		{name: "milliseconds is parsed", value: "250ms", def: time.Second, want: 250 * time.Millisecond},
		{name: "minutes is parsed", value: "2m", def: time.Second, want: 2 * time.Minute},
		{name: "bare integer returns error", value: "5", def: time.Second, wantErr: true},
		{name: "garbage returns error", value: "not-a-duration", def: time.Second, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.value != "" {
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

func TestGetPositiveInt(t *testing.T) {
	const key = "STL_TEST_GET_POSITIVE_INT"

	tests := []struct {
		name    string
		value   *string
		def     int
		want    int
		wantErr bool
	}{
		{name: "unset returns default", def: 100, want: 100},
		{name: "positive is parsed", value: new("1000"), def: 100, want: 1000},
		{name: "zero returns error", value: new("0"), def: 100, wantErr: true},
		{name: "negative returns error", value: new("-1"), def: 100, wantErr: true},
		{name: "empty returns error", value: new(""), def: 100, wantErr: true},
		{name: "non-numeric returns error", value: new("lots"), def: 100, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.value != nil {
				t.Setenv(key, *tc.value)
			}

			got, err := GetPositiveInt(key, tc.def)
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
				t.Errorf("GetPositiveInt = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestGetPositiveDuration(t *testing.T) {
	const key = "STL_TEST_GET_POSITIVE_DURATION"

	tests := []struct {
		name    string
		value   *string
		def     time.Duration
		want    time.Duration
		wantErr bool
	}{
		{name: "unset returns default", def: 30 * time.Second, want: 30 * time.Second},
		{name: "positive is parsed", value: new("10s"), def: 30 * time.Second, want: 10 * time.Second},
		{name: "zero returns error", value: new("0s"), def: 30 * time.Second, wantErr: true},
		{name: "negative returns error", value: new("-5s"), def: 30 * time.Second, wantErr: true},
		{name: "empty returns error", value: new(""), def: 30 * time.Second, wantErr: true},
		{name: "garbage returns error", value: new("soon"), def: 30 * time.Second, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.value != nil {
				t.Setenv(key, *tc.value)
			}

			got, err := GetPositiveDuration(key, tc.def)
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
				t.Errorf("GetPositiveDuration = %s, want %s", got, tc.want)
			}
		})
	}
}
