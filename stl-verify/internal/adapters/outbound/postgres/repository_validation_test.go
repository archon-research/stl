package postgres

import (
	"testing"
)

// TestNewTokenRepository_NilDB tests that NewTokenRepository returns an error when db is nil.
func TestNewTokenRepository_NilDB(t *testing.T) {
	_, err := NewTokenRepository(nil, nil, 0)
	if err == nil {
		t.Fatal("expected error when db is nil, got nil")
	}
	expectedMsg := "database connection cannot be nil"
	if err.Error() != expectedMsg {
		t.Errorf("expected error message %q, got %q", expectedMsg, err.Error())
	}
}

// TestNewUserRepository_NilDB tests that NewUserRepository returns an error when db is nil.
func TestNewUserRepository_NilDB(t *testing.T) {
	_, err := NewUserRepository(nil, nil, 0)
	if err == nil {
		t.Fatal("expected error when db is nil, got nil")
	}
	expectedMsg := "database connection cannot be nil"
	if err.Error() != expectedMsg {
		t.Errorf("expected error message %q, got %q", expectedMsg, err.Error())
	}
}

// TestNewPositionRepository_NilDB tests that NewPositionRepository returns an error when db is nil.
func TestNewPositionRepository_NilDB(t *testing.T) {
	_, err := NewPositionRepository(nil, nil, 0)
	if err == nil {
		t.Fatal("expected error when db is nil, got nil")
	}
	expectedMsg := "database connection cannot be nil"
	if err.Error() != expectedMsg {
		t.Errorf("expected error message %q, got %q", expectedMsg, err.Error())
	}
}

// TestNewProtocolRepository_NilDB tests that NewProtocolRepository returns an error when db is nil.
func TestNewProtocolRepository_NilDB(t *testing.T) {
	_, err := NewProtocolRepository(nil, nil, 0)
	if err == nil {
		t.Fatal("expected error when db is nil, got nil")
	}
	expectedMsg := "database connection cannot be nil"
	if err.Error() != expectedMsg {
		t.Errorf("expected error message %q, got %q", expectedMsg, err.Error())
	}
}
