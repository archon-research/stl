package entity

import "fmt"

// UserProtocolMetadata represents protocol-specific metadata for a user.
type UserProtocolMetadata struct {
	ID         int64
	UserID     int64
	ProtocolID int64
	Metadata   map[string]any
}

// NewUserProtocolMetadata creates a new UserProtocolMetadata entity.
func NewUserProtocolMetadata(id, userID, protocolID int64) (*UserProtocolMetadata, error) {
	upm := &UserProtocolMetadata{
		ID:         id,
		UserID:     userID,
		ProtocolID: protocolID,
		Metadata:   make(map[string]any),
	}
	if err := upm.validate(); err != nil {
		return nil, err
	}
	return upm, nil
}

// validate checks that all fields have valid values.
func (upm *UserProtocolMetadata) validate() error {
	if upm.ID <= 0 {
		return fmt.Errorf("id must be positive, got %d", upm.ID)
	}
	if upm.UserID <= 0 {
		return fmt.Errorf("userID must be positive, got %d", upm.UserID)
	}
	if upm.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", upm.ProtocolID)
	}
	return nil
}

// SetMetadata sets a key-value pair in the metadata.
func (u *UserProtocolMetadata) SetMetadata(key string, value any) {
	if u.Metadata == nil {
		u.Metadata = make(map[string]any)
	}
	u.Metadata[key] = value
}

// GetMetadata retrieves a value from metadata by key.
func (u *UserProtocolMetadata) GetMetadata(key string) (any, bool) {
	if u.Metadata == nil {
		return nil, false
	}
	val, ok := u.Metadata[key]
	return val, ok
}
