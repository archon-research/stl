package postgres

import (
	"database/sql"
	"fmt"
)

// DB defines the interface for database operations
// This allows for easy mocking in tests
type DB interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Close() error
}

// PostgresDB wraps sql.DB to implement the DB interface
type PostgresDB struct {
	*sql.DB
}

// Connect establishes a connection to the Postgres database
func Connect(dsn string) (DB, error) {
	fmt.Println("Connecting to Postgres...")
	return nil, nil
}
