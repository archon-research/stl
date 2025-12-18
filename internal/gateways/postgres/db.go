package postgres

import (
	"database/sql"
	"fmt"
)

func Connect(dsn string) (*sql.DB, error) {
	fmt.Println("Connecting to Postgres...")
	return nil, nil
}
