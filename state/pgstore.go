package state

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/GuiaBolso/darwin"
)

var migrations = []darwin.Migration{
	{
		Version:     1,
		Description: "Create collection states",
		Script: `
      CREATE TABLE collection_states(
        name VARCHAR NOT NULL,
        last_timestamp TIMESTAMP NOT NULL,
        last_ordinal INTEGER NOT NULL,
        PRIMARY KEY (name)
      )
    `,
	},
}

// PgStore is state storage into PostgreSQL
type PgStore struct {
	db *sql.DB
}

// Exists check collection
func (pgStore *PgStore) Exists(name string) (bool, error) {
	rows, err := pgStore.db.Query(`SELECT 1 FROM collection_states WHERE name = $1`, name)

	if nil != err {
		return false, err
	}

	defer rows.Close()

	if !rows.Next() && nil == rows.Err() {
		return false, nil
	} else if nil != rows.Err() {
		return false, rows.Err()
	}
	return true, nil
}

// Add new collection into PgStore
func (pgStore *PgStore) Add(name string, ts Timestamp) error {
	_, err := pgStore.db.Exec(
		`INSERT INTO collection_states(name, last_timestamp, last_ordinal) VALUES($1, $2, $3)`,
		name,
		ts.Time,
		ts.Ordinal,
	)
	return err
}

// CollectionNotFoundErr is error return if collection name not found
type CollectionNotFoundErr string

func (collectionName CollectionNotFoundErr) Error() string {
	return fmt.Sprintf(`Collection "%s" not found`, string(collectionName))
}

func checkResult(result sql.Result, name string) error {
	rowCount, err := result.RowsAffected()
	if nil != err {
		return err
	}
	if 1 != rowCount {
		return CollectionNotFoundErr(name)
	}
	return nil
}

// UpdateTimestamp update last_timestamp for collection by `name`
func (pgStore *PgStore) UpdateTimestamp(name string, ts Timestamp) error {
	result, err := pgStore.db.Exec(
		`UPDATE collection_states SET last_timestamp = $1, last_ordinal = $2 WHERE name = $3`,
		ts.Time,
		ts.Ordinal,
		name,
	)
	if nil != err {
		return err
	}
	return checkResult(result, name)
}

// GetTimestamp return current timestamp
func (pgStore *PgStore) GetTimestamp(name string) (Timestamp, error) {
	row := pgStore.db.QueryRow(
		`SELECT last_timestamp, last_ordinal FROM collection_states WHERE name = $1`,
		name,
	)

	var ts Timestamp
	err := row.Scan(&ts.Time, &ts.Ordinal)
	return ts, err
}

// Close PgStore
func (pgStore *PgStore) Close() error {
	return pgStore.db.Close()
}

func printMigrationInfo(infoChan chan darwin.MigrationInfo) {
	log.Println("Start migrations")
	for info := range infoChan {
		log.Printf(`Migration "%s" is %s`, info.Migration.Description, info.Status)
		if darwin.Error == info.Status {
			log.Println(info.Error)
		}
	}
	log.Println("End migrations")
}

func migrate(db *sql.DB) error {
	darwinDriver := darwin.NewGenericDriver(db, darwin.PostgresDialect{})
	infoChan := make(chan darwin.MigrationInfo)
	defer close(infoChan)
	go printMigrationInfo(infoChan)
	return darwin.Migrate(darwinDriver, migrations, infoChan)
}

// NewPgStore create new PgStore
func NewPgStore(url string) (Store, error) {
	db, sqlErr := sql.Open("postgres", url)
	if nil != sqlErr {
		return nil, sqlErr
	}

	if migrateErr := migrate(db); nil != migrateErr {
		return nil, migrateErr
	}

	return &PgStore{
		db: db,
	}, nil
}
