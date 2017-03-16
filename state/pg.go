package state

import (
  "time"
  "fmt"
  "database/sql"
  "log"

  "github.com/GuiaBolso/darwin"
)

var migrations = []darwin.Migration{
  {
    Version: 1,
    Description: "Create collection states",
    Script: `
      CREATE TABLE collection_states(
        name VARCHAR NOT NULL,
        bootstraped BOOLEAN NOT NULL DEFAULT false,
        last_timestamp TIMESTAMP
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
func (pgStore *PgStore) Add(name string) error {
  _, err := pgStore.db.Exec(
    `INSERT INTO collection_states(name) VALUES($1)`,
    name,
  )
  return err
}

// CollectionNotFoundErr is error return if collection name not found
type CollectionNotFoundErr string

func (collectionName CollectionNotFoundErr) Error() string {
  return fmt.Sprintf(`Collection "%s" not found`, string(collectionName))
}

func checkResult(result sql.Result, name string) error {
  rowCount, err := result.RowsAffected();
  if nil != err {
    return err
  }
  if 1 != rowCount {
    return CollectionNotFoundErr(name)
  }
  return nil
}

// SetBootstraped change flag `bootstraped` for collection by `name`
func (pgStore *PgStore) SetBootstraped(name string, bootstraped bool) error {
  result, err := pgStore.db.Exec(
    `UPDATE collection_states SET bootstraped = $1 WHERE name = $2`,
    bootstraped,
    name,
  )
  if nil != err {
    return err
  }
  return checkResult(result, name)
}

// UpdateTimestamp update last_timestamp for collection by `name`
func (pgStore *PgStore) UpdateTimestamp(name string, timestamp time.Time) error {
  result, err := pgStore.db.Exec(
    `UPDATE collection_states SET last_timestamp = $1 WHERE name = $2`,
    timestamp,
    name,
  )
  if nil != err {
    return err
  }
  return checkResult(result, name)
}

// IsBootstraped return bootstraped status for collection
func (pgStore *PgStore) IsBootstraped(name string) (bool, error)  {
  row := pgStore.db.QueryRow(
    `SELECT bootstraped FROM collection_states WHERE name = $1`,
    name,
  )

  var bootstraped bool
  scanErr := row.Scan(&bootstraped)
  return bootstraped, scanErr
}

// Timestamp return current timestamp
func (pgStore *PgStore) Timestamp(name string) (time.Time, error) {
  row := pgStore.db.QueryRow(
    `SELECT last_timestamp FROM collection_states WHERE name = $1`,
    name,
  )

  var timestamp time.Time
  scanErr := row.Scan(&timestamp)
  return timestamp, scanErr
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
func NewPgStore(db *sql.DB) (*PgStore, error) {
  if migrateErr := migrate(db); nil != migrateErr {
    return nil, migrateErr
  }

  return &PgStore{
    db: db,
  }, nil
}
