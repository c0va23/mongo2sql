package main

import (
  "os"
  "database/sql"
  "log"
  "fmt"

  // "github.com/GuiaBolso/darwin"

  _ "github.com/lib/pq"
)

func initSQLDb() (*sql.DB, error) {
  var url, urlExists = os.LookupEnv("DATABASE_URL")
  if !urlExists {
    return nil, fmt.Errorf("Envvar DATABASE_URL required")
  }

  var dbErr error
  sqlDb, dbErr := sql.Open("postgres", url)
  if nil != dbErr {
    return nil, dbErr
  }

  log.Printf("SQL DB initialized")
  return sqlDb, nil
}
