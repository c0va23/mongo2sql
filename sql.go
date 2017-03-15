package main

import (
  "os"
  "database/sql"
  "log"
  "fmt"

  // "github.com/GuiaBolso/darwin"

  _ "github.com/lib/pq"
)

const defaultDriver = "postgres"

func initSQLDb() (*sql.DB, error) {
  var driver, driverExists = os.LookupEnv("DATABASE_DRIVER")
  if !driverExists {
    driver = defaultDriver
    log.Printf("Use default driver: %s", defaultDriver)
  }
  var url, urlExists = os.LookupEnv("DATABASE_URL")
  if !urlExists {
    return nil, fmt.Errorf("Envvar DATABASE_URL required")
  }

  var dbErr error
  sqlDb, dbErr := sql.Open(driver, url)
  if nil != dbErr {
    return nil, dbErr
  }

  log.Printf("SQL DB initialized")
  return sqlDb, nil
}
