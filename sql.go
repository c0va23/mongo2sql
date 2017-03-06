package main

import (
  "os"
  "database/sql"
  "log"

  _ "github.com/lib/pq"
)

var defaultDriver = "postgres"

var sqlDb *sql.DB

func init()  {
  var driver, driverExists = os.LookupEnv("DATABASE_DRIVER")
  if !driverExists {
    panic("Envvar DATABASE_DRIVER required")
  }
  var url, urlExists = os.LookupEnv("DATABASE_URL")
  if !urlExists {
    panic("Envvar DATABASE_URL required")
  }

  var dbErr error
  sqlDb, dbErr = sql.Open(driver, url)
  if nil != dbErr {
    panic(dbErr)
  }
  log.Printf("SQL DB initialized")
}
