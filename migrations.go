package main

import (
  "path/filepath"
  "regexp"
  "database/sql"
  "strconv"
  "io/ioutil"
  "log"

  "gopkg.in/yaml.v2"
  "github.com/GuiaBolso/darwin"
)

const migrationsDir = "migrations"

type migrationSource struct {
  Script string
}

var fileNameRegexp = regexp.MustCompile("^([0-9]+)_(.+)\\.yaml$")

func loadMigrations(db *sql.DB) ([]darwin.Migration, error) {
  filePaths, globErr := filepath.Glob(filepath.Join(migrationsDir, "[0-9]*.yaml"))
  if nil != globErr {
    return nil, globErr
  }

  migrations := make([]darwin.Migration, 0, len(filePaths))

  for _, fullFileName := range filePaths {
    fileName := filepath.Base(fullFileName)
    matches := fileNameRegexp.FindStringSubmatch(fileName)
    versionStr := matches[1]
    description := matches[2]
    version, _ := strconv.ParseFloat(versionStr, 10)

    yamlData, readErr := ioutil.ReadFile(fullFileName)
    if nil != readErr {
      return nil, readErr
    }

    var source migrationSource

    if err := yaml.Unmarshal(yamlData, &source); nil != err {
      return nil, err
    }

    migrations = append(migrations, darwin.Migration {
      Version: version,
      Description: description,
      Script: source.Script,
    })
  }

  return migrations, nil
}

func logMigration(infoChan <-chan darwin.MigrationInfo) {
  log.Printf("Migration is started")
  for info := range infoChan {
    if nil == info.Error {
      log.Printf(
        `Migration %f "%s" is %s`,
        info.Migration.Version,
        info.Migration.Description,
        info.Status,
      )
    } else {
      log.Printf("Migration error: %v", info.Error)
    }
  }
  log.Printf("Migration is ended")
}

func appleMigrations(db *sql.DB) error {
  migrations, loadErr := loadMigrations(db)
  if nil != loadErr {
    return loadErr
  }

  driver := darwin.NewGenericDriver(db, darwin.PostgresDialect{})

  infoChan := make(chan darwin.MigrationInfo)

  go logMigration(infoChan)

  return darwin.Migrate(driver, migrations, infoChan)
}
