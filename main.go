package main

import (
	"os"
	"log"
	"database/sql"

	"gopkg.in/mgo.v2"

	"github.com/c0va23/mongo2sql/converter"
	"github.com/c0va23/mongo2sql/state"
	"github.com/c0va23/mongo2sql/processor"

	_ "github.com/lib/pq"
)

func fetchEnvvarOrPanic(envvar string) string {
	val, exists := os.LookupEnv(envvar)
	if !exists {
		log.Fatalf("Envvar %s is required", envvar)
	}
	return val
}

var stateStoreURL = fetchEnvvarOrPanic("STATE_STORE_URL")
var stateStoreDriver = fetchEnvvarOrPanic("STATE_STORE_DRIVER")
var mongodbURL = fetchEnvvarOrPanic("MONGODB_URL")
var dbDriver = fetchEnvvarOrPanic("DATABASE_DRIVER")
var dbURL = fetchEnvvarOrPanic("DATABASE_URL")

func main() {
	log.Println("Dial MongoDB")
	session, dialErr := mgo.Dial(mongodbURL)
	if nil != dialErr {
		log.Fatalf("Mongo dial error: %v", dialErr)
	}
	defer session.Close()
	log.Println("Dial MongoDB is successful")

	log.Printf("Open SQL database")
	sqlDb, sqlErr := sql.Open(dbDriver, dbURL)
	if nil != sqlErr {
		log.Fatalf("Database init error: %v", sqlErr)
	}
	defer sqlDb.Close()
	log.Printf("Open SQL database is success")

	log.Printf(`Init state store "%s"`, stateStoreDriver)
	store, storeErr := state.NewStore(stateStoreDriver, stateStoreURL)
	if nil != storeErr {
		log.Fatalf("Store init error: %v", storeErr)
	}
	defer store.Close()
	log.Printf("State store initialized")

	converters, convErr := converter.LoadAll(sqlDb)
	if nil != convErr {
		log.Fatalf("Load converters error: %v", convErr)
	}

	if err := appleMigrations(sqlDb); nil != err {
		log.Fatalf("Apple migrations error: %v", err)
	}

	if err := processor.BootAll(session, store, converters); nil != err {
		log.Fatalf("Bootstrap error: %v", err)
	}

	if err := processor.ProcessOplog(session, store, converters); nil != err {
		log.Fatalf("Process error: %v", err)
	}
}
