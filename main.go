package main

import (
	"time"
	"os"
	"log"
	"database/sql"
	"path/filepath"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/c0va23/mongo2sql/converter"
	"github.com/c0va23/mongo2sql/state"
)

const convertersDir = "converters"

type converterMap map[string]*converter.Converter

func (converters converterMap) names() []string {
	names := make([]string, 0, len(converters))
	for name := range converters {
		names = append(names, name)
	}
	return names
}

func loadConverters(sqlDb *sql.DB) (converterMap, error) {
	filePaths, globErr := filepath.Glob(filepath.Join(convertersDir, "*.lua"))
	if nil != globErr {
		return nil, globErr
	}

	converters := make(converterMap)

	for _, filePath := range filePaths {
		conv, err := converter.New(filePath, sqlDb)
		if nil != err {
			log.Fatal(err)
		}

		defer conv.Close()
		converters[conv.FullName()] = conv
	}

	return converters, nil
}

func bootColl(session *mgo.Session, store state.Store, conv *converter.Converter) error {
	collection := session.DB(conv.DbName).C(conv.ColName)
	iter := collection.Find(bson.D{}).Iter()
	defer iter.Close()

	var doc converter.Document
	for iter.Next(&doc) {
		if err := conv.Inserted(doc); nil != err {
			return err
		}
	}

	if err := iter.Err(); nil != err {
		return err
	}

	return nil
}

func bootAll(
	session *mgo.Session,
	store state.Store,
	converters converterMap,
) error {
	for _, conv := range converters {
		log.Printf("Start bootstarap %s", conv.FullName())
		if exists, err := store.Exists(conv.FullName()); nil != err {
			return err
		} else if exists {
			log.Printf("Collection already %s initialized", conv.FullName())
			continue
		}

		timestamp := state.Timestamp {
			Time: time.Now().Truncate(time.Second),
		}
		if err := bootColl(session, store, conv); nil != err {
			return err
		}

		if err := store.Add(conv.FullName(), timestamp); nil != err {
			return err
		}
		log.Printf("Finish bootstaraping %s", conv.FullName())
	}
	return nil
}

func parseMongoTimestap(ts bson.MongoTimestamp) state.Timestamp {
	seconds := int64(ts) >> 32
	ordinal := int32(ts & 0xFFFFFFFF)
	return state.Timestamp {
		Time: time.Unix(seconds, 0),
		Ordinal: ordinal,
	}
}

func processOplog(session *mgo.Session, store state.Store, converters converterMap) {
	db := session.DB("local")
	oplogCol := db.C("oplog.$main")

	log.Println("Tail")
	iter := oplogCol.Find(bson.M{"ns": bson.M{ "$in": converters.names() }}).Tail(-1)

	var oplogDoc converter.Document
	for iter.Next(&oplogDoc) {
		fullName := oplogDoc["ns"].(string)
		conv := converters[fullName]

		lastTs, tsErr := store.GetTimestamp(fullName)
		if nil != tsErr {
			log.Fatal(tsErr)
		}

		ts := parseMongoTimestap(oplogDoc["ts"].(bson.MongoTimestamp))
		log.Printf("ts %s", ts)

		if !ts.After(lastTs) {
			log.Printf("Skip %s %s", fullName, ts)
			continue
		}

		if processErr := conv.ProcessOplogRecord(oplogDoc); nil != processErr {
			log.Fatal(processErr)
		}

		if updateErr := store.UpdateTimestamp(fullName, ts); nil != updateErr {
			log.Fatal(updateErr)
		}
		log.Printf("Update timestamp for %s to %+v", fullName, ts)
	}

	if nil != iter.Err() {
		log.Fatal(iter.Err())
	}
}

func fetchEnvvarOrPanic(envvar string) string {
	val := os.Getenv(envvar)
	if "" == val {
		log.Fatalf("Envvar %s is required", envvar)
	}
	return val
}

var stateStoreURL = fetchEnvvarOrPanic("STATE_STORE_URL")
var stateStoreDriver = fetchEnvvarOrPanic("STATE_STORE_DRIVER")
var mongodbURL = fetchEnvvarOrPanic("MONGODB_URL")

func main() {
	log.Println("Dial mongo")
	session, dialErr := mgo.Dial(mongodbURL)
	if nil != dialErr {
		log.Fatalf("Mongo dial error: %v", dialErr)
	}
	defer session.Close()
	log.Println("Dial mongo is successful")

	sqlDb, sqlErr := initSQLDb()
	if nil != sqlErr {
		log.Fatalf("Database init error: %v", sqlErr)
	}
	defer sqlDb.Close()

	store, storeErr := state.NewStore(stateStoreDriver, stateStoreURL)
	if nil != storeErr {
		log.Fatal(storeErr)
	}

	converters, convErr := loadConverters(sqlDb)
	if nil != convErr {
		log.Fatal(convErr)
	}

	if err := bootAll(session, store, converters); nil != err {
		log.Fatal(err)
	}

	processOplog(session, store, converters)
}
