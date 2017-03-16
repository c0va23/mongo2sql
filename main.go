package main

import (
	"time"
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

func bootAll(session *mgo.Session, store state.Store, converters converterMap) {
	for _, conv := range converters {
		log.Printf("Start bootstarap %s", conv.FullName())
		if exists, err := store.Exists(conv.FullName()); nil != err {
			log.Print(err)
			continue
		} else if !exists {
			addErr := store.Add(conv.FullName())
			if nil != addErr {
				log.Print(addErr)
				continue
			}
			log.Printf("Collection %s initialized", conv.FullName())
		}
		if bootstraped, storeErr := store.IsBootstraped(conv.FullName()); nil != storeErr {
			log.Fatal(storeErr)
			continue
		} else if bootstraped {
			log.Printf("Collection %s already bootstraped", conv.FullName())
			continue
		}
		bootstrapTime := time.Now()
		if bootErr := bootColl(session, store, conv); nil != bootErr {
			log.Printf("Bootstarap error: %v", bootErr)
			continue
		}
		if err := store.SetBootstraped(conv.FullName(), true); nil != err {
			log.Print(err)
			continue
		}
		if err := store.UpdateTimestamp(conv.FullName(), bootstrapTime); nil != err {
			log.Print(err)
			continue
		}
		log.Printf("End bootstarap %s", conv.FullName())
	}
}

func processOplog(session *mgo.Session, converters converterMap) {
	db := session.DB("local")
	oplogCol := db.C("oplog.$main")

	log.Println("Tail")
	iter := oplogCol.Find(bson.M{"ns": bson.M{ "$in": converters.names() }}).Tail(-1)

	var oplogDoc converter.Document
	for iter.Next(&oplogDoc) {
		conv := converters[oplogDoc["ns"].(string)]

		if processErr := conv.ProcessOplogRecord(oplogDoc); nil != processErr {
			log.Fatal(processErr)
		}
	}

	if nil != iter.Err() {
		log.Fatal(iter.Err())
	}
}

func main() {
	log.Println("Dial")
	session, dialErr := mgo.Dial("mongo")
	if nil != dialErr {
		log.Fatal(dialErr)
	}
	defer session.Close()

	sqlDb, sqlErr := initSQLDb()
	if nil != sqlErr {
		log.Fatal(sqlErr)
	}
	defer sqlDb.Close()

	store, storeErr := state.NewPgStore(sqlDb)
	if nil != storeErr {
		log.Fatal(storeErr)
	}

	converters, convErr := loadConverters(sqlDb)
	if nil != convErr {
		log.Fatal(convErr)
	}

	bootAll(session, store, converters)

	processOplog(session, converters)
}
