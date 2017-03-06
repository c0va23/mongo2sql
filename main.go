package main

import (
	"log"
	"path/filepath"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/c0va23/mongo2sql/converter"
)

const convertersDir = "converters"

func main() {
	filePaths, globErr := filepath.Glob(filepath.Join(convertersDir, "*.lua"))
	if nil != globErr {
		log.Fatal(globErr)
	}

	converterMap := map[string]*converter.Converter{}
	colNames := make([]string, 0, len(filePaths))

	for _, filePath := range filePaths {
		conv, err := converter.New(filePath, sqlDb)
		if nil != err {
			log.Fatal(err)
		}

		defer conv.Close()
		converterMap[conv.ColName] = conv
		colNames = append(colNames, conv.ColName)
	}

	log.Println("Dial")
	session, dialErr := mgo.Dial("mongo")
	if nil != dialErr {
		log.Fatal(dialErr)
	}
	defer session.Close()

	db := session.DB("local")
	oplogCol := db.C("oplog.$main")

	log.Println("Tail")
	iter := oplogCol.Find(bson.M{"ns": bson.M{ "$in": colNames }}).Tail(-1)

	var logRecord map[string]interface{}
	for iter.Next(&logRecord) {
		conv := converterMap[logRecord["ns"].(string)]

		if processErr := conv.ProcessOplogRecord(logRecord); nil != processErr {
			log.Fatal(processErr)
		}
	}

	if nil != iter.Err() {
		log.Fatal(iter.Err())
	}
}
