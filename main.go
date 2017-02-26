package main

import (
	"log"
	"path/filepath"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/c0va23/mongo2sql/converter"
)

const operationKey = "op"
const operationInsert = "i"
const convertersDir = "converters"

func main() {
	filePaths, globErr := filepath.Glob(filepath.Join(convertersDir, "*.lua"))
	if nil != globErr {
		log.Fatal(globErr)
	}

	converterMap := map[string]*converter.Converter{}
	colNames := make([]string, 0, len(filePaths))

	for _, filePath := range filePaths {
		conv, err := converter.New(filePath)
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

	var data map[string]interface{}

	log.Println("Tail")
	iter := oplogCol.Find(bson.M{"ns": bson.M{ "$in": colNames }}).Tail(-1)

	for iter.Next(&data) {
		conv := converterMap[data["ns"].(string)]

		operation := data[operationKey]
		log.Printf(`Operation "%s"`, operation)
		switch operation {
		case operationInsert:
			if printErr := conv.Inserted(data); nil != printErr {
				log.Fatal(printErr)
			}
		default:
			log.Printf(`Unknown operatoin "%s" for %+v`, operation, data)
		}
	}

	if nil != iter.Err() {
		log.Fatal(iter.Err())
	}
}
