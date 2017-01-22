package main

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
)

type User struct {
	Email string
	Name string
	Login string
}

func main() {
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
	iter := oplogCol.Find(bson.M{}).Tail(-1)

	for iter.Next(&data) {
		log.Printf("data %+v", data)
	}

	if nil != iter.Err() {
		log.Fatal(iter.Err())
	}
}
