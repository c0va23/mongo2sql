package processor

import (
	"log"
	"time"

	"github.com/c0va23/mongo2sql/converter"
	"github.com/c0va23/mongo2sql/state"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func bootColl(
	session *mgo.Session,
	store state.Store,
	conv *converter.Converter,
) error {
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

// BootAll bootstarap all collections
func BootAll(
	session *mgo.Session,
	store state.Store,
	converters converter.Map,
) error {
	for _, conv := range converters {
		log.Printf("Start bootstarap %s", conv.FullName())
		if exists, err := store.Exists(conv.FullName()); nil != err {
			return err
		} else if exists {
			log.Printf("Collection already %s initialized", conv.FullName())
			continue
		}

		timestamp := state.Timestamp{
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
