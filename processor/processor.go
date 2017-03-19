package processor

import (
	"log"
	"time"

	"github.com/c0va23/mongo2sql/converter"
	"github.com/c0va23/mongo2sql/state"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func parseMongoTimestap(ts bson.MongoTimestamp) state.Timestamp {
	seconds := int64(ts) >> 32
	ordinal := int32(ts & 0xFFFFFFFF)
	return state.Timestamp{
		Time:    time.Unix(seconds, 0),
		Ordinal: ordinal,
	}
}

// ProcessOplog iterate over oplog
func ProcessOplog(
	session *mgo.Session,
	store state.Store,
	converters converter.Map,
) error {
	db := session.DB("local")
	oplogCol := db.C("oplog.$main")

	log.Println("Tail")
	iter := oplogCol.Find(bson.M{"ns": bson.M{"$in": converters.Names()}}).Tail(-1)

	var oplogDoc converter.Document
	for iter.Next(&oplogDoc) {
		fullName := oplogDoc["ns"].(string)
		conv := converters[fullName]

		lastTs, tsErr := store.GetTimestamp(fullName)
		if nil != tsErr {
			return tsErr
		}

		ts := parseMongoTimestap(oplogDoc["ts"].(bson.MongoTimestamp))
		log.Printf("ts %s", ts)

		if !ts.After(lastTs) {
			log.Printf("Skip %s %s", fullName, ts)
			continue
		}

		if err := conv.ProcessOplogRecord(oplogDoc); nil != err {
			return err
		}

		if err := store.UpdateTimestamp(fullName, ts); nil != err {
			return err
		}
		log.Printf("Update timestamp for %s to %+v", fullName, ts)
	}

	return iter.Err()
}
