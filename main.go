package main

import (
	"log"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"github.com/yuin/gopher-lua"
)

func main() {
	L := lua.NewState()
	defer L.Close()

	lErr := L.DoString(`
		inserted = function(data)
			print("insert")
			for key, value in pairs(data) do
				print(key, value)
			end
		end
	`)
	if nil != lErr {
		log.Fatal(lErr)
	}

	insertedValue := L.GetGlobal("inserted")
	log.Printf("insertedValue %+v", insertedValue)

	inserted := func(
		data map[string]interface{},
	) error {
		dataTable := L.NewTable()

		o, oExists := data["o"].(map[string]interface{})
		if !oExists {
			log.Fatal(`key "o" not exits`)
		}

		for key, rawValue := range o {
			switch value := rawValue.(type) {
			case string:
				dataTable.RawSetString(key, lua.LString(value))
			case float64:
				floatValue := lua.LNumber(value)
				dataTable.RawSet(lua.LString(key), floatValue)
			case int:
				intValue := lua.LNumber(value)
				dataTable.RawSet(lua.LString(key), intValue)
			case int64:
				longValue := lua.LNumber(value)
				dataTable.RawSet(lua.LString(key), longValue)
			case bson.ObjectId:
				objectIDValue := lua.LString(value.Hex())
				dataTable.RawSet(lua.LString(key), objectIDValue)
			case time.Time:
				timeValue := lua.LString(value.String())
				dataTable.RawSet(lua.LString(key), timeValue)
			case bool:
				boolValue := lua.LBool(value)
				dataTable.RawSet(lua.LString(key), boolValue)
			default:
				log.Printf("Unknown value %#v for key %s", rawValue, key)
			}
		}

		return L.CallByParam(lua.P{
			Fn: insertedValue,
			NRet: 0,
			Protect: true,
		}, dataTable)
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
	iter := oplogCol.Find(bson.M{"ns": "testme.users"}).Tail(-1)

	for iter.Next(&data) {
		if printErr := inserted(data); nil != printErr {
			log.Fatal(printErr)
		}
	}

	if nil != iter.Err() {
		log.Fatal(iter.Err())
	}
}
