package main

import (
	"fmt"
	"log"
	"time"

	"github.com/yuin/gopher-lua"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func bsonToTable(L *lua.LState, data map[string]interface{}) (*lua.LTable, error) {
	dataTable := L.NewTable()

	for key, rawValue := range data {
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
		case bool:
			boolValue := lua.LBool(value)
			dataTable.RawSet(lua.LString(key), boolValue)
		case time.Time:
			timeValue := lua.LString(value.String())
			dataTable.RawSet(lua.LString(key), timeValue)
		case map[string]interface{}:
			tableValue, tableErr := bsonToTable(L, value)
			if nil != tableErr {
				return nil, tableErr
			}
			dataTable.RawSet(lua.LString(key), tableValue)
		case bson.ObjectId:
			objectIDValue := lua.LString(value.Hex())
			dataTable.RawSet(lua.LString(key), objectIDValue)
		// TODO: Add other types https://docs.mongodb.com/manual/reference/bson-types/
		default:
			err := fmt.Errorf("Unknown value %#v for key %s", rawValue, key)
			return nil, err
		}
	}

	return dataTable, nil
}

const operationKey = "op"
const operationInsert = "i"

func main() {
	L := lua.NewState()
	defer L.Close()

	lErr := L.DoString(`
		inserted = function(data)
			print("insert")
			for key, value in pairs(data) do
				print(key, value)
				if "flags" == key then
					print("flags.checked", value.checked)
				end
			end
		end
	`)
	if nil != lErr {
		log.Fatal(lErr)
	}

	insertedValue := L.GetGlobal("inserted")
	log.Printf("insertedValue %+v", insertedValue)

	inserted := func(
		record map[string]interface{},
	) error {
		data, oExists := record["o"].(map[string]interface{})
		if !oExists {
			return fmt.Errorf(`Key "o" not exist for %+v`, record)
		}

		dataTable, dataErr := bsonToTable(L, data)

		if dataErr != nil {
			return dataErr
		}

		return L.CallByParam(lua.P{
			Fn:      insertedValue,
			NRet:    0,
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
		operation := data[operationKey]
		log.Printf(`Operation "%s"`, operation)
		switch operation {
		case operationInsert:
			if printErr := inserted(data); nil != printErr {
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
