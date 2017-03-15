package converter

import (
  "fmt"
	"time"

	"gopkg.in/mgo.v2/bson"
	"github.com/yuin/gopher-lua"
)

func bsonToTable(L *lua.LState, data Document) (*lua.LTable, error) {
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
		case Document:
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
