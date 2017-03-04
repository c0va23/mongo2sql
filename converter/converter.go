package converter

import (
  "fmt"
  "path"

	"github.com/yuin/gopher-lua"
)

// Converter load LUA for handle mongo opload record
type Converter struct {
  ColName string
  LuaState *lua.LState
  insertedFunc *lua.LFunction
  updatedFunc *lua.LFunction
  deletedFunc *lua.LFunction
}

const luaExt = ".lua"

// New create new converter
func New(filePath string) (*Converter, error) {
  fileName := path.Base(filePath)

  if fileExt := path.Ext(fileName); luaExt != fileExt {
    return nil, fmt.Errorf(`File "%s" should have extension "%s".`, filePath, luaExt)
  }

  luaState := lua.NewState()

  if err := luaState.DoFile(filePath); nil != err {
    return nil, err
  }

  insertedFunc, err := initFunction(luaState, "inserted")
  if nil != err {
    return nil, err
  }

  updatedFunc, err := initFunction(luaState, "updated")
  if nil != err {
    return nil, err
  }

  deletedFunc, err := initFunction(luaState, "deleted")
  if nil != err {
    return nil, err
  }

  converter := Converter {
    ColName: fileName[0:len(fileName)-len(luaExt)],
    LuaState: luaState,
    insertedFunc: insertedFunc,
    updatedFunc: updatedFunc,
    deletedFunc: deletedFunc,
  }

  return &converter, nil
}

func initFunction(luaState *lua.LState, name string) (*lua.LFunction, error) {
  value := luaState.GetGlobal(name)
  if value.Type() == lua.LTNil {
    return nil, fmt.Errorf(`Function "%s" not defined`, name)
  }

  funcValue, isFunc := value.(*lua.LFunction)
  if !isFunc {
    return nil, fmt.Errorf(`Variable "%s" is not function (is %+v)`, name, value)
  }

  return funcValue, nil
}

// Close converter (LuaState)
func (conv *Converter) Close() {
  conv.LuaState.Close()
}

func (conv *Converter) inserted(record map[string]interface{}) error {
	doc, oExists := record["o"].(map[string]interface{})
	if !oExists {
		return fmt.Errorf(`Key "o" not exist for %+v`, record)
	}

	docTable, tableErr := bsonToTable(conv.LuaState, doc)

	if tableErr != nil {
		return tableErr
	}

	return conv.LuaState.CallByParam(lua.P{
		Fn:      conv.insertedFunc,
		NRet:    0,
		Protect: true,
	}, docTable)
}

func (conv *Converter) updated(record map[string]interface{}) error {
	query, queryExists := record["o2"].(map[string]interface{})
	if !queryExists {
		return fmt.Errorf(`Key "o2" not exist for %+v`, record)
	}

	queryTable, tableErr := bsonToTable(conv.LuaState, query)

	if tableErr != nil {
		return tableErr
	}

  update, updateExists := record["o"].(map[string]interface{})

  if !updateExists {
		return fmt.Errorf(`Key "o" not exist for %+v`, record)
  }

  updateTable, tableErr := bsonToTable(conv.LuaState, update)

	if tableErr != nil {
		return tableErr
	}

	return conv.LuaState.CallByParam(lua.P{
		Fn:      conv.updatedFunc,
		NRet:    0,
		Protect: true,
	}, queryTable, updateTable)
}

func (conv *Converter) deleted(record map[string]interface{}) error {
	query, queryExists := record["o"].(map[string]interface{})
	if !queryExists {
		return fmt.Errorf(`Key "o2" not exist for %+v`, record)
	}

	queryTable, tableErr := bsonToTable(conv.LuaState, query)

	if tableErr != nil {
		return tableErr
	}

	return conv.LuaState.CallByParam(lua.P{
		Fn:      conv.deletedFunc,
		NRet:    0,
		Protect: true,
	}, queryTable)
}

const logOperationKey = "op"
const operationInsert = "i"
const operationUpdate = "u"
const operationDelete = "d"

// ProcessOplogRecord accept oplog record and process with operation callback
func (conv *Converter) ProcessOplogRecord(oplogRecord map[string]interface{}) error {
	operation := oplogRecord[logOperationKey]
	switch operation {
	case operationInsert:
		return conv.inserted(oplogRecord)
  case operationUpdate:
		return conv.updated(oplogRecord)
  case operationDelete:
    return conv.deleted(oplogRecord)
	default:
		return fmt.Errorf(`Unknown operatoin "%s" for %+v`, operation, oplogRecord)
	}
}
