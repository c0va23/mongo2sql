package converter

import (
  "fmt"
  "path"
  "database/sql"
  "log"
  "strings"

	"github.com/yuin/gopher-lua"
)

// Converter load LUA for handle mongo opload record
type Converter struct {
  DbName string
  ColName string
  LuaState *lua.LState
  sqlDb *sql.DB
  insertedFunc *lua.LFunction
  updatedFunc *lua.LFunction
  deletedFunc *lua.LFunction
}

// Document is record of oplog collection
type Document map[string]interface{}

const luaExt = ".lua"

func parseName(name string) (dbName string, collName string, err error) {
  nameParts := strings.Split(name, ".")
  if 2 != len(nameParts) {
    return "", "", fmt.Errorf("Invalid name: %s", name)
  }

  dbName = nameParts[0]
  collName = nameParts[1]

  if 0 == len(dbName) {
    err = fmt.Errorf("Invalid database name: %s", dbName)
  } else if 0 == len(collName) {
    err = fmt.Errorf("Invalid collection name: %s", collName)
  }

  return
}

// New create new converter
func New(filePath string, sqlDb *sql.DB) (*Converter, error) {
  fileName := path.Base(filePath)

  if fileExt := path.Ext(fileName); luaExt != fileExt {
    return nil, fmt.Errorf(`File "%s" should have extension "%s".`, filePath, luaExt)
  }

  fullName := fileName[0:len(fileName)-len(luaExt)]
  dbName, collName, nameErr := parseName(fullName)
  if nil != nameErr {
    return nil, nameErr
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
    DbName: dbName,
    ColName: collName,
    LuaState: luaState,
    sqlDb: sqlDb,
    insertedFunc: insertedFunc,
    updatedFunc: updatedFunc,
    deletedFunc: deletedFunc,
  }

  luaState.Register("exec", converter.exec)

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
	value, oExists := record["o"]
	if !oExists {
		return fmt.Errorf(`Key "o" not exist for %+v`, record)
	}

  doc, isDoc := value.(Document)
  if !isDoc {
    return fmt.Errorf(`Inavlid doc: %+v`, value)
  }

  return conv.Inserted(doc)
}

// Inserted handler new records
func (conv *Converter) Inserted(doc Document) error {
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


func (conv *Converter) updated(record Document) error {
  queryValue, queryExists := record["o2"]
	if !queryExists {
		return fmt.Errorf(`Key "o2" not exist for %+v`, record)
	}

	query, queryDoc := queryValue.(Document)
  if !queryDoc {
    return fmt.Errorf(`Invalid document: %+v`, queryValue)
  }

	queryTable, tableErr := bsonToTable(conv.LuaState, query)

	if tableErr != nil {
		return tableErr
	}

  updateValue, updateExists := record["o"]
  if !updateExists {
		return fmt.Errorf(`Key "o" not exist for %+v`, record)
  }

  update, updateDoc := updateValue.(Document)
  if !updateDoc {
    return fmt.Errorf(`Invalid document: %+v`, updateValue)
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

func (conv *Converter) deleted(record Document) error {
	queryValue, queryExists := record["o"]
	if !queryExists {
		return fmt.Errorf(`Key "o" not exist for %+v`, record)
	}

  query, queryDoc := queryValue.(Document)
  if !queryDoc {
		return fmt.Errorf(`Invalid document %+v`, queryValue)
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
func (conv *Converter) ProcessOplogRecord(oplogRecord Document) error {
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

func (conv *Converter) exec(L *lua.LState) int {
  query := L.ToString(1)
  args := make([]interface{}, 0, L.GetTop() - 1)

  for i := 2; i <= L.GetTop(); i++ {
    switch value := L.Get(i).(type) {
    case lua.LString:
      args = append(args, string(value))
    case lua.LNumber:
      args = append(args, float64(value))
    case lua.LBool:
      args = append(args, bool(value))
    case *lua.LNilType:
      args = append(args, nil)
    default:
      log.Printf("Unknown value: %+v", value)
    }
  }

  log.Printf(`Exec "%s" with %+v`, query, args)

  if result, err := conv.sqlDb.Exec(query, args...); nil != err {
    log.Print(err)
    L.Push(lua.LBool(false))
  } else if rowsAffected, err := result.RowsAffected(); nil != err {
    log.Print(err)
    L.Push(lua.LBool(false))
  } else {
    log.Printf("Affected %d rows", rowsAffected)
    L.Push(lua.LBool(true))
  }
  return 1
}

// FullName return databse name + collection name
func (conv *Converter) FullName() string {
  return conv.DbName + "." + conv.ColName
}
