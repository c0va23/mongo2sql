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

  converter := Converter {
    ColName: fileName[0:len(fileName)-len(luaExt)],
    LuaState: luaState,
  }

  return &converter, nil
}

// Close converter (LuaState)
func (conv *Converter) Close() {
  conv.LuaState.Close()
}

// Inserted is callback on document insert
func (conv *Converter) Inserted(record map[string]interface{}) error {
  // TODO: Get function only one time
	insertedValue := conv.LuaState.GetGlobal("inserted")

	data, oExists := record["o"].(map[string]interface{})
	if !oExists {
		return fmt.Errorf(`Key "o" not exist for %+v`, record)
	}

	dataTable, dataErr := bsonToTable(conv.LuaState, data)

	if dataErr != nil {
		return dataErr
	}

	return conv.LuaState.CallByParam(lua.P{
		Fn:      insertedValue,
		NRet:    0,
		Protect: true,
	}, dataTable)
}
