package converter

import (
	"database/sql"
	"log"
	"path/filepath"
)

const convertersDir = "converters"

// Map string to Converter
type Map map[string]*Converter

// Names of converter collections
func (converters Map) Names() []string {
	names := make([]string, 0, len(converters))
	for name := range converters {
		names = append(names, name)
	}
	return names
}

// LoadAll converters
func LoadAll(sqlDb *sql.DB) (Map, error) {
	filePaths, globErr := filepath.Glob(filepath.Join(convertersDir, "*.lua"))
	if nil != globErr {
		return nil, globErr
	}

	converters := make(Map)

	for _, filePath := range filePaths {
		conv, err := New(filePath, sqlDb)
		if nil != err {
			log.Fatal(err)
		}

		defer conv.Close()
		converters[conv.FullName()] = conv
	}

	return converters, nil
}
