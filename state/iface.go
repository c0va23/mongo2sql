package state

import (
  "time"
)

// Collection describe current collection state
// type Collection struct {
//   Name string
//   Bootstraped bool
//   LastTimestamp *time.Time
// }

// Store is interface for state storages
type Store interface {
  Exists(name string) (bool, error)
  Add(name string) error
  SetBootstraped(name string, bootstraped bool) error
  IsBootstraped(name string) (bool, error)
  UpdateTimestamp(name string, timestamp time.Time) error
  Timestamp(name string) (time.Time, error)
}
