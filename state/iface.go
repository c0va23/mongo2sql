package state

import (
  "time"
)

// Timestamp is mapping mongo Timestamp for pg
type Timestamp struct {
  time.Time
  Ordinal int32
}

// After return true if curemt Timestamp after other
func (t Timestamp) After(o Timestamp) bool {
  return t.Time.After(o.Time) || (t == o && t.Ordinal > o.Ordinal)
}

// Store is interface for state storages
type Store interface {
  Exists(name string) (bool, error)
  Add(name string, timestamp Timestamp) error
  UpdateTimestamp(name string, timestamp Timestamp) error
  GetTimestamp(name string) (Timestamp, error)
}
