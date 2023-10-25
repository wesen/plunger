package logger

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"time"
)

// MetaKey is used for often used meta keys, to not store the entire string of the key name,
// but instead only write use an ID. A separate table is used to keep the metakeys.
type MetaKey struct {
	Name string
	ID   int
}

// MetaKeys is a collection of MetaKey. It is used to quickly manage
// adding new keys.
type MetaKeys struct {
	Keys      map[string]*MetaKey
	namesById map[int]string
	maxID     int
}

func NewMetaKeys() *MetaKeys {
	return &MetaKeys{
		Keys:      make(map[string]*MetaKey),
		namesById: make(map[int]string),
		maxID:     0,
	}
}

// Get retrieves a MetaKey from the MetaKeys collection by name. It returns the
// MetaKey and a boolean indicating whether the key was found.
func (m *MetaKeys) Get(name string) (*MetaKey, bool) {
	key, ok := m.Keys[name]
	return key, ok
}

// GetByID retrieves a MetaKey from the MetaKeys collection by ID. It returns the
// MetaKey and a boolean indicating whether the key was found.
func (m *MetaKeys) GetByID(id int) (*MetaKey, bool) {
	name, ok := m.namesById[id]
	if !ok {
		return nil, false
	}
	return m.Get(name)
}

// Add adds a new MetaKey to the MetaKeys collection with the given name. If a
// MetaKey with the same name already exists, it returns the existing MetaKey.
func (m *MetaKeys) Add(name string) *MetaKey {
	key, ok := m.Get(name)
	if ok {
		return key
	}

	key = &MetaKey{
		Name: name,
		ID:   m.maxID,
	}
	m.maxID++
	m.Keys[name] = key
	m.namesById[key.ID] = name
	return key
}

// AddWithID adds a new MetaKey to the MetaKeys collection with the given name and
// ID. If a MetaKey with the same name or ID already exists, it returns an error.
func (m *MetaKeys) AddWithID(name string, id int) (*MetaKey, error) {
	name_, ok := m.namesById[id]
	if ok {
		if name_ != name {
			return nil, fmt.Errorf("key %s already exists with id %d", name_, id)
		}
	}

	key, ok := m.Get(name)
	if ok {
		if key.ID != id {
			return key, fmt.Errorf("key %s already exists with id %d", name, key.ID)
		}
		return key, nil
	}

	key = &MetaKey{
		Name: name,
		ID:   id,
	}
	if id > m.maxID {
		m.maxID = id
	}
	m.maxID++
	m.namesById[id] = name
	m.Keys[name] = key

	return key, nil
}

// Schema is a set of MetaKeys
type Schema struct {
	MetaKeys *MetaKeys
}

func NewSchema() *Schema {
	return &Schema{
		MetaKeys: NewMetaKeys(),
	}
}

// LogEntry represents a log entry. It contains metadata and other information
// about the log entry.
type LogEntry struct {
	ID      int       `db:"id"`
	Date    time.Time `db:"date"`
	Level   string    `db:"level"`
	Session *string   `db:"session"`
	Meta    map[string]interface{}
}

// LogEntryMeta represents metadata for a LogEntry. It contains the type of the
// metadata and its value.
type LogEntryMeta struct {
	ID         int          `db:"id"`
	LogEntryID int          `db:"log_entry_id"`
	Type       LogEntryType `db:"type"`
	Name       *string      `db:"name"`
	MetaKeyID  *int         `db:"meta_key_id"`
	IntValue   *int64       `db:"int_value"`
	RealValue  *float64     `db:"real_value"`
	TextValue  *string      `db:"text_value"`
	BlobValue  *[]byte      `db:"blob_value"`
	MetaKey    *string      `db:"meta_key"`
}

// Value retrieves the value of the LogEntryMeta based on its type. It returns an
// error if the value is nil or if the type is unknown.
func (lem *LogEntryMeta) Value() (interface{}, error) {
	switch lem.Type {
	case LogEntryTypeReal:
		if lem.RealValue == nil {
			return nil, errors.New("real value is nil")
		}
		return *lem.RealValue, nil
	case LogEntryTypeText:
		if lem.TextValue == nil {
			return nil, errors.New("text value is nil")
		}
		return *lem.TextValue, nil
	case LogEntryTypeJSON:
		if lem.BlobValue == nil {
			return nil, errors.New("blob value is nil")
		}
		var v interface{}
		if err := json.Unmarshal(*lem.BlobValue, &v); err != nil {
			return nil, err
		}
		return v, nil
	case LogEntryTypeBlob:
		if lem.BlobValue == nil {
			return nil, errors.New("blob value is nil")
		}
		return *lem.BlobValue, nil
	default:
		return nil, errors.New("unknown type")
	}
}

// LogEntryType represents the different types LogEntries can have.
type LogEntryType int

const (
	LogEntryTypeReal LogEntryType = iota
	LogEntryTypeText
	LogEntryTypeBlob
	LogEntryTypeJSON
)

func (t LogEntryType) String() string {
	switch t {
	case LogEntryTypeReal:
		return "real"
	case LogEntryTypeText:
		return "text"
	case LogEntryTypeBlob:
		return "blob"
	case LogEntryTypeJSON:
		return "json"
	}
	return "unknown"
}

// Row represents a row in a log entry. It contains the name of the row and its type.
type Row struct {
	Name string
	Type LogEntryType
}

// ToLogEntryType converts a value to a LogEntryType based on its type.
func ToLogEntryType(v interface{}) LogEntryType {
	switch v.(type) {
	case float32, float64, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return LogEntryTypeReal
	case string:
		return LogEntryTypeText
	case []byte:
		return LogEntryTypeBlob
	default:
		return LogEntryTypeJSON
	}
}
