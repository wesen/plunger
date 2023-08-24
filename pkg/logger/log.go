package logger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"sort"
	"strings"
	"time"
)

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

type Row struct {
	Name string
	Type LogEntryType
}

// MetaKey is used to store keys that are often used, to avoid storing them as full strings.
// Instead, we store them by id and use a lookup table to get the full string.
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

func (m *MetaKeys) Get(name string) (*MetaKey, bool) {
	key, ok := m.Keys[name]
	return key, ok
}

func (m *MetaKeys) GetByID(id int) (*MetaKey, bool) {
	name, ok := m.namesById[id]
	if !ok {
		return nil, false
	}
	return m.Get(name)
}

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

// LogWriter is the main class in Plunger.
//
// It deserializes the JSON binaries handed over by zerolog, and decomposes
// the message into the database schema specified at creation time.
type LogWriter struct {
	db *sqlx.DB

	schema *Schema
}

func NewLogWriter(db *sqlx.DB, schema *Schema) *LogWriter {
	return &LogWriter{
		db:     db,
		schema: schema,
	}
}

func (l *LogWriter) Close() error {
	if l.db != nil {
		return l.db.Close()
	} else {
		return nil
	}
}

func ToLogEntryType(v interface{}) LogEntryType {
	switch v.(type) {
	case float32:
		return LogEntryTypeReal
	case float64:
		return LogEntryTypeReal
	case int:
		return LogEntryTypeReal
	case int8:
		return LogEntryTypeReal
	case int16:
		return LogEntryTypeReal
	case int32:
		return LogEntryTypeReal
	case int64:
		return LogEntryTypeReal
	case uint:
		return LogEntryTypeReal
	case uint8:
		return LogEntryTypeReal
	case uint16:
		return LogEntryTypeReal
	case uint32:
		return LogEntryTypeReal
	case uint64:
		return LogEntryTypeReal

	case string:
		return LogEntryTypeText
	case []byte:
		return LogEntryTypeBlob
	default:
		return LogEntryTypeJSON
	}

}

func (l *LogWriter) Write(p []byte) (int, error) {
	var log map[string]interface{}
	if err := json.Unmarshal(p, &log); err != nil {
		return 0, err
	}

	tx, err := l.db.Beginx()
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			err = tx.Rollback()
			return
		}
		err = tx.Commit()
	}()

	// Insert the log entry
	logEntryID := 0
	q := sqlbuilder.NewInsertBuilder()
	q.InsertInto("log_entries").
		Cols("date", "level", "session").
		Values(time.Now().UTC(), log["level"], log["session"]).
		SQL("RETURNING id")
	s, args := q.Build()
	if err := tx.QueryRowx(s, args...).Scan(&logEntryID); err != nil {
		return 0, err
	}

	// Serialize the log data as log entries meta
	for k, v := range log {
		if k == "level" || k == "session" {
			continue
		}

		var intValue sql.NullInt64
		var realValue sql.NullFloat64
		var textValue, blobValue sql.NullString
		var typeValue LogEntryType
		var name sql.NullString
		var meta_key_id sql.NullInt32

		switch v := v.(type) {
		case float64:
			realValue = sql.NullFloat64{Float64: v, Valid: true}
			typeValue = LogEntryTypeReal
		case []byte:
			blobValue = sql.NullString{String: string(v), Valid: true}
			typeValue = LogEntryTypeBlob
		case string:
			textValue = sql.NullString{String: v, Valid: true}
			typeValue = LogEntryTypeText
		default:
			b, err := json.Marshal(v)
			if err != nil {
				return 0, err
			}
			blobValue = sql.NullString{String: string(b), Valid: true}
			typeValue = LogEntryTypeJSON
		}

		if metaKey, ok := l.schema.MetaKeys.Get(k); ok {
			meta_key_id = sql.NullInt32{Int32: int32(metaKey.ID), Valid: true}
		} else {
			name = sql.NullString{String: k, Valid: true}
		}

		q := sqlbuilder.NewInsertBuilder()
		q.InsertInto("log_entries_meta").
			Cols("log_entry_id", "type", "name", "meta_key_id", "int_value", "real_value", "text_value", "blob_value").
			Values(logEntryID, typeValue, name, meta_key_id, intValue, realValue, textValue, blobValue)
		s, args := q.Build()
		if _, err := tx.Exec(s, args...); err != nil {
			return 0, err
		}
	}

	return len(p), nil
}

type LogEntry struct {
	ID      int       `db:"id"`
	Date    time.Time `db:"date"`
	Level   string    `db:"level"`
	Session *string   `db:"session"`
	Meta    map[string]interface{}
}

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

type GetEntriesFilter struct {
	Level            string
	Session          string
	From             time.Time
	To               time.Time
	SelectedMetaKeys []string
	MetaFilters      map[string]interface{}
}

type GetEntriesFilterOption func(*GetEntriesFilter)

func WithLevel(level string) GetEntriesFilterOption {
	return func(f *GetEntriesFilter) {
		f.Level = level
	}
}

func WithSession(session string) GetEntriesFilterOption {
	return func(f *GetEntriesFilter) {
		f.Session = session
	}
}

func WithFrom(from time.Time) GetEntriesFilterOption {
	return func(f *GetEntriesFilter) {
		f.From = from
	}
}

func WithTo(to time.Time) GetEntriesFilterOption {
	return func(f *GetEntriesFilter) {
		f.To = to
	}
}

func WithSelectedMetaKeys(keys ...string) GetEntriesFilterOption {
	return func(f *GetEntriesFilter) {
		if f.SelectedMetaKeys == nil {
			f.SelectedMetaKeys = []string{}
		}
		f.SelectedMetaKeys = append(f.SelectedMetaKeys, keys...)
	}
}

func WithMetaFilters(filters map[string]interface{}) GetEntriesFilterOption {
	return func(f *GetEntriesFilter) {
		if f.MetaFilters == nil {
			f.MetaFilters = map[string]interface{}{}
		}
		for k, v := range filters {
			f.MetaFilters[k] = v
		}
	}
}

func NewGetEntriesFilter(opts ...GetEntriesFilterOption) *GetEntriesFilter {
	f := &GetEntriesFilter{}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

func (gef *GetEntriesFilter) Apply(metaKeys *MetaKeys, q *sqlbuilder.SelectBuilder) {
	if gef.Level != "" {
		q.Where(q.E("level", gef.Level))
	}
	if gef.Session != "" {
		q.Where(q.E("session", gef.Session))
	}
	if !gef.From.IsZero() {
		q.Where(q.GE("date", gef.From.Format(time.RFC3339)))
	}
	if !gef.To.IsZero() {
		q.Where(q.LE("date", gef.To.Format(time.RFC3339)))
	}
	if len(gef.SelectedMetaKeys) > 0 {
		stringKeys := []string{}
		intKeys := []int{}
		for _, k := range gef.SelectedMetaKeys {
			v, ok := metaKeys.Get(k)
			if !ok {
				stringKeys = append(stringKeys, k)
			} else {
				intKeys = append(intKeys, v.ID)
			}
		}
		exprs := []string{}
		for _, k := range stringKeys {
			exprs = append(exprs, q.In("mk.name", k))
		}
		for _, k := range intKeys {
			exprs = append(exprs, q.In("mk.meta_key_id", k))
		}
		if len(exprs) > 0 {
			q.Where(q.Or(exprs...))
		}
	}

	if len(gef.MetaFilters) > 0 {
		for k, v := range gef.MetaFilters {
			v_, ok := metaKeys.Get(k)
			entryType := ToLogEntryType(v)
			fieldName := entryType.String() + "_value"
			exprs := []string{}
			exprs = append(exprs, q.And(q.E("mk.name", k), q.E(fmt.Sprintf("lem.%s", fieldName), v)))
			if ok {
				exprs = append(exprs, q.And(q.E("mk.meta_key_id", v_.ID), q.E(fmt.Sprintf("lem.%s", fieldName), v)))
			}
			q.Where(q.Or(exprs...))
		}
	}
}

func (l *LogWriter) GetEntries(filter *GetEntriesFilter) ([]*LogEntry, error) {
	if filter == nil {
		filter = NewGetEntriesFilter()
	}

	entries := map[int]*LogEntry{}
	q := sqlbuilder.Select("*").From("log_entries").OrderBy("id ASC")
	filter.Apply(l.schema.MetaKeys, q)
	s2, args := q.Build()
	s2 = l.db.Rebind(s2)
	rows, err := l.db.Queryx(s2, args...)
	if err != nil {
		return nil, err
	}
	defer func(rows *sqlx.Rows) {
		_ = rows.Close()
	}(rows)

	ids := []interface{}{}

	for rows.Next() {
		entry := &LogEntry{}
		if err := rows.StructScan(entry); err != nil {
			return nil, err
		}
		entries[entry.ID] = entry
		ids = append(ids, entry.ID)
	}

	sb := sqlbuilder.Select("lem.*, mk.key AS meta_key").
		From("log_entries_meta lem")

	sb = sb.Where(sb.In("lem.log_entry_id", ids...)).
		JoinWithOption(sqlbuilder.LeftJoin, "meta_keys mk", "mk.id = lem.meta_key_id")

	s, args := sb.Build()
	s = l.db.Rebind(s)
	rows, err = l.db.Queryx(s, args...)
	if err != nil {
		return nil, err
	}
	defer func(rows *sqlx.Rows) {
		_ = rows.Close()
	}(rows)

	for rows.Next() {
		meta := &LogEntryMeta{}
		if err := rows.StructScan(meta); err != nil {
			return nil, err
		}
		entry, ok := entries[meta.LogEntryID]
		if !ok {
			continue
		}

		if entry.Meta == nil {
			entry.Meta = map[string]interface{}{}
		}
		v, err := meta.Value()
		if err != nil {
			return nil, err
		}
		if v == nil {
			continue
		}
		name := ""
		if meta.Name != nil {
			name = *meta.Name
		} else if meta.MetaKey != nil {
			name = *meta.MetaKey
		} else {
			continue
		}
		entry.Meta[name] = v
	}

	ret := []*LogEntry{}
	for _, entry := range entries {
		ret = append(ret, entry)
	}

	// sort by id
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].ID < ret[j].ID
	})

	return ret, nil
}

func (l *LogWriter) Init() error {
	ctb := sqlbuilder.NewCreateTableBuilder()
	ctb.CreateTable("log_entries").
		IfNotExists().
		Define("id", "INTEGER", "PRIMARY KEY", "AUTOINCREMENT").
		Define("date", "TIMESTAMP", "NOT NULL").
		Define("level", "VARCHAR(255)", "NOT NULL").
		Define("session", "VARCHAR(255)")
	if _, err := l.db.Exec(ctb.String()); err != nil {
		return err
	}

	ctb = sqlbuilder.NewCreateTableBuilder()
	ctb.CreateTable("log_entries_meta").
		IfNotExists().
		Define("id", "INTEGER", "PRIMARY KEY", "AUTOINCREMENT").
		Define("log_entry_id", "INTEGER", "NOT NULL").
		Define("type", "INTEGER", "NOT NULL").
		Define("meta_key_id", "INTEGER").
		Define("name", "VARCHAR(255)").
		Define("int_value", "INTEGER").
		Define("real_value", "REAL").
		Define("text_value", "TEXT").
		Define("blob_value", "BLOB")

	if _, err := l.db.Exec(ctb.String()); err != nil {
		return err
	}

	// create indices using raw sql
	indexedColumns := []string{
		"log_entry_id",
		"type",
		"name",
	}
	for _, col := range indexedColumns {
		query := fmt.Sprintf("CREATE INDEX IF NOT EXISTS log_entries_meta_%s_idx ON log_entries_meta (%s)", col, col)
		_, err := l.db.Exec(query)
		if err != nil {
			return err
		}
	}

	ctb = sqlbuilder.NewCreateTableBuilder()
	ctb.CreateTable("meta_keys").
		IfNotExists().
		Define("id", "INTEGER", "PRIMARY KEY NOT NULL").
		Define("key", "VARCHAR(255)")
	if _, err := l.db.Exec(ctb.String()); err != nil {
		return err
	}

	// add unique index on key
	_, err := l.db.Exec("CREATE UNIQUE INDEX IF NOT EXISTS meta_keys_key_idx ON meta_keys (key)")
	if err != nil {
		return err
	}

	err = l.saveSchema()
	if err != nil {
		return err
	}

	err = l.createTypeEnumTable()
	if err != nil {
		return err
	}

	err = l.loadSchema()
	if err != nil {
		return err
	}

	return nil
}

// TODO(manuel, 2023-08-19) Add a function to upgrade previously non-meta keys to a meta key

// TODO(manuel, 2023-08-19) Add a function to add column names straight to the log entries table

// saveSchema stores the schema of the logwriter in the database.
//
// NOTE(manuel, 2023-02-06): This is a very naive implementation.
// It currently blindly overwrites it, but in the future, it will warn
// if there is a schema mismatch with what is already present.
func (l *LogWriter) saveSchema() error {
	err := l.saveMetaKeys()
	if err != nil {
		return err
	}

	return nil
}

func (l *LogWriter) loadSchema() error {
	err := l.loadMetaKeys()
	if err != nil {
		return err
	}

	return nil
}

func (l *LogWriter) createTypeEnumTable() error {
	ctb := sqlbuilder.NewCreateTableBuilder()
	ctb.CreateTable("type_enum").
		IfNotExists().
		Define("type", "VARCHAR(255)", "PRIMARY KEY").
		Define("seq", "INTEGER", "NOT NULL")
	if _, err := l.db.Exec(ctb.String()); err != nil {
		return err
	}

	// Insert the types using InsertBuilder
	q := sqlbuilder.NewInsertBuilder()
	q.InsertInto("type_enum").
		Cols("type", "seq").
		Values("real", LogEntryTypeReal).
		Values("text", LogEntryTypeText).
		Values("blob", LogEntryTypeBlob).
		Values("json", LogEntryTypeJSON).
		SQL("ON CONFLICT (type) DO NOTHING")
	s, args := q.Build()
	if _, err := l.db.Exec(s, args...); err != nil {
		return err
	}

	return nil
}

func (l *LogWriter) saveMetaKeys() error {
	// Insert the keys using InsertBuilder
	if len(l.schema.MetaKeys.Keys) > 0 {
		q := sqlbuilder.NewInsertBuilder()
		q.InsertInto("meta_keys").
			Cols("id", "key")
		for _, v := range l.schema.MetaKeys.Keys {
			q.Values(v.ID, v.Name)
		}
		s, args := q.Build()
		// replace INSERT with INSERT OR REPLACE
		s = strings.Replace(s, "INSERT", "INSERT OR REPLACE", 1)
		if _, err := l.db.Exec(s, args...); err != nil {
			return err
		}
	}

	return nil
}

func (l *LogWriter) loadMetaKeys() error {
	l.schema.MetaKeys = NewMetaKeys()

	s := sqlbuilder.Select("*").From("meta_keys")
	rows, err := l.db.Query(s.String())
	if err != nil {
		return err
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	for rows.Next() {
		var id int
		var key string
		err = rows.Scan(&id, &key)
		if err != nil {
			return err
		}
		_, err = l.schema.MetaKeys.AddWithID(key, id)
		if err != nil {
			return err
		}
	}

	return nil
}
