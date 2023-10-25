package logger

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/huandu/go-sqlbuilder"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"sort"
	"strings"
	"time"
)

// LogWriter is the main class in Plunger.
//
// It deserializes the JSON binaries handed over by zerolog, and decomposes
// the message into the database schema specified at creation time.
type LogWriter struct {
	db *sqlx.DB

	schema *Schema
}

var _ io.Writer = (*LogWriter)(nil)

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

		// NOTE(manuel, 2023-10-22) Honestly this is all preemptive optimization, I actually don't know if this is necessary.
		// Maybe the app using the logger could instead just give which columns should be used.

		// If we have a metakey for this key, use its id for storage.
		if metaKey, ok := l.schema.MetaKeys.Get(k); ok {
			meta_key_id = sql.NullInt32{Int32: int32(metaKey.ID), Valid: true}
		} else {
			name = sql.NullString{String: k, Valid: true}
		}

		q := sqlbuilder.NewInsertBuilder()
		// NOTE(manuel, 2023-10-22) We could probably collect the values and do only a single insert with all the values at once
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
