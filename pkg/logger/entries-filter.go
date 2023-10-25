package logger

import (
	"fmt"
	"github.com/huandu/go-sqlbuilder"
	"time"
)

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
