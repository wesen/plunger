package main

import (
	"fmt"
	"github.com/go-go-golems/plunger/pkg/logger"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
	"io"
	"os"
)

func main() {
	schema := logger.NewSchema()
	schema.MetaKeys.Add("foo")
	schema.MetaKeys.Add("bar")

	logger, db, err := logger.InitLogging(&logger.LoggerConfig{
		WithCaller: false,
		Level:      "info",
		DBFile:     "/tmp/test.db",
		Schema:     schema,
	})
	if err != nil {
		panic(err)
	}

	defer func(db *sqlx.DB) {
		_ = logger.Close()
	}(db)

	err = logger.Init()
	if err != nil {
		panic(err)
	}

	multi := io.MultiWriter(logger, os.Stdout)

	// use logger as zerolog.Logger
	logger_ := zerolog.New(multi).With().Timestamp().Logger()

	logger_.Info().Msg("hello world")
	logger_.Info().Str("foo", "foo").Msg("hello world")
	logger_.Info().Str("bar", "bar").Msg("hello world")
	logger_.Info().Str("foo", "foo").Str("bar", "bar").Msg("hello world")

	// add additional keys
	logger_.Info().Str("foo", "foo").Str("bar", "bar").Str("baz", "baz").Msg("hello world")
	// add integer keys
	logger_.Info().Int("foo", 1).Int("bar", 2).Int("baz", 3).Msg("hello world")
	// add array keys
	logger_.Info().Ints("foo", []int{1, 2, 3}).Ints("bar", []int{4, 5, 6}).Ints("baz", []int{7, 8, 9}).Msg("hello world")

	entries, err := logger.GetEntries(nil)
	if err != nil {
		panic(err)
	}

	for _, entry := range entries {
		fmt.Printf("%+v\n", entry)
	}
}
