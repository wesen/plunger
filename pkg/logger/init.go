package logger

import (
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type LoggerConfig struct {
	WithCaller bool
	Level      string
	DBFile     string
	Schema     *Schema
}

type MissingDBFileError struct {
}

func (e *MissingDBFileError) Error() string {
	return "missing db file"
}

func InitLogging(config *LoggerConfig) (*LogWriter, *sqlx.DB, error) {
	if config.WithCaller {
		log.Logger = log.With().Caller().Logger()
	}

	if config.DBFile == "" {
		return nil, nil, &MissingDBFileError{}
	}

	db, err := sqlx.Open("sqlite3", config.DBFile)
	if err != nil {
		return nil, nil, err
	}

	logWriter := NewLogWriter(db, config.Schema)
	err = logWriter.Init()
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	log.Logger = log.Output(logWriter)

	switch config.Level {
	case "debug":
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	case "info":
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case "warn":
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case "error":
		zerolog.SetGlobalLevel(zerolog.ErrorLevel)
	case "fatal":
		zerolog.SetGlobalLevel(zerolog.FatalLevel)
	}

	return logWriter, db, nil
}
