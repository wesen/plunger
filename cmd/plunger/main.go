package main

import (
	"fmt"
	clay "github.com/go-go-golems/clay/pkg"
	"github.com/go-go-golems/plunger/pkg/logger"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	"time"
)

var logCmd = &cobra.Command{
	Use:   "log",
	Short: "Log a message",
	Run: func(cmd *cobra.Command, args []string) {
		force, _ := cmd.Flags().GetBool("force")

		metaKeys, _ := cmd.Flags().GetStringSlice("meta-keys")

		schema := logger.NewSchema()

		for _, metaKey := range metaKeys {
			schema.MetaKeys.Add(metaKey)
		}

		logWriter, err := initConfigAndLogging(force, schema)
		cobra.CheckErr(err)

		defer func(logWriter *logger.LogWriter) {
			err := logWriter.Close()
			if err != nil {
				fmt.Println(err)
			}
		}(logWriter)

		log.Debug().Msg("hello world")
		log.Info().
			Int("int", 32).
			Ints("ints", []int{1, 2, 3}).
			Str("string", "hello").
			Time("time", time.Now()).
			Msg("hello world")
	},
}

func initConfigAndLogging(deleteFile bool, schema *logger.Schema) (*logger.LogWriter, error) {
	err := clay.InitViper("plunger", rootCmd)
	cobra.CheckErr(err)

	logLevel := viper.GetString("log-level")
	verbose := viper.GetBool("verbose")
	if verbose && logLevel != "trace" {
		logLevel = "debug"
	}

	config := &logger.LoggerConfig{
		WithCaller: viper.GetBool("with-caller"),
		Level:      logLevel,
		DBFile:     viper.GetString("db"),
		Schema:     schema,
	}

	if deleteFile {
		err = os.Remove(config.DBFile)
		if err != nil {
			fmt.Println(err)
		}
	}

	logWriter, _, err := logger.InitLogging(config)
	cobra.CheckErr(err)

	return logWriter, err
}

var rootCmd = &cobra.Command{
	Use: "plunger",
}

func init() {
	rootCmd.PersistentFlags().String("db", "", "Database file")

	rootCmd.AddCommand(logCmd)
	logCmd.Flags().Bool("force", false, "Delete the log file before starting")
	logCmd.Flags().StringSlice("meta-keys", []string{}, "Meta keys")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
