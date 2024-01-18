package cmds

import (
	"embed"
	_ "embed"
	"fmt"
	"github.com/go-go-golems/clay/pkg/sql"
	"github.com/go-go-golems/sqleton/pkg/cmds"
)

//go:embed sqleton
var fs embed.FS

func NewLogsCommand(connectionFactory sql.DBConnectionFactory) (*cmds.SqlCommand, error) {
	scl := &cmds.SqlCommandLoader{DBConnectionFactory: connectionFactory}

	cmds_, err := scl.LoadCommands(fs, "sqleton/logs.yaml", nil, nil)
	if err != nil {
		return nil, err
	}

	if len(cmds_) != 1 {
		return nil, fmt.Errorf("expected 1 command, got %d", len(cmds_))
	}

	sqlCommand, ok := cmds_[0].(*cmds.SqlCommand)
	if !ok {
		return nil, fmt.Errorf("expected *pkg.SqlCommand, got %T", cmds_[0])
	}

	return sqlCommand, nil
}
