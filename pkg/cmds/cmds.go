package cmds

import (
	"bytes"
	_ "embed"
	"fmt"
	"github.com/go-go-golems/sqleton/pkg/cmds"
)

//go:embed sqleton/logs.yaml
var Logs []byte

func NewLogsCommand(connectionFactory cmds.DBConnectionFactory) (*cmds.SqlCommand, error) {
	reader := bytes.NewReader(Logs)

	scl := &cmds.SqlCommandLoader{DBConnectionFactory: connectionFactory}
	cmds_, err := scl.LoadCommandFromYAML(reader)
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
