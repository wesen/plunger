package cmds

import (
	"bytes"
	_ "embed"
	"fmt"
	"github.com/go-go-golems/sqleton/pkg"
)

//go:embed sqleton/logs.yaml
var Logs []byte

func NewLogsCommand(connectionFactory pkg.DBConnectionFactory) (*pkg.SqlCommand, error) {
	reader := bytes.NewReader(Logs)

	scl := &pkg.SqlCommandLoader{DBConnectionFactory: connectionFactory}
	cmds, err := scl.LoadCommandFromYAML(reader)
	if err != nil {
		return nil, err
	}

	if len(cmds) != 1 {
		return nil, fmt.Errorf("expected 1 command, got %d", len(cmds))
	}

	sqlCommand, ok := cmds[0].(*pkg.SqlCommand)
	if !ok {
		return nil, fmt.Errorf("expected *pkg.SqlCommand, got %T", cmds[0])
	}

	return sqlCommand, nil
}
