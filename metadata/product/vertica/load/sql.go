package load

import (
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/load/reader"
	"strings"
)

//BuildSQL builds "COPY FROM" statement
func BuildSQL(config *reader.Config, tableName string, columns []io.Column) string {
	sb := strings.Builder{}
	sb.WriteString("COPY ")
	sb.WriteString(tableName)
	sb.WriteString("(")
	for i := 0; i < len(columns); i++ {
		if i != 0 {
			sb.WriteString(",")
		}
		sb.WriteString(columns[i].Name())
	}
	sb.WriteString(")")
	sb.WriteString(" FROM STDIN")
	sb.WriteString(" DELIMITER AS '")
	sb.WriteString(config.FieldSeparator)
	sb.WriteString("' ENCLOSED BY '")
	sb.WriteString(config.EncloseBy)
	sb.WriteString("' ESCAPE AS '")
	sb.WriteString(config.EscapeBy)
	sb.WriteString("' RECORD TERMINATOR '")
	sb.WriteString(config.ObjectSeparator)
	sb.WriteString("'")
	//sb.WriteString(" AUTO") //allowed options: AUTO (is default) / DIRECT / TRICKLE
	sb.WriteString(" NO COMMIT")
	return sb.String()
}
