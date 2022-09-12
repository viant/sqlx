package load

import (
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/load/reader/csv"
	"strings"
)

//BuildSQL builds "LOAD DATA" statement
func BuildSQL(config *csv.Config, readerID, tableName string, columns []io.Column) string {
	sb := strings.Builder{}
	sb.WriteString("LOAD DATA LOCAL INFILE 'Reader::")
	sb.WriteString(readerID)
	sb.WriteString("' INTO TABLE ")
	sb.WriteString(tableName)
	sb.WriteString(" FIELDS TERMINATED BY '")
	sb.WriteString(config.FieldSeparator)
	sb.WriteString("' ESCAPED BY '")
	sb.WriteString(config.EscapeBy)
	sb.WriteString("' ENCLOSED BY '")
	sb.WriteString(config.EncloseBy)
	sb.WriteString("' LINES TERMINATED BY '")
	sb.WriteString(config.ObjectSeparator)
	sb.WriteString("' ")
	sb.WriteString("(")
	for i := 0; i < len(columns); i++ {
		if i != 0 {
			sb.WriteString(",")
		}
		sb.WriteString(columns[i].Name())
	}
	sb.WriteString(")")
	return sb.String()
}
