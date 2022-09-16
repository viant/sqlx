package load

import (
	"strings"
)

//BuildSQL builds "LOAD" statement
func BuildSQL(loadFormat, readerID, loadHint, tableID string) string {

	//"LOAD 'Reader:csv:123' /*+ LOAD_CONFIG_HINT +*/ DATA INTO TABLE tableID",
	sb := strings.Builder{}
	sb.WriteString("LOAD 'Reader:")
	sb.WriteString(loadFormat)
	sb.WriteString(":")
	sb.WriteString(readerID)
	sb.WriteString("' ")

	if loadHint != "" {
		sb.WriteString("/*+ ")
		sb.WriteString(loadHint)
		sb.WriteString(" +*/ ")
	}

	sb.WriteString("DATA INTO TABLE ")
	sb.WriteString(tableID)

	return sb.String()
}
