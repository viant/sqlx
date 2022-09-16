package load

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	vBigquery "github.com/viant/bigquery/reader"
	"github.com/viant/sqlx/io"
	readerCsv "github.com/viant/sqlx/io/load/reader/csv"
	readerJson "github.com/viant/sqlx/io/load/reader/json"
	readerParquet "github.com/viant/sqlx/io/load/reader/parquet"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
	"google.golang.org/api/bigquery/v2"
	goIo "io"
	"strings"
)

const (
	formatCSV              = "CSV"
	formatJSON             = "JSON"
	formatPARQUET          = "PARQUET"
	formatConfigLoaderJSON = "NEWLINE_DELIMITED_JSON"
)

var loadConfig = &readerCsv.Config{
	FieldSeparator:  ",",
	ObjectSeparator: "\n", // Just the "\n" character is valid
	EncloseBy:       `"`,
	EscapeBy:        `"`, // EscapeBy must be equal to EncloseBy
	NullValue:       ``,
	Stringify: readerCsv.StringifyConfig{ // For BigQuery set all: IgnoreFieldSeparator, IgnoreObjectSeparator, IgnoreEncloseBy to true
		IgnoreFieldSeparator:  true,
		IgnoreObjectSeparator: true,
		IgnoreEncloseBy:       true,
	},
}

//Session represents BigQuery session
type Session struct {
	dialect *info.Dialect
}

//NewSession returns new BigQuery session
func NewSession(dialect *info.Dialect) io.Session {
	return &Session{
		dialect: dialect,
	}
}

//Exec loads given data to database
func (s *Session) Exec(ctx context.Context, data interface{}, db *sql.DB, tableName string, options ...option.Option) (sql.Result, error) {
	loadFormat := option.Options(options).LoadFormat()
	loadHint := option.Options(options).LoadHint()

	if err := s.normalizeLoadConfig(loadHint, loadFormat); err != nil {
		return nil, err
	}

	dataReader, err := s.getReader(loadFormat, data)
	if err != nil {
		return nil, err
	}
	readerID := uuid.New().String()
	err = vBigquery.Register(readerID, dataReader)
	if err != nil {
		return nil, err
	}
	defer vBigquery.Unregister(readerID)

	SQL := BuildSQL(loadFormat, readerID, loadHint, tableName)

	return db.ExecContext(ctx, SQL)
}

func (s *Session) getReader(loadFormat string, data interface{}) (goIo.Reader, error) {
	switch strings.ToUpper(loadFormat) {
	case formatCSV:
		reader, _, err := readerCsv.NewReader(data, loadConfig)
		return reader, err
	case formatJSON:
		return readerJson.NewReader(data)
	case formatPARQUET:
		return readerParquet.NewReader(data)
	default:
		return nil, fmt.Errorf("unsupported format: %s, supported[%s|%s|%s]", loadFormat, formatCSV, formatJSON, formatPARQUET)
	}
}

func (s *Session) normalizeLoadConfig(loadHint string, loadFormat string) error {
	if loadHint == "" {
		return nil
	}

	var config = bigquery.JobConfigurationLoad{}
	if err := json.Unmarshal([]byte(loadHint), &config); err != nil {
		return err
	}

	loadHintFormat := strings.ToUpper(config.SourceFormat)
	if err := s.checkLoadFormat(loadHintFormat, loadFormat); err != nil {
		return err
	}

	if strings.ToUpper(loadFormat) != formatCSV {
		return nil
	}

	loadConfig.NullValue = config.NullMarker

	if config.FieldDelimiter == "" {
		return fmt.Errorf("field FieldDelimiter passed by LoadHint option is empty")
	}
	loadConfig.FieldSeparator = config.FieldDelimiter

	if config.Quote == nil || *config.Quote == "" {
		return fmt.Errorf("field Quote passed by LoadHint option is empty")
	}
	loadConfig.EncloseBy = *config.Quote
	loadConfig.EscapeBy = loadConfig.EncloseBy // EncloseBy and EscapeBy must be the same

	return nil
}

func (s *Session) checkLoadFormat(loadHintFormat string, loadFormat string) error {
	inSet := false

	for _, v := range []string{"CSV", "DATASTORE_BACKUP", "NEWLINE_DELIMITED_JSON", "AVRO", "PARQUET", "ORC"} {
		if v == loadHintFormat {
			inSet = true
		}
	}

	if !inSet {
		return fmt.Errorf("unnsuported format passed by LoadHint option: \"%s\", supported: [%s]", loadHintFormat, `"CSV", "DATASTORE_BACKUP", "NEWLINE_DELIMITED_JSON", "AVRO", "PARQUET", "ORC"`)
	}

	if loadHintFormat == formatConfigLoaderJSON {
		loadHintFormat = formatJSON
	}

	if loadHintFormat != strings.ToUpper(loadFormat) {
		return fmt.Errorf("inconsistent formats found: passed by LoadFormat == \"%s\" <> passed by LoadHint == \"%s\"", strings.ToUpper(loadFormat), strings.ToUpper(loadHintFormat))
	}

	return nil
}
