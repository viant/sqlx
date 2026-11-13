package metadata

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/sqlx/option"
)

var ErrMetadataCursor = errors.New("metadata cursor does not match query scope")

type metadataCursor struct {
	Query  string `json:"q"`
	Offset int64  `json:"o"`
}
type metadataPage struct {
	option              *option.MetadataPage
	destination, staged reflect.Value
	offset              int64
	query               string
}

// prepareMetadataPage extends the native metadata SQL path, not application SQL.
// MySQL's table/column projections provide stable aliases for page ordering.
func prepareMetadataPage(query *info.Query, statement string, args []interface{}, target Sink, options []option.Option) (string, []interface{}, Sink, *metadataPage, error) {
	var page *option.MetadataPage
	for _, o := range options {
		if p, ok := o.(*option.MetadataPage); ok {
			page = p
			break
		}
	}
	if page == nil {
		return statement, args, target, nil, nil
	}
	page.Handled = false
	page.NextToken = ""
	if !strings.EqualFold(query.Name, "MySQL") || (query.Kind != info.KindTables && query.Kind != info.KindTable) {
		return "", nil, nil, nil, fmt.Errorf("SQL metadata paging is unsupported for %s/%s", query.Name, query.Kind)
	}
	if page.Limit <= 0 || page.Limit > 1000 {
		return "", nil, nil, nil, fmt.Errorf("metadata page limit must be between 1 and 1000")
	}
	var scope *option.Args
	option.Assign(options, &scope)
	if scope == nil || scope.Size() < 2 {
		return "", nil, nil, nil, fmt.Errorf("metadata paging requires a database/schema")
	}
	values := scope.Unwrap()
	schema, ok := values[1].(string)
	if !ok || strings.TrimSpace(schema) == "" {
		return "", nil, nil, nil, fmt.Errorf("metadata paging requires a database/schema")
	}
	destination := reflect.ValueOf(target)
	if destination.Kind() != reflect.Pointer || destination.IsNil() || destination.Elem().Kind() != reflect.Slice {
		return "", nil, nil, nil, fmt.Errorf("paged metadata requires a slice destination")
	}
	bytes, err := json.Marshal([]any{query.Name, query.Kind, statement, args})
	if err != nil {
		return "", nil, nil, nil, err
	}
	digest := sha256.Sum256(bytes)
	identity := base64.RawURLEncoding.EncodeToString(digest[:])
	offset := int64(0)
	if page.Token != "" {
		raw, e := base64.RawURLEncoding.DecodeString(page.Token)
		var cursor metadataCursor
		if e != nil || json.Unmarshal(raw, &cursor) != nil || cursor.Query != identity || cursor.Offset < 0 || cursor.Offset > 1<<40 {
			return "", nil, nil, nil, ErrMetadataCursor
		}
		offset = cursor.Offset
	}
	order := "TABLE_NAME"
	if query.Kind == info.KindTable {
		order += ", ORDINAL_POSITION"
	}
	statement += " ORDER BY " + order + " LIMIT ? OFFSET ?"
	args = append(args, page.Limit+1, offset)
	staged := reflect.New(destination.Elem().Type())
	return statement, args, staged.Interface(), &metadataPage{option: page, destination: destination.Elem(), staged: staged.Elem(), offset: offset, query: identity}, nil
}
func (p *metadataPage) finish() {
	count := p.staged.Len()
	if int64(count) > p.option.Limit {
		count = int(p.option.Limit)
		token, _ := json.Marshal(metadataCursor{Query: p.query, Offset: p.offset + int64(count)})
		p.option.NextToken = base64.RawURLEncoding.EncodeToString(token)
	}
	p.destination.Set(reflect.AppendSlice(p.destination, p.staged.Slice(0, count)))
	p.option.Handled = true
}
