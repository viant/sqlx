package insert

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/option"
)

func (s *session) sequenceUpdater(options []option.Option) (*numericSequencer, error) {
	field, column := option.Options(options).SequenceField(), option.Options(options).SequenceColumn()
	var candidates, declared, conventional []*numericSequencer
	for _, updater := range s.recordUpdaters {
		n, ok := updater.(*numericSequencer)
		if !ok {
			continue
		}
		if field != "" {
			mapped, ok := n.column.(io.Fielder)
			if !ok {
				continue
			}
			fields := mapped.Fields()
			if len(fields) == 0 || fields[len(fields)-1].Name != field {
				continue
			}
		}
		if column != "" && n.column.Name() != column {
			continue
		}
		candidates = append(candidates, n)
		if tag := n.column.Tag(); tag != nil && (tag.Autoincrement || tag.Sequence != "") {
			declared = append(declared, n)
		}
		if strings.EqualFold(n.column.Name(), "id") {
			conventional = append(conventional, n)
		}
	}
	if field == "" && column == "" {
		if len(declared) > 0 {
			candidates = declared
		} else if len(conventional) > 0 {
			candidates = conventional
		}
	}
	if len(candidates) != 1 {
		return nil, fmt.Errorf("sequence selection for %s matched %d numeric columns; specify SequenceField or SequenceColumn", s.TableName, len(candidates))
	}
	return candidates[0], nil
}

func (n *numericSequencer) reservationValueType() (reflect.Type, error) {
	target := n.column.ScanType()
	for target.Kind() == reflect.Ptr {
		target = target.Elem()
	}
	switch target.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return target, nil
	}
	return nil, fmt.Errorf("mapped sequence column %s has non-integer Go type %s", n.column.Name(), target)
}
