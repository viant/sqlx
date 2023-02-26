package validator

import (
	"fmt"
	"reflect"
	"strings"
)

type (
	Violation struct {
		Path    string
		Field   string
		Value   interface{}
		Message string
		Reason  string
	}

	Validation struct {
		Violation []*Violation
		Failed    bool
	}
)

func (e *Validation) AppendNotNull(path *Path, field, msg string) {
	if msg == "" {
		msg = fmt.Sprintf("Field validation for '%v' failed; value is null", field)
	}
	e.Violation = append(e.Violation, &Violation{
		Path:    path.String(),
		Field:   field,
		Message: msg,
		Reason:  string(CheckKidNotNull),
	})
}

func (e *Validation) AppendUnique(path *Path, field string, value interface{}, msg string) {
	value = derefIfNeeded(value)
	if msg == "" {
		msg = fmt.Sprintf("Field validation for '%v' failed; value '%v' is not unique", field, value)
	} else {
		msg = strings.Replace(msg, "$value", fmt.Sprintf("%v", value), 1)
	}

	e.Violation = append(e.Violation, &Violation{
		Path:    path.String(),
		Field:   field,
		Value:   value,
		Message: msg,
		Reason:  string(CheckKidUnique),
	})
}

func derefIfNeeded(value interface{}) interface{} {
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
		value = v.Interface()
	}
	return value
}

func (e *Validation) AppendRef(path *Path, field string, value interface{}, msg string) {
	value = derefIfNeeded(value)
	if msg == "" {
		msg = fmt.Sprintf("Field validation for '%v' failed; ref key '%v' does not exists ", field, value)
	} else {
		msg = strings.Replace(msg, "$value", fmt.Sprintf("%v", value), 1)
	}
	e.Violation = append(e.Violation, &Violation{
		Path:    path.String(),
		Field:   field,
		Value:   value,
		Message: msg,
		Reason:  string(CheckKidRefKey),
	})
}

func (e *Validation) String() string {
	if e == nil || len(e.Violation) == 0 {
		return ""
	}
	msg := strings.Builder{}
	for i, v := range e.Violation {
		if i > 0 {
			msg.WriteString(",")
		}
		msg.WriteString(v.Message)
	}
	return msg.String()
}

func (e *Validation) Error() string {
	return e.String()
}
