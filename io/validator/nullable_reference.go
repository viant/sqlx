package validator

import (
	"fmt"
	"strings"

	"github.com/viant/sqlx/io"
)

type referenceMember struct {
	index int
	field string
}

// refKey retains the physical members of one schema-derived composite FK.
// It supplies SQL MATCH SIMPLE nullability, not a replacement for the database's
// complete tuple constraint. Existing nonnull scalar checks remain unchanged.
func compileReferenceMembers(raw string, column io.Column, columns []io.Column, byName map[string]io.Column) ([]referenceMember, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	names := strings.Split(raw, ",")
	if len(names) < 2 {
		return nil, fmt.Errorf("reference group for %s needs multiple columns", column.Name())
	}
	var result []referenceMember
	seen := map[string]bool{}
	own := false
	for _, name := range names {
		name = strings.TrimSpace(name)
		member, ok := byName[name]
		if !ok || name == "" || seen[name] {
			return nil, fmt.Errorf("reference group for %s has missing or repeated column %q", column.Name(), name)
		}
		seen[name] = true
		if name == column.Name() {
			own = true
		}
		tag := member.Tag()
		target := column.Tag()
		if tag == nil || tag.RefTable != target.RefTable || tag.RefDb != target.RefDb || tag.RefColumn == "" {
			return nil, fmt.Errorf("reference group for %s has inconsistent target at %s", column.Name(), name)
		}
		fielder, ok := member.(io.Fielder)
		if !ok || len(fielder.Fields()) == 0 {
			return nil, fmt.Errorf("reference member %s has no field", name)
		}
		fields := fielder.Fields()
		position := -1
		for i, c := range columns {
			if c.Name() == name {
				position = i
				break
			}
		}
		result = append(result, referenceMember{index: position, field: fields[len(fields)-1].Name})
	}
	if !own {
		return nil, fmt.Errorf("reference group for %s omits its own column", column.Name())
	}
	return result, nil
}

func (c *Check) nullableReferenceAt(index, count int, current any, options *Options) (bool, error) {
	if len(c.refMembers) == 0 {
		return false, nil
	}
	var previous any
	if options.candidatePoliciesSet {
		previous = options.CandidatePolicies[index].Previous
	} else if options.Previous != nil {
		at, n, err := io.Values(options.Previous)
		if err != nil {
			return false, fmt.Errorf("reference previous rows: %w", err)
		}
		if n != count {
			return false, fmt.Errorf("reference previous row count %d does not match candidate count %d", n, count)
		}
		previous, err = c.previousRow(current, at(index), index)
		if err != nil {
			return false, err
		}
	}
	for _, member := range c.refMembers {
		if options.deferredAt(index, member.field) {
			return false, nil
		}
	}
	for _, member := range c.refMembers {
		source := current
		if !options.includesAt(index, current, member.field) {
			if previous != nil {
				source = previous
			} else if !options.candidatePoliciesSet && !options.previousSet {
				// A standalone sparse validation without matched Previous does not prove
				// that an omitted member will become SQL NULL. Keep its scalar check.
				return false, nil
			}
		}
		value, err := c.sqlValue(source, member.index)
		if err != nil {
			return false, err
		}
		if value == nil {
			return true, nil
		}
	}
	return false, nil
}
