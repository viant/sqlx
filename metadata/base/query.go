package base

import (
	"github.com/viant/sqlx/metadata/dialect/version"
)

//Query represents dialect metadata queries
type Query struct {
	Kind       QueryKind
	SQL        string
	MinVersion string
	info       *version.Info
}

func (q *Query) Init() (err error) {
	q.info, err = version.Parse([]byte(q.MinVersion))
	return err
}

//Queries represents meta data queries
type Queries []Query

func (q Queries) Len() int {
	return len(q)
}

// Swap is part of sort.Interface.
func (q Queries) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

// Less is part of sort.Interface.
func (q Queries) Less(i, j int) bool {
	if q[i].info == nil || q[j].info == nil {
		return true
	}
	return q[i].info.Major < q[j].info.Major && q[i].info.Minor < q[j].info.Minor
}

//Match matches queries for version, or latest version
func (q Queries) Match(info version.Info) Query {
	if len(q) == 1 {
		return q[0]
	}
	for _, candidate := range q {
		if candidate.info.Major >= info.Major {
			if candidate.info.Minor >= info.Minor {
				return candidate
			}
		}
	}
	//by default return the latest version
	return q[len(q)-1]
}
