package sink

import "github.com/viant/sqlx"

//Columns represents column callback
type Columns func (column sqlx.Column)(toContinue bool)
