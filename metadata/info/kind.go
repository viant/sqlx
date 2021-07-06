package info

import "fmt"

const (
	Catalog  = "Catalog"
	Schema   = "Schema"
	Table    = "Table"
	Index    = "Index"
	View     = "View"
	Sequence = "Sequence"
	Function = "Function"
)

type Kind int

const (
	KindVersion = Kind(iota)
	KindCatalogs
	KindCatalog
	KindCurrentSchema
	KindSchemas
	KindSchema
	KindTables
	KindTable
	KindViews
	KindView
	KindPrimaryKeys
	KindForeignKeys
	KindConstraints
	KindIndexes
	KindIndex
	KindSequences
	KindFunctions
	KindForeignKeysCheckOn
	KindForeignKeysCheckOff
	KindReserved
)

func (k Kind) String() string {
	switch k {
	case KindVersion:
		return "Version"
	case KindCatalogs:
		return "Catalogs"
	case KindCatalog:
		return "Catalog"
	case KindCurrentSchema:
		return "CurrentSchema"
	case KindSchemas:
		return "Schemas"
	case KindSchema:
		return "Schema"
	case KindTables:
		return "Tables"
	case KindTable:
		return "Table"
	case KindViews:
		return "Views"
	case KindView:
		return "View"
	case KindPrimaryKeys:
		return "PrimaryKeys"
	case KindForeignKeys:
		return "ForeignKeys"
	case KindConstraints:
		return "Constraints"
	case KindIndexes:
		return "KindIndexes"
	case KindIndex:
		return "KindIndex"
	case KindSequences:
		return "Sequences"
	case KindFunctions:
		return "Functions"
	case KindForeignKeysCheckOn:
		return "KindForeignKeysCheckOn"
	case KindForeignKeysCheckOff:
		return "KindForeignKeysCheckOff"

	}
	return fmt.Sprintf("undefined: %v", k)
}

var emptyCriteria = []string{}

//Criteria defines criteria for each query kind
func (k Kind) Criteria() []string {
	switch k {
	case KindVersion:
		return emptyCriteria
	case KindCatalogs:
		return emptyCriteria
	case KindCatalog:
		return []string{Catalog}
	case KindCurrentSchema:
		return emptyCriteria
	case KindSchemas:
		return []string{Catalog}
	case KindSchema:
		return []string{Catalog, Schema}
	case KindTables:
		return []string{Catalog, Schema}
	case KindTable:
		return []string{Catalog, Schema, Table}
	case KindViews:
		return []string{Catalog, Schema}
	case KindView:
		return []string{Catalog, Schema, View}
	case KindPrimaryKeys:
		return []string{Catalog, Schema, Table}
	case KindForeignKeys:
		return []string{Catalog, Schema, Table}
	case KindConstraints:
		return []string{Catalog, Schema, Table}
	case KindIndexes:
		return []string{Catalog, Schema, Table}
	case KindIndex:
		return []string{Catalog, Schema, Table, Index}
	case KindSequences:
		return []string{Catalog, Schema, Sequence}
	case KindFunctions:
		return []string{Catalog, Schema, Function}
	case KindForeignKeysCheckOn:
		return []string{Catalog, Schema, Table}
	case KindForeignKeysCheckOff:
		return []string{Catalog, Schema, Table}
	}
	return emptyCriteria
}
