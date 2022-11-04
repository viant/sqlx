package info

import "fmt"

const (
	//Region defines catalog region (only for cloud database)
	Region = "Region"
	//Catalog defines catalog kind literal
	Catalog = "Catalog"
	//Schema defines schema kind literal
	Schema = "Schema"
	//Table defines table kind literal
	Table = "Table"
	//Index defines index kind literal
	Index = "Restriction"
	//View defines view kind literal
	View = "View"
	//Sequence defines sequence kind literal
	Sequence = "Sequence"
	//Function defines function kind literal
	Function = "Function"
	// SequenceNewCurrentValue defines kind literal
	SequenceNewCurrentValue = "SequenceNewCurrentValue"
	// Object defines kind literal
	Object = "Object"
)

//Kind represents dictionary info kind
type Kind int

const (
	//KindVersion defines information kind
	KindVersion = Kind(iota)
	//KindCatalogs defines catalogs kind
	KindCatalogs
	//KindCatalog defines catalog kind
	KindCatalog
	//KindCurrentSchema defines current schema kind
	KindCurrentSchema
	//KindSchemas defines schemas kind
	KindSchemas
	//KindSchema defines schema kind
	KindSchema
	//KindTables defines tabkes kind
	KindTables
	//KindTable defines table kind
	KindTable
	//KindViews defines views kind
	KindViews
	//KindView defines view kind
	KindView
	//KindPrimaryKeys defines primary keys kind
	KindPrimaryKeys
	//KindForeignKeys defines foreign key kind
	KindForeignKeys
	//KindConstraints defines constraints kind
	KindConstraints
	//KindIndexes defines indexes kind
	KindIndexes
	//KindIndex defines index kind
	KindIndex
	//KindSequences defines sequences kind
	KindSequences
	//KindFunctions defines functions kind
	KindFunctions
	//KindSession defines session kind
	KindSession
	//KindForeignKeysCheckOn defines fk check on kind
	KindForeignKeysCheckOn
	//KindForeignKeysCheckOff defines fk check off kind
	KindForeignKeysCheckOff
	// KindSequenceNextValue defines setting next value of sequence/autoincrement/identity kind
	KindSequenceNextValue
	// KindLockTableAllRowsNoWait defines lock for all table rows
	KindLockTableAllRowsNoWait
	// KindLockGet defines lock get kind
	KindLockGet
	// KindLockRelease defines lock release kind
	KindLockRelease
	//KindReserved defines reserved kind
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
	case KindSession:
		return "KindSession"
	case KindSequenceNextValue:
		return "KindSequenceNextValue"
	case KindLockTableAllRowsNoWait:
		return "KindLockTableAllRowsNoWait"
	case KindLockGet:
		return "KindLockGet"
	case KindLockRelease:
		return "KindLockRelease"
	}
	return fmt.Sprintf("undefined kind: %v", int(k))
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
	case KindSequenceNextValue:
		return []string{Catalog, Schema, Object, SequenceNewCurrentValue}
	case KindLockTableAllRowsNoWait:
		return []string{Catalog, Schema, Table}
	case KindLockGet:
		return []string{Catalog, Schema, Table}
	case KindLockRelease:
		return []string{Catalog, Schema, Table}
	}
	return emptyCriteria
}
