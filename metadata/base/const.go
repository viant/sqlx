package base

//QueryKind represents info type
type QueryKind int


const (
	QueryKindVersion = QueryKind(iota)
	QueryKindCatalogs
	QueryKindCatalog
	QueryKindSchemas
	QueryKindSchema
	QueryKindTables
	QueryKindTable
	QueryKindViews
	QueryKindView
	QueryKindPrimaryKeys
	QueryKindExportedKeys
	QueryKindIndexes
	QueryKindIndex
	QueryKindSequences
	QueryKindSequence
	QueryKindAutoIncrement
)


