package metadata


type Kind string

const (
	KindVersion = Kind(iota)
	KindCatalogs
	KindCatalog
	KindSchemas
	KindSchema
	KindTables
	KindTable
	KindViews
	KindView
	KindPrimaryKeys
	KindExportedKeys
	KindIndexes
	KindIndex
	KindSequences
	KindSequence
	KindAutoIncrement
)

