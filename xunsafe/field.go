package xunsafe

//Field represent a field
type Field struct {
	Index  int
	Field  *Field
	Getter
}
