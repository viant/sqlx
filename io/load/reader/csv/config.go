package csv

//Config represents reader config
type Config struct {
	FieldSeparator  string
	ObjectSeparator string
	EncloseBy       string
	EscapeBy        string
	NullValue       string
	Stringify       StringifyConfig
}

// StringifyConfig "extends" Config with ignore flags
type StringifyConfig struct {
	IgnoreFieldSeparator  bool
	IgnoreObjectSeparator bool
	IgnoreEncloseBy       bool
}
