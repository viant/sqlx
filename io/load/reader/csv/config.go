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

type StringifyConfig struct {
	IgnoreFieldSeparator  bool
	IgnoreObjectSeparator bool
	IgnoreEncloseBy       bool
}
