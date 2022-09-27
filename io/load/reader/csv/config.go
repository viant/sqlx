package csv

type (
	//Config represents reader config
	Config struct {
		FieldSeparator  string
		ObjectSeparator string
		EncloseBy       string
		EscapeBy        string
		NullValue       string
		Stringify       StringifyConfig
		UniqueFields    []string
		References      []*Reference // parent -> children. Foo.ID -> Boo.FooId
		ExcludedPaths   []string
	}

	// StringifyConfig "extends" Config with ignore flags
	StringifyConfig struct {
		IgnoreFieldSeparator  bool
		IgnoreObjectSeparator bool
		IgnoreEncloseBy       bool
	}
)
