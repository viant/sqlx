package cache

type Recorder interface {
	AddValues(values []interface{})
	ScanValues(values []interface{})
}
