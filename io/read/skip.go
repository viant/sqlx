package read

type SkipError string

func (s SkipError) Error() string {
	return string(s)
}
