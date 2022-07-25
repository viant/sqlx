package cache

type LineReader interface {
	ReadLine() (line []byte, prefix bool, err error)
}

func ReadLine(reader LineReader) ([]byte, error) {
	line, prefix, err := reader.ReadLine()
	if err != nil {
		return nil, err
	}

	var restLine []byte
	for prefix {
		restLine, prefix, err = reader.ReadLine()
		if err != nil {
			return nil, err
		}
		line = append(line, restLine...)
	}

	return line, nil
}
