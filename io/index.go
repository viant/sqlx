package io

//index stores the map between struct field and column position
type index map[string]uint16

func (i *index) match(name string) int {
	key := name
	if _, ok := (*i)[key]; ok {
		return int((*i)[key])
	}
	key = byLowerCase(key)
	if _, ok := (*i)[key]; ok {
		return int((*i)[key])
	}
	key = fuzzyKey(key)
	if _, ok := (*i)[key]; ok {
		return int((*i)[key])
	}
	return -1
}

func (i *index) add(name string, index int) {
	val := uint16(index)
	key := name
	if _, ok := (*i)[key]; !ok {
		(*i)[key] = val
	}
	key = byLowerCase(name)
	if _, ok := (*i)[key]; !ok {
		(*i)[key] = val
	}
	key = fuzzyKey(name)
	if _, ok := (*i)[key]; !ok {
		(*i)[key] = val
	}
}
