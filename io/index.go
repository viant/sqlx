package io

// index keeps native matching tiers separate. An inferred spelling cannot
// occupy an authored exact tag or its case-insensitive lookup entry.
type index map[indexKey]uint16

type indexKey struct {
	name string
	tier uint8
}

func (i *index) match(name string) int {
	for tier, key := range []string{name, byLowerCase(name), fuzzyKey(name)} {
		if position, ok := (*i)[indexKey{name: key, tier: uint8(tier)}]; ok {
			return int(position)
		}
	}
	return -1
}

func (i *index) add(name string, position int, inferred bool) {
	keys := []string{name, byLowerCase(name)}
	if inferred {
		keys = append(keys, fuzzyKey(name))
	}
	for tier, key := range keys {
		lookup := indexKey{name: key, tier: uint8(tier)}
		if _, exists := (*i)[lookup]; !exists {
			(*i)[lookup] = uint16(position)
		}
	}
}
