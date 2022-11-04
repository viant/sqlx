package io

func MergeErrorIfNeeded(currErr error, err *error) {
	if currErr == nil || *err != nil {
		return
	}
	*err = currErr
}
