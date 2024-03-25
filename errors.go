package bitcask

import (
	"fmt"
)

func withPrefix(text string) error {
	return fmt.Errorf("bitcask: %s", text)
}

func withErr(err error) error {
	if err == nil {
		return nil
	}
	return withPrefix(err.Error())
}

var (
	ErrPathIsDir       = withPrefix("path is a dir")
	ErrKeyTooLarge     = withPrefix("key is too large")
	ErrHeaderCorrupted = withPrefix("header is corrupted")
	ErrKeyNotFound     = withPrefix("key not found")
	ErrEntryCorrupted  = withPrefix("entry is corrupted")
)
