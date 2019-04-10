package internal

import (
	"strings"
	"sync"
)

type MultiError struct {
	mux    sync.Mutex
	errors []error
}

func NewMultiError() *MultiError {
	return &MultiError{}
}

// Error addition in async context supported
func (merr *MultiError) AddError(err error) {
	merr.mux.Lock()
	merr.errors = append(merr.errors, err)
	merr.mux.Unlock()
}

func (merr MultiError) Error() string {
	var b strings.Builder
	for i, err := range merr.errors {
		b.WriteString(err.Error())
		if i < len(merr.errors) {
			b.WriteString(" and ")
		}
	}
	return b.String()
}

func (merr MultiError) Build() error {
	if len(merr.errors) > 0 {
		return merr
	}
	return nil
}
