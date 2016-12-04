package dataframe

import "bytes"

// MultiError contains multiple errors.
type MultiError struct {
	Sep    string
	Errors []error
}

func (e *MultiError) Error() string {
	bf := bytes.NewBufferString("")

	for i, err := range e.Errors {
		if i > 0 {
			bf.WriteString(e.Sep)
		}

		bf.WriteString(err.Error())
	}

	return bf.String()
}
