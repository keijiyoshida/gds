package dataframe

// Type represents a type of a data frame element.
type Type int

// Types of a data frame element.
const (
	String Type = iota
	Float64
)

// valid checks whether the t is a valid type or not.
func (t Type) valid() bool {
	return t == String || t == Float64
}
