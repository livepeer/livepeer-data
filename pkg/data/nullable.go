package data

// Nullable represents a field that can be null, 0, or omitted from the
// serialized JSON (if omitempty is used). It is basically a double pointer for
// the optional fields so we can represent all possible "nullish" values:
//
// - the field was not asked for and should be omitted: outer pointer nil
// - the field was asked but not valid (null): outer pointer non-nil, inner pointer nil
// - the field value is "zero": outer pointer non-nil, inner pointer to &zero
//
// If the field is not nullish, it's just a double pointer to the value.
type Nullable[T any] **T

func ToNullable[T any](val T, valid, asked bool) Nullable[T] {
	if !asked {
		return nil
	}

	if !valid {
		var null *T
		return &null
	}

	ptr := &val
	return &ptr
}
