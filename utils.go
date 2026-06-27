package mongodb

// pointerTo returns a pointer to a copy of the given value, for APIs (such as
// the mongodb options builders) that take pointer arguments.
func pointerTo[T any](value T) *T {
	return &value
}
