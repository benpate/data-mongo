package mongodb

func pointerTo[T any](value T) *T {
	return &value
}
