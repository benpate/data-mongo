package mongodb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPointerTo(t *testing.T) {

	// The returned pointer must point to a copy holding the original value.
	intPointer := pointerTo(42)
	require.NotNil(t, intPointer)
	assert.Equal(t, 42, *intPointer)

	stringPointer := pointerTo("hello")
	require.NotNil(t, stringPointer)
	assert.Equal(t, "hello", *stringPointer)
}
