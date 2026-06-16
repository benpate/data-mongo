package mongodb

import (
	"context"
	"testing"

	"github.com/benpate/exp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
)

/******************************************
 * NewIterator()
 ******************************************/

func TestNewIterator(t *testing.T) {

	ctx := context.Background()
	cursor := &mongo.Cursor{}

	iterator := NewIterator(ctx, cursor)

	assert.Equal(t, ctx, iterator.Context)
	assert.Same(t, cursor, iterator.Cursor)
}

// A cursor-less iterator (the zero value, or one returned alongside an error)
// must behave like an empty iterator instead of panicking on a nil cursor.
func TestIterator_NilCursor(t *testing.T) {

	iterator := Iterator{} // no cursor

	assert.NotPanics(t, func() {
		assert.Equal(t, 0, iterator.Count())
		assert.False(t, iterator.Next(&testPerson{}))
		assert.NoError(t, iterator.Error())
		assert.NoError(t, iterator.Close())
	})
}

/******************************************
 * Iterator (live cursor)
 ******************************************/

func TestIterator_Next(t *testing.T) {

	collection := getTestCollection(t)
	require.NoError(t, collection.Save(newTestPerson("John Connor", 20), "seed"))
	require.NoError(t, collection.Save(newTestPerson("Sarah Connor", 45), "seed"))

	iterator, err := collection.Iterator(exp.All())
	require.NoError(t, err)
	t.Cleanup(func() { _ = iterator.Close() })

	// Walk the whole cursor, collecting the names it yields.
	names := make([]string, 0)
	for {
		person := testPerson{}
		if !iterator.Next(&person) {
			break
		}
		names = append(names, person.Name)
	}

	assert.ElementsMatch(t, []string{"John Connor", "Sarah Connor"}, names)
	assert.NoError(t, iterator.Error())
}

// Next returns FALSE once the cursor is exhausted.
func TestIterator_NextExhausted(t *testing.T) {

	collection := getTestCollection(t)
	require.NoError(t, collection.Save(newTestPerson("Solo", 30), "seed"))

	iterator, err := collection.Iterator(exp.All())
	require.NoError(t, err)
	t.Cleanup(func() { _ = iterator.Close() })

	person := testPerson{}
	assert.True(t, iterator.Next(&person))  // first (and only) row
	assert.False(t, iterator.Next(&person)) // exhausted
}

func TestIterator_Count(t *testing.T) {

	collection := getTestCollection(t)
	require.NoError(t, collection.Save(newTestPerson("A", 1), "seed"))
	require.NoError(t, collection.Save(newTestPerson("B", 2), "seed"))
	require.NoError(t, collection.Save(newTestPerson("C", 3), "seed"))

	iterator, err := collection.Iterator(exp.All())
	require.NoError(t, err)
	t.Cleanup(func() { _ = iterator.Close() })

	// Count reports the remaining rows in the current batch (all 3 fit one batch).
	assert.Equal(t, 3, iterator.Count())
}

func TestIterator_Close(t *testing.T) {

	collection := getTestCollection(t)
	require.NoError(t, collection.Save(newTestPerson("John Connor", 20), "seed"))

	iterator, err := collection.Iterator(exp.All())
	require.NoError(t, err)

	assert.NoError(t, iterator.Close())
}

func TestIterator_Error(t *testing.T) {

	collection := getTestCollection(t)
	require.NoError(t, collection.Save(newTestPerson("John Connor", 20), "seed"))

	iterator, err := collection.Iterator(exp.All())
	require.NoError(t, err)
	t.Cleanup(func() { _ = iterator.Close() })

	// A healthy cursor reports no error.
	assert.NoError(t, iterator.Error())
}
