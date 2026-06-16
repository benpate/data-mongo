package mongodb

import (
	"context"
	"net/http"

	"github.com/benpate/derp"
	"go.mongodb.org/mongo-driver/mongo"
)

// Iterator wraps the mongodb Cursor object
type Iterator struct {
	Context context.Context
	Cursor  *mongo.Cursor
}

// NewIterator returns a fully populated Iterator object
func NewIterator(context context.Context, cursor *mongo.Cursor) Iterator {
	return Iterator{
		Context: context,
		Cursor:  cursor,
	}
}

// Count returns the total number of records contained by this iterator.  A
// cursor-less iterator (for example, one returned alongside an error) is empty.
func (iterator Iterator) Count() int {
	if iterator.Cursor == nil {
		return 0
	}
	return iterator.Cursor.RemainingBatchLength()
}

// Next populates the next value from the wrapped Cursor, or returns FALSE.  A
// cursor-less iterator is always exhausted.
func (iterator Iterator) Next(output any) bool {

	if iterator.Cursor == nil {
		return false
	}

	if !iterator.Cursor.Next(iterator.Context) {
		return false
	}

	if err := iterator.Cursor.Decode(output); err != nil {
		return false
	}

	return true
}

// Close closes the wrapped Cursor.  A cursor-less iterator has nothing to close.
func (iterator Iterator) Close() error {

	const location = "data-mongo.Iterator.Close"

	if iterator.Cursor == nil {
		return nil
	}

	if err := iterator.Cursor.Close(iterator.Context); err != nil {
		return derp.Wrap(err, location, "Closing cursor", derp.WithCode(http.StatusInternalServerError))
	}

	return nil
}

// Error returns any error encountered while iterating the wrapped Cursor.
func (iterator Iterator) Error() error {
	if iterator.Cursor == nil {
		return nil
	}
	return iterator.Cursor.Err()
}
