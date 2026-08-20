package mongodb

import (
	"context"
	"net/http"

	"github.com/benpate/derp"
	"go.mongodb.org/mongo-driver/mongo"
)

// Iterator wraps the mongodb Cursor object
type Iterator struct {
	context context.Context
	cursor  *mongo.Cursor
	err     error
}

// NewIterator returns a fully populated Iterator object
func NewIterator(ctx context.Context, cursor *mongo.Cursor) *Iterator {
	return &Iterator{
		context: ctx,
		cursor:  cursor,
	}
}

// Count returns the number of records remaining in the cursor's CURRENT batch,
// which is the most the driver can report without another round trip to the
// server.  For a result set larger than one batch this is less than the total.
func (iterator *Iterator) Count() int {
	if iterator.cursor == nil {
		return 0
	}
	return iterator.cursor.RemainingBatchLength()
}

// Next populates the next value from the wrapped Cursor, or returns FALSE.  A
// cursor-less iterator is always exhausted.
func (iterator *Iterator) Next(output any) bool {

	const location = "data-mongo.Iterator.Next"

	if iterator.cursor == nil {
		return false
	}

	if !iterator.cursor.Next(iterator.context) {
		return false
	}

	// RULE: A record that will not decode must be REPORTED, not skipped.  Cursor
	// errors reach the caller through cursor.Err(), but a decode failure happens
	// on this side of the wire, so it is recorded here or it is lost -- and a
	// silent short read looks exactly like a clean end of iteration.
	if err := iterator.cursor.Decode(output); err != nil {
		iterator.err = derp.Wrap(err, location, "Decoding database record", derp.WithCode(http.StatusInternalServerError))
		return false
	}

	return true
}

// Close closes the wrapped Cursor.  A cursor-less iterator has nothing to close.
func (iterator *Iterator) Close() error {

	const location = "data-mongo.Iterator.Close"

	if iterator.cursor == nil {
		return nil
	}

	if err := iterator.cursor.Close(iterator.context); err != nil {
		return derp.Wrap(err, location, "Closing cursor", derp.WithCode(http.StatusInternalServerError))
	}

	// Nothing left to see here
	return nil
}

// Error returns the first error encountered while iterating, whether it came
// from decoding a record or from the wrapped Cursor itself.
func (iterator *Iterator) Error() error {

	// A decode error is always the first one: Next stops iterating on it.
	if iterator.err != nil {
		return iterator.err
	}

	if iterator.cursor == nil {
		return nil
	}

	return iterator.cursor.Err()
}
