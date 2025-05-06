package mongodb

import (
	"context"

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

// Count returns the total number of records contained by this iterator
func (iterator Iterator) Count() int {
	return iterator.Cursor.RemainingBatchLength()
}

// Next populates the next value from the wrapped Cursor, or returns FALSE
func (iterator Iterator) Next(output any) bool {

	if !iterator.Cursor.Next(iterator.Context) {
		return false
	}

	if err := iterator.Cursor.Decode(output); err != nil {
		return false
	}

	return true
}

// Close closes the wrapped Cursor
func (iterator Iterator) Close() error {

	if err := iterator.Cursor.Close(iterator.Context); err != nil {
		return derp.InternalError("mongodb.Iterator.Close", err.Error())
	}

	return nil
}

func (iterator Iterator) Error() error {
	return iterator.Cursor.Err()
}
