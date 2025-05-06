package mongodb

import (
	"context"

	"github.com/benpate/data"
	"github.com/benpate/data/option"
	"github.com/benpate/derp"
	"github.com/benpate/exp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// Collection wraps a mongodb.Collection with all of the methods required by the data.Collection interface
type Collection struct {
	collection *mongo.Collection
	context    context.Context
}

// NewCollection creates a new Collection object directly from a mongo.Collection
func NewCollection(collection *mongo.Collection) Collection {
	return Collection{
		collection: collection,
		context:    context.Background(),
	}
}

func (c Collection) Count(criteria exp.Expression, _ ...option.Option) (int64, error) {

	const location = "data-mongo.collection.Count"

	criteriaBSON := ExpressionToBSON(criteria)
	count, err := c.collection.CountDocuments(c.context, criteriaBSON)

	// TODO: LOW: Add options to this function
	// https://pkg.go.dev/go.mongodb.org/mongo-driver/mongo/options#CountOptions

	if err != nil {
		return 0, derp.InternalError(location, "Error counting objects", err.Error(), criteriaBSON)
	}

	return count, nil
}

// Query retrieves a group of objects from the database and populates a target interface
func (c Collection) Query(target any, criteria exp.Expression, options ...option.Option) error {

	const location = "data-mongo.collection.Query"

	criteriaBSON := ExpressionToBSON(criteria)
	optionsBSON := convertOptions(options...)
	cursor, err := c.collection.Find(c.context, criteriaBSON, optionsBSON)

	if err != nil {
		return derp.InternalError(location, "Error Listing Objects", err.Error(), criteriaBSON, options)
	}

	if err := cursor.All(c.context, target); err != nil {
		return derp.Wrap(err, location, "Error unmarshalling database objects", target, criteriaBSON, options)
	}

	return nil
}

// Iterator retrieves a group of objects from the database as an iterator
func (c Collection) Iterator(criteria exp.Expression, options ...option.Option) (data.Iterator, error) {

	criteriaBSON := ExpressionToBSON(criteria)
	optionsBSON := convertOptions(options...)
	cursor, err := c.collection.Find(c.context, criteriaBSON, optionsBSON)

	if err != nil {
		return NewIterator(c.context, cursor), derp.InternalError("mongodb.List", "Error Listing Objects", err.Error(), criteria, criteriaBSON, options)
	}

	iterator := NewIterator(c.context, cursor)

	return iterator, nil
}

// Load retrieves a single object from the database
func (c Collection) Load(criteria exp.Expression, target data.Object) error {

	criteriaBSON := ExpressionToBSON(criteria)

	if err := c.collection.FindOne(c.context, criteriaBSON).Decode(target); err != nil {

		if err == mongo.ErrNoDocuments {
			return derp.NotFoundError("mongodb.Load", "Error loading object", err.Error(), criteria, criteriaBSON, target)
		}

		return derp.InternalError("mongodb.Load", "Error loading object", err.Error(), criteria, criteriaBSON, target)
	}

	return nil
}

// Save inserts/updates a single object in the database.
func (c Collection) Save(object data.Object, note string) error {

	object.SetUpdated(note)

	// If new, then INSERT the object
	if object.IsNew() {
		object.SetCreated(note)

		if _, err := c.collection.InsertOne(c.context, object); err != nil {
			return derp.InternalError("mongodb.Save", "Error inserting object", err.Error(), object)
		}

		return nil
	}

	// Fall through to here means UPDATE object

	objectID, err := primitive.ObjectIDFromHex(object.ID())

	if err != nil {
		return derp.InternalError("mongodb.Save", "Error generating objectID", err, object)
	}

	filter := bson.M{"_id": objectID}

	if _, err := c.collection.ReplaceOne(c.context, filter, object); err != nil {
		return derp.InternalError("mongodb.Save", "Error replacing object", err.Error(), filter, object)
	}

	return nil
}

// Delete removes a single object from the database, using a "virtual delete"
func (c Collection) Delete(object data.Object, note string) error {

	if object.IsNew() {
		return derp.BadRequestError("mongo.Delete", "Cannot delete a new object", object, note)
	}

	// Use virtual delete to mark this object as deleted.
	object.SetDeleted(note)
	return c.Save(object, note)
}

// HardDelete physically removes an object from the database.
func (c Collection) HardDelete(criteria exp.Expression) error {

	criteriaBSON := ExpressionToBSON(criteria)

	if _, err := c.collection.DeleteMany(c.context, criteriaBSON); err != nil {
		return derp.Wrap(err, "mondodb.HardDelete", "Error performing hard delete", criteria)
	}

	return nil
}

// Mongo returns the underlying mongodb collection for libraries that need to bypass this abstraction.
func (c Collection) Mongo() *mongo.Collection {
	return c.collection
}
