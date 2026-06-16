package mongodb

import (
	"context"
	"net/http"
	"strconv"
	"time"

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

// Context returns the context associated with this collection.
func (c Collection) Context() context.Context {
	return c.context
}

// Count returns the number of records in the collection that match the provided criteria.
func (c Collection) Count(criteria exp.Expression, _ ...option.Option) (int64, error) {

	const location = "data-mongo.Collection.Count"

	// TODO: LOW: Add options to this function
	// https://pkg.go.dev/go.mongodb.org/mongo-driver/mongo/options#CountOptions

	criteriaBSON := ExpressionToBSON(criteria)
	defer c.reportIfSlow(location, startTimer(), criteriaBSON)

	count, err := c.collection.CountDocuments(c.context, criteriaBSON)

	if err != nil {
		return 0, derp.Wrap(err, location, "Unable to count objects", criteriaBSON, derp.WithCode(http.StatusInternalServerError))
	}

	return count, nil
}

// Query retrieves a group of objects from the database and populates a target interface
func (c Collection) Query(target any, criteria exp.Expression, options ...option.Option) error {

	const location = "data-mongo.Collection.Query"

	criteriaBSON := ExpressionToBSON(criteria)
	defer c.reportIfSlow(location, startTimer(), criteriaBSON)

	optionsBSON := findOptions(options...)
	cursor, err := c.collection.Find(c.context, criteriaBSON, optionsBSON)

	if err != nil {
		return derp.Wrap(err, location, "Unable to list objects", criteriaBSON, options, derp.WithCode(http.StatusInternalServerError))
	}

	if err := cursor.All(c.context, target); err != nil {
		return derp.Wrap(err, location, "Unable to unmarshal database objects", target, criteriaBSON, options)
	}

	return nil
}

// Iterator retrieves a group of objects from the database as an iterator
func (c Collection) Iterator(criteria exp.Expression, options ...option.Option) (data.Iterator, error) {

	const location = "data-mongo.Collection.Iterator"

	criteriaBSON := ExpressionToBSON(criteria)
	defer c.reportIfSlow(location, startTimer(), criteriaBSON)

	optionsBSON := findOptions(options...)
	cursor, err := c.collection.Find(c.context, criteriaBSON, optionsBSON)

	if err != nil {
		return NewIterator(c.context, cursor), derp.Wrap(err, location, "Error Listing Objects", criteria, criteriaBSON, options, derp.WithCode(http.StatusInternalServerError))
	}

	iterator := NewIterator(c.context, cursor)

	return iterator, nil
}

// Load retrieves a single object from the database
func (c Collection) Load(criteria exp.Expression, target data.Object, options ...option.Option) error {

	const location = "data-mongo.Collection.Load"

	criteriaBSON := ExpressionToBSON(criteria)
	defer c.reportIfSlow(location, startTimer(), criteriaBSON)

	optionsBSON := findOneOptions(options...)

	// Try to query the database
	if err := c.collection.FindOne(c.context, criteriaBSON, optionsBSON).Decode(target); err != nil {

		if err == mongo.ErrNoDocuments {
			return derp.NotFound("mongodb.Load", "Unable to load object", err.Error(), criteria, criteriaBSON, target)
		}

		return derp.Internal("mongodb.Load", "Unable to load object", err.Error(), criteria, criteriaBSON, target)
	}

	return nil
}

// Save inserts/updates a single object in the database.
func (c Collection) Save(object data.Object, note string) error {

	const location = "data-mongo.Collection.Save"

	// object.ID() is read lazily, since an INSERT may assign it during this call.
	startTime := startTimer()
	defer func() { c.reportIfSlow(location, startTime, object.ID()) }()

	object.SetUpdated(note)

	// If new, then INSERT the object
	if object.IsNew() {
		object.SetCreated(note)

		if _, err := c.collection.InsertOne(c.context, object); err != nil {
			return derp.Wrap(err, location, "Unable to insert object", object, derp.WithBadRequest())
		}

		return nil
	}

	// Fall through to here means UPDATE object

	objectID, err := primitive.ObjectIDFromHex(object.ID())

	if err != nil {
		return derp.Internal(location, "Unable to generate objectID", err, object)
	}

	filter := bson.M{"_id": objectID}

	if _, err := c.collection.ReplaceOne(c.context, filter, object); err != nil {
		return derp.Wrap(err, location, "Unable to replace object", filter, object, derp.WithBadRequest())
	}

	return nil
}

// Delete removes a single object from the database, using a "virtual delete"
func (c Collection) Delete(object data.Object, note string) error {

	const location = "data-mongo.Collection.Delete"

	defer c.reportIfSlow(location, startTimer(), object.ID())

	if object.IsNew() {
		return derp.BadRequest(location, "Unable to delete a new object", object, note)
	}

	// Use virtual delete to mark this object as deleted.
	object.SetDeleted(note)

	if err := c.Save(object, note); err != nil {
		return derp.Wrap(err, location, "Unable to perform virtual delete", object, derp.WithCode(http.StatusInternalServerError))
	}

	return nil
}

// HardDelete physically removes an object from the database.
func (c Collection) HardDelete(criteria exp.Expression) error {

	const location = "data-mongo.Collection.HardDelete"

	criteriaBSON := ExpressionToBSON(criteria)
	defer c.reportIfSlow(location, startTimer(), criteriaBSON)

	if _, err := c.collection.DeleteMany(c.context, criteriaBSON); err != nil {
		return derp.Wrap(err, location, "Unable to hard delete object", criteria)
	}

	return nil
}

// Mongo returns the underlying mongodb collection for libraries that need to bypass this abstraction.
func (c Collection) Mongo() *mongo.Collection {
	return c.collection
}

// reportIfSlow logs a slow-query warning when the time elapsed since startTime
// exceeds the configured threshold.  It is meant to be deferred at the top of
// each query method.
func (c Collection) reportIfSlow(location string, startTime int64, data ...any) {
	if isTimeoutExceeded(startTime) {
		c.timeoutError(location, startTime, data...)
	}
}

func (c Collection) timeoutError(location string, startTime int64, data ...any) {

	data = append([]any{
		"time: " + strconv.FormatInt(time.Now().UnixMilli()-startTime, 10) + "ms",
		"collection: " + c.collection.Name(),
	}, data...)

	derp.Report(derp.TimeoutError(
		location,
		"Timeout exceeded",
		data...,
	))
}
