package mongodb

import (
	"context"
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

func (c Collection) Context() context.Context {
	return c.context
}

func (c Collection) Count(criteria exp.Expression, _ ...option.Option) (int64, error) {

	const location = "data-mongo.Collection.Count"

	var startTime int64

	if logTimeout > 0 {
		startTime = time.Now().UnixMilli()
	}

	// TODO: LOW: Add options to this function
	// https://pkg.go.dev/go.mongodb.org/mongo-driver/mongo/options#CountOptions

	criteriaBSON := ExpressionToBSON(criteria)
	count, err := c.collection.CountDocuments(c.context, criteriaBSON)

	if err != nil {
		return 0, derp.InternalError(location, "Unable to count objects", err.Error(), criteriaBSON)
	}

	if isTimeoutExceeded(startTime) {
		c.timeoutError(location, startTime, criteriaBSON)
	}

	return count, nil
}

// Query retrieves a group of objects from the database and populates a target interface
func (c Collection) Query(target any, criteria exp.Expression, options ...option.Option) error {

	const location = "data-mongo.Collection.Query"

	var startTime int64

	if logTimeout > 0 {
		startTime = time.Now().UnixMilli()
	}

	criteriaBSON := ExpressionToBSON(criteria)
	optionsBSON := findOptions(options...)
	cursor, err := c.collection.Find(c.context, criteriaBSON, optionsBSON)

	if isTimeoutExceeded(startTime) {
		c.timeoutError(location, startTime, criteriaBSON)
	}

	if err != nil {
		return derp.InternalError(location, "Unable to list objects", err.Error(), criteriaBSON, options)
	}

	if err := cursor.All(c.context, target); err != nil {
		return derp.Wrap(err, location, "Unable to unmarshal database objects", target, criteriaBSON, options)
	}

	return nil
}

// Iterator retrieves a group of objects from the database as an iterator
func (c Collection) Iterator(criteria exp.Expression, options ...option.Option) (data.Iterator, error) {

	const location = "data-mongo.Collection.Iterator"

	var startTime int64

	if logTimeout > 0 {
		startTime = time.Now().UnixMilli()
	}

	criteriaBSON := ExpressionToBSON(criteria)
	optionsBSON := findOptions(options...)
	cursor, err := c.collection.Find(c.context, criteriaBSON, optionsBSON)

	if isTimeoutExceeded(startTime) {
		c.timeoutError(location, startTime, criteriaBSON)
	}

	if err != nil {
		return NewIterator(c.context, cursor), derp.InternalError(location, "Error Listing Objects", err.Error(), criteria, criteriaBSON, options)
	}

	iterator := NewIterator(c.context, cursor)

	return iterator, nil
}

// Load retrieves a single object from the database
func (c Collection) Load(criteria exp.Expression, target data.Object, options ...option.Option) error {

	const location = "data-mongo.Collection.Load"

	var startTime int64

	if logTimeout > 0 {
		startTime = time.Now().UnixMilli()
	}

	criteriaBSON := ExpressionToBSON(criteria)

	optionsBSON := findOneOptions(options...)

	err := c.collection.FindOne(c.context, criteriaBSON, optionsBSON).Decode(target)

	if isTimeoutExceeded(startTime) {
		c.timeoutError(location, startTime, criteriaBSON)
	}

	if err != nil {

		if err == mongo.ErrNoDocuments {
			return derp.NotFoundError("mongodb.Load", "Unable to load object", err.Error(), criteria, criteriaBSON, target)
		}

		return derp.InternalError("mongodb.Load", "Unable to load object", err.Error(), criteria, criteriaBSON, target)
	}

	return nil
}

// Save inserts/updates a single object in the database.
func (c Collection) Save(object data.Object, note string) error {

	const location = "data-mongo.Collection.Save"

	var startTime int64

	if logTimeout > 0 {
		startTime = time.Now().UnixMilli()
	}

	object.SetUpdated(note)

	// If new, then INSERT the object
	if object.IsNew() {
		object.SetCreated(note)

		if _, err := c.collection.InsertOne(c.context, object); err != nil {
			return derp.Wrap(err, location, "Unable to insert object", object, derp.WithBadRequest())
		}

		if isTimeoutExceeded(startTime) {
			c.timeoutError(location, startTime, object.ID())
		}

		return nil
	}

	// Fall through to here means UPDATE object

	objectID, err := primitive.ObjectIDFromHex(object.ID())

	if err != nil {
		return derp.InternalError(location, "Unable to generate objectID", err, object)
	}

	filter := bson.M{"_id": objectID}

	if _, err := c.collection.ReplaceOne(c.context, filter, object); err != nil {
		return derp.Wrap(err, location, "Unable to replace object", filter, object, derp.WithBadRequest())
	}

	if isTimeoutExceeded(startTime) {
		c.timeoutError(location, startTime, object.ID())
	}

	return nil
}

// Delete removes a single object from the database, using a "virtual delete"
func (c Collection) Delete(object data.Object, note string) error {

	const location = "data-mongo.Collection.Delete"

	var startTime int64

	if logTimeout > 0 {
		startTime = time.Now().UnixMilli()
	}

	if object.IsNew() {
		return derp.BadRequestError(location, "Unable to delete a new object", object, note)
	}

	// Use virtual delete to mark this object as deleted.
	object.SetDeleted(note)

	if err := c.Save(object, note); err != nil {
		return derp.InternalError(location, "Unable to perform virtual delete", err.Error(), object)
	}

	if isTimeoutExceeded(startTime) {
		c.timeoutError(location, startTime, object.ID())
	}

	return nil
}

// HardDelete physically removes an object from the database.
func (c Collection) HardDelete(criteria exp.Expression) error {

	const location = "data-mongo.Collection.HardDelete"

	var startTime int64

	if logTimeout > 0 {
		startTime = time.Now().UnixMilli()
	}

	criteriaBSON := ExpressionToBSON(criteria)

	if _, err := c.collection.DeleteMany(c.context, criteriaBSON); err != nil {
		return derp.Wrap(err, location, "Unable to hard delete object", criteria)
	}

	if isTimeoutExceeded(startTime) {
		c.timeoutError(location, startTime, criteriaBSON)
	}

	return nil
}

// Mongo returns the underlying mongodb collection for libraries that need to bypass this abstraction.
func (c Collection) Mongo() *mongo.Collection {
	return c.collection
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
