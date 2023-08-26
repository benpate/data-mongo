package mongodb

import (
	"context"

	"github.com/benpate/data"
	"github.com/benpate/derp"
	"go.mongodb.org/mongo-driver/mongo"
)

// Session represents a single database session, such as a session encompassing all of the database queries to respond to
// a single REST service call.
type Session struct {
	database *mongo.Database
	context  context.Context
}

// Collection returns a reference to an individual database collection.
func (s Session) Collection(collection string) data.Collection {

	return Collection{
		collection: s.database.Collection(collection),
		context:    s.context,
	}
}

// Close cleans up any remaining connections that need to be removed.
func (s Session) Close() {
	if err := s.database.Client().Disconnect(s.context); err != nil {
		derp.Report(derp.Wrap(err, "mongodb.Session.Close", "Error disconnecting from database"))
	}
}

// Mongo returns the underlying mongodb client for libraries that need to bypass this abstraction.
func (s Session) Mongo() *mongo.Database {
	return s.database
}
