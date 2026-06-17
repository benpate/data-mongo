package mongodb

import (
	"context"

	"github.com/benpate/data"
	"go.mongodb.org/mongo-driver/mongo"
)

// Session represents a single database session, such as a session encompassing all of the database queries to respond to
// a single REST service call.
type Session struct {
	database *mongo.Database
	context  context.Context
}

// NewSession generates a new Session object from a mongo.Database
func NewSession(database *mongo.Database) Session {
	return Session{
		database: database,
		context:  context.Background(),
	}
}

// Collection returns a reference to an individual database collection.
func (s Session) Collection(collection string) data.Collection {

	return Collection{
		collection: s.database.Collection(collection),
		context:    s.context,
	}
}

// Context returns the context associated with this session.
func (s Session) Context() context.Context {
	return s.context
}

// Close is a no-op that exists to satisfy the data.Session interface.  This
// Session holds no per-session resources: connections are owned by the
// long-lived *mongo.Client pool and recycled automatically after each
// operation.  Callers release per-request resources by cancelling the context
// they passed to Server.Session, not by calling Close.
func (s Session) Close() {
	// Intentionally empty: there are no per-session resources to release.
}

// Mongo returns the underlying mongodb database for libraries that need to bypass this abstraction.
func (s Session) Mongo() *mongo.Database {
	return s.database
}
