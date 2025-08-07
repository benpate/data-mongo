package mongodb

import (
	"context"
	"time"

	"github.com/benpate/data"
	"github.com/benpate/derp"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

// Server is an abstract representation of a MongoDB database.  It implements the data.Server interface,
// so that it should be usable anywhere that requires a data.Server.
type Server struct {
	client   *mongo.Client
	database *mongo.Database
}

// New returns a fully populated mongodb.Server.  It requires that you provide the URI for the mongodb
// cluster, along with the name of the database to be used for all transactions.
func New(uri string, database string, opt *options.ClientOptions) (Server, error) {

	const location = "data.mongodb.New"

	// Connect to the server
	client, err := mongo.Connect(context.Background(), opt.ApplyURI(uri))

	if err != nil {
		return Server{}, derp.Wrap(err, location, "Unable to connect to mongodb server", uri, database)
	}

	// Return a wrapped "data.Server" value
	result := Server{
		client:   client,
		database: client.Database(database),
	}

	return result, nil
}

func NewServer(database *mongo.Database) Server {
	return Server{
		client:   database.Client(),
		database: database,
	}
}

// Mongo returns the underlying mongodb client for libraries that need to bypass this abstraction.
func (server Server) Client() *mongo.Client {
	return server.client
}

func (server Server) Database() *mongo.Database {
	return server.database
}

// Session returns a new client session that can be used to perform CRUD transactions on this datastore.
func (server Server) Session(ctx context.Context) (data.Session, error) {
	return Session{
		database: server.database,
		context:  ctx,
	}, nil
}

func (server Server) WithTransaction(ctx context.Context, fn data.TransactionCallbackFunc) (any, error) {

	const location = "data-mongo.server.WithTransaction"

	sessionOptions := options.Session().
		SetCausalConsistency(true).
		SetDefaultReadConcern(readconcern.Majority()).
		SetDefaultWriteConcern(writeconcern.Majority()).
		SetDefaultReadPreference(readpref.Primary()).
		SetDefaultMaxCommitTime(pointerTo(time.Minute))

	// Start a new Session just for this Transaction
	mongoSession, err := server.Client().StartSession(sessionOptions)

	if err != nil {
		return nil, derp.Wrap(err, location, "Unable to start database session")
	}

	defer mongoSession.EndSession(ctx)

	// Execute a MongoDB transaction with the new session, passing a standard
	// data.Session to the callback
	return mongoSession.WithTransaction(ctx, func(ctx mongo.SessionContext) (any, error) {

		// Build a Session
		session := Session{
			database: server.database,
			context:  ctx,
		}

		// Execute the Transaction
		return fn(session)
	})
}
