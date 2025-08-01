package mongodb

import (
	"context"

	"github.com/benpate/data"
	"github.com/benpate/derp"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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

	// Connect to the server
	client, err := mongo.Connect(context.Background(), opt.ApplyURI(uri))

	if err != nil {
		return Server{}, derp.Wrap(err, "data.mongodb.New", "Error connecting to mongodb server", uri, database)
	}

	// Return a wrapped "data.Server" value
	result := Server{
		client:   client,
		database: client.Database(database),
	}

	return result, nil
}

// Session returns a new client session that can be used to perform CRUD transactions on this datastore.
func (server Server) Session(ctx context.Context) (data.Session, error) {

	return Session{
		database: server.database,
		context:  ctx,
	}, nil
}

// Mongo returns the underlying mongodb client for libraries that need to bypass this abstraction.
func (server Server) Client() *mongo.Client {
	return server.client
}

func (server Server) Database() *mongo.Database {
	return server.database
}
