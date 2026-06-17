package mongodb

import (
	"context"
	"testing"
	"time"

	"github.com/benpate/data"
	"github.com/benpate/data/journal"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

/******************************************
 * MongoDB Test Gate
 ******************************************/

// testMongoURI is the connection string used by every integration test.
//
// directConnection=true tells the driver to talk to this single node directly
// instead of performing replica-set discovery.  Without it, a node that
// advertises an unreachable hostname (a common Docker setup) makes every
// operation hang until it times out.
const testMongoURI = "mongodb://localhost:27017/?directConnection=true"

// getTestServer connects to a local MongoDB and returns a ready-to-use Server.
//
// It is the GATE for every test that needs a live database: if MongoDB is not
// reachable the calling test is SKIPPED (not failed), so the suite still passes
// on machines without MongoDB installed.  Each call creates its own throw-away
// database, which is dropped — along with the connection — during cleanup.
func getTestServer(t *testing.T) Server {
	t.Helper()

	// A unique database name keeps parallel tests from colliding.
	databaseName := "test_datamongo_" + primitive.NewObjectID().Hex()

	server, err := New(testMongoURI, databaseName, options.Client())
	require.NoError(t, err)

	// mongo.Connect is lazy, so Ping is what actually proves the server is up.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := server.Client().Ping(ctx, readpref.Primary()); err != nil {
		_ = server.Client().Disconnect(context.Background())
		t.Skipf("MongoDB is not available at %s; skipping integration test (%v)", testMongoURI, err)
	}

	t.Cleanup(func() {
		_ = server.Database().Drop(context.Background())
		_ = server.Client().Disconnect(context.Background())
	})

	return server
}

// getTestCollection returns a Collection backed by a fresh, empty test database.
func getTestCollection(t *testing.T) Collection {
	t.Helper()

	server := getTestServer(t)

	session, err := server.Session(context.Background())
	require.NoError(t, err)

	return session.Collection("testPeople").(Collection)
}

/******************************************
 * Shared Test Model
 ******************************************/

// testPerson is a minimal data.Object used to exercise CRUD operations.  The
// embedded journal.Journal supplies everything the data.Object interface needs
// except for ID().
type testPerson struct {
	PersonID        primitive.ObjectID `bson:"_id"`
	Name            string             `bson:"name"`
	Age             int                `bson:"age"`
	journal.Journal `bson:"journal"`
}

// Compile-time proof that *testPerson satisfies the data.Object interface.
var _ data.Object = (*testPerson)(nil)

// newTestPerson builds a brand-new (unsaved) testPerson with a unique ID.
func newTestPerson(name string, age int) *testPerson {
	return &testPerson{
		PersonID: primitive.NewObjectID(),
		Name:     name,
		Age:      age,
	}
}

// ID implements the data.Object interface.
func (person *testPerson) ID() string {
	return person.PersonID.Hex()
}

// badIDObject is a data.Object whose ID() is not a valid Mongo ObjectID, used
// to exercise the "update" error path in Collection.Save.
type badIDObject struct {
	journal.Journal
}

var _ data.Object = (*badIDObject)(nil)

// ID implements the data.Object interface with a deliberately invalid hex value.
func (b *badIDObject) ID() string {
	return "this-is-not-a-valid-object-id"
}

// markOld stamps a CreateDate so the object reports IsNew() == false, forcing
// Save down its UPDATE branch.
func (b *badIDObject) markOld() {
	b.CreateDate = time.Now().UnixMilli()
}
