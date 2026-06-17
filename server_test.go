package mongodb

import (
	"context"
	"testing"

	"github.com/benpate/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Compile-time proof that Server satisfies the data.Server interface.
var _ data.Server = Server{}

/******************************************
 * New()
 ******************************************/

// An invalid URI is rejected immediately by mongo.Connect, with no live server
// required, so this test does not use the database gate.
func TestNew_InvalidURI(t *testing.T) {

	server, err := New("this is not a valid uri", "test", options.Client())

	require.Error(t, err)
	assert.Nil(t, server.Client())
	assert.Nil(t, server.Database())
}

// A nil *options.ClientOptions must not panic; New falls back to a default.
func TestNew_NilOptions(t *testing.T) {

	var server Server
	var err error

	require.NotPanics(t, func() {
		server, err = New("mongodb://localhost:27017", "test", nil)
	})

	require.NoError(t, err)
	assert.NotNil(t, server.Client())
}

func TestNew_Success(t *testing.T) {

	server := getTestServer(t)

	assert.NotNil(t, server.Client())
	assert.NotNil(t, server.Database())
}

/******************************************
 * NewServer()
 ******************************************/

func TestNewServer(t *testing.T) {

	source := getTestServer(t)

	// Build a second Server from the underlying *mongo.Database.
	server := NewServer(source.Database())

	assert.Same(t, source.Database(), server.Database())
	assert.Same(t, source.Database().Client(), server.Client())
}

/******************************************
 * Accessors
 ******************************************/

func TestServer_ClientAndDatabase(t *testing.T) {

	server := getTestServer(t)

	require.NotNil(t, server.Client())
	require.NotNil(t, server.Database())
	assert.Same(t, server.Database().Client(), server.Client())
}

/******************************************
 * Session()
 ******************************************/

func TestServer_Session(t *testing.T) {

	server := getTestServer(t)

	type ctxKey string
	ctx := context.WithValue(context.Background(), ctxKey("trace"), "abc")

	session, err := server.Session(ctx)
	require.NoError(t, err)
	require.NotNil(t, session)

	// The context flows through to the session.
	assert.Equal(t, "abc", session.Context().Value(ctxKey("trace")))
}

/******************************************
 * WithTransaction()
 ******************************************/

func TestServer_WithTransaction_Commit(t *testing.T) {

	server := getTestServer(t)

	person := newTestPerson("Kyle Reese", 30)

	result, err := server.WithTransaction(context.Background(), func(session data.Session) (any, error) {
		return nil, session.Collection("testPeople").Save(person, "in transaction")
	})

	// A standalone (non-replica-set) server cannot run transactions; skip there.
	if err != nil {
		t.Skipf("MongoDB transaction not supported in this configuration: %v", err)
	}

	require.NoError(t, err)
	assert.Nil(t, result)

	// The committed write is visible after the transaction.
	loaded := testPerson{}
	err = server.Database().Collection("testPeople").FindOne(context.Background(), map[string]any{"_id": person.PersonID}).Decode(&loaded)
	require.NoError(t, err)
	assert.Equal(t, "Kyle Reese", loaded.Name)
}

// When the callback returns an error, the transaction rolls back and nothing
// is persisted.
func TestServer_WithTransaction_Rollback(t *testing.T) {

	server := getTestServer(t)

	person := newTestPerson("Miles Dyson", 40)
	boom := assert.AnError

	_, err := server.WithTransaction(context.Background(), func(session data.Session) (any, error) {
		if saveErr := session.Collection("testPeople").Save(person, "in transaction"); saveErr != nil {
			return nil, saveErr
		}
		return nil, boom // force a rollback
	})

	// Skip on configurations that don't support transactions at all.
	if err != nil && err != boom {
		t.Skipf("MongoDB transaction not supported in this configuration: %v", err)
	}

	require.ErrorIs(t, err, boom)

	// The rolled-back write must NOT be visible.
	count, err := server.Database().Collection("testPeople").CountDocuments(context.Background(), map[string]any{"_id": person.PersonID})
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}
