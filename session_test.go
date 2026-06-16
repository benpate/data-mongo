package mongodb

import (
	"context"
	"testing"

	"github.com/benpate/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Compile-time proof that Session satisfies the data.Session interface.
var _ data.Session = Session{}

/******************************************
 * NewSession()
 ******************************************/

func TestNewSession(t *testing.T) {

	server := getTestServer(t)

	session := NewSession(server.Database())

	assert.Same(t, server.Database(), session.Mongo())
	// NewSession defaults to a background context.
	assert.Equal(t, context.Background(), session.Context())
}

/******************************************
 * Collection()
 ******************************************/

func TestSession_Collection(t *testing.T) {

	server := getTestServer(t)

	type ctxKey string
	ctx := context.WithValue(context.Background(), ctxKey("trace"), "xyz")

	session, err := server.Session(ctx)
	require.NoError(t, err)

	collection := session.Collection("testPeople")
	require.NotNil(t, collection)

	// The session's context propagates into the collection.
	assert.Equal(t, "xyz", collection.Context().Value(ctxKey("trace")))
}

/******************************************
 * Context()
 ******************************************/

func TestSession_Context(t *testing.T) {

	server := getTestServer(t)

	type ctxKey string
	ctx := context.WithValue(context.Background(), ctxKey("trace"), "123")

	session, err := server.Session(ctx)
	require.NoError(t, err)

	assert.Equal(t, ctx, session.Context())
}

/******************************************
 * Mongo()
 ******************************************/

func TestSession_Mongo(t *testing.T) {

	server := getTestServer(t)

	session, err := server.Session(context.Background())
	require.NoError(t, err)

	mongoSession := session.(Session)
	assert.Same(t, server.Database(), mongoSession.Mongo())
}

/******************************************
 * Close()
 ******************************************/

// Close is currently a no-op, but it must remain safe to call.
func TestSession_Close(t *testing.T) {

	server := getTestServer(t)

	session, err := server.Session(context.Background())
	require.NoError(t, err)

	assert.NotPanics(t, session.Close)
}
