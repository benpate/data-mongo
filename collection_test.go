package mongodb

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/benpate/data"
	"github.com/benpate/data/option"
	"github.com/benpate/derp"
	"github.com/benpate/exp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// Compile-time proof that Collection satisfies the data.Collection interface.
var _ data.Collection = Collection{}

// seedPeople saves a batch of people into the collection, failing the test on
// any error.
func seedPeople(t *testing.T, collection Collection, people ...*testPerson) {
	t.Helper()
	for _, person := range people {
		require.NoError(t, collection.Save(person, "seed"))
	}
}

/******************************************
 * NewCollection() / Accessors
 ******************************************/

func TestNewCollection(t *testing.T) {

	raw := getTestServer(t).Database().Collection("raw")

	collection := NewCollection(raw)

	assert.Same(t, raw, collection.Mongo())
	assert.Equal(t, context.Background(), collection.Context())
}

func TestCollection_Context(t *testing.T) {

	server := getTestServer(t)

	type ctxKey string
	ctx := context.WithValue(context.Background(), ctxKey("trace"), "abc")

	session, err := server.Session(ctx)
	require.NoError(t, err)

	collection := session.Collection("testPeople").(Collection)
	assert.Equal(t, "abc", collection.Context().Value(ctxKey("trace")))
}

/******************************************
 * Save() - Insert
 ******************************************/

func TestCollection_Save_Insert(t *testing.T) {

	collection := getTestCollection(t)
	person := newTestPerson("John Connor", 20)

	require.True(t, person.IsNew()) // unsaved objects are "new"

	err := collection.Save(person, "first save")
	require.NoError(t, err)

	// After the insert, the object is no longer new and has a CreateDate.
	assert.False(t, person.IsNew())
	assert.Greater(t, person.Created(), int64(0))
}

/******************************************
 * Save() - Update
 ******************************************/

func TestCollection_Save_Update(t *testing.T) {

	collection := getTestCollection(t)
	person := newTestPerson("John Connor", 20)
	seedPeople(t, collection, person)

	// Modify the saved object and save it again.
	person.Age = 21
	err := collection.Save(person, "birthday")
	require.NoError(t, err)

	// Reload and confirm the update persisted.
	loaded := testPerson{}
	err = collection.Load(exp.Equal("name", "John Connor"), &loaded)
	require.NoError(t, err)
	assert.Equal(t, 21, loaded.Age)
}

// Saving a non-new object whose ID is not a valid Mongo ObjectID fails on the
// update path.
func TestCollection_Save_InvalidObjectID(t *testing.T) {

	collection := getTestCollection(t)

	object := &badIDObject{}
	object.markOld() // not new => Save takes the UPDATE branch

	err := collection.Save(object, "should fail")
	require.Error(t, err)
}

/******************************************
 * Load()
 ******************************************/

func TestCollection_Load(t *testing.T) {

	collection := getTestCollection(t)
	seedPeople(t, collection, newTestPerson("Sarah Connor", 45))

	loaded := testPerson{}
	err := collection.Load(exp.Equal("name", "Sarah Connor"), &loaded)

	require.NoError(t, err)
	assert.Equal(t, "Sarah Connor", loaded.Name)
	assert.Equal(t, 45, loaded.Age)
}

func TestCollection_Load_NotFound(t *testing.T) {

	collection := getTestCollection(t)

	loaded := testPerson{}
	err := collection.Load(exp.Equal("name", "Nobody"), &loaded)

	require.Error(t, err)
	assert.True(t, derp.IsNotFound(err))         // mapped to a 404
	assert.ErrorIs(t, err, mongo.ErrNoDocuments) // driver error preserved in the chain
}

func TestCollection_Load_WithFieldsOption(t *testing.T) {

	collection := getTestCollection(t)
	seedPeople(t, collection, newTestPerson("Sarah Connor", 45))

	// Project only the "name" field; "age" should come back as its zero value.
	loaded := testPerson{}
	err := collection.Load(exp.Equal("name", "Sarah Connor"), &loaded, option.Fields("name"))

	require.NoError(t, err)
	assert.Equal(t, "Sarah Connor", loaded.Name)
	assert.Equal(t, 0, loaded.Age)
}

/******************************************
 * Count()
 ******************************************/

func TestCollection_Count(t *testing.T) {

	collection := getTestCollection(t)
	seedPeople(t, collection,
		newTestPerson("John Connor", 20),
		newTestPerson("Sarah Connor", 45),
		newTestPerson("Kyle Reese", 30),
	)

	// Count everything.
	count, err := collection.Count(exp.All())
	require.NoError(t, err)
	assert.Equal(t, int64(3), count)

	// Count a filtered subset.
	count, err = collection.Count(exp.Equal("name", "John Connor"))
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// Count something that does not exist.
	count, err = collection.Count(exp.Equal("name", "Nobody"))
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

/******************************************
 * Query()
 ******************************************/

func TestCollection_Query(t *testing.T) {

	collection := getTestCollection(t)
	seedPeople(t, collection,
		newTestPerson("John Connor", 20),
		newTestPerson("Sarah Connor", 45),
	)

	results := make([]testPerson, 0)
	err := collection.Query(&results, exp.All())

	require.NoError(t, err)
	assert.Len(t, results, 2)
}

func TestCollection_Query_Sorted(t *testing.T) {

	collection := getTestCollection(t)
	seedPeople(t, collection,
		newTestPerson("John Connor", 20),
		newTestPerson("Sarah Connor", 45),
		newTestPerson("Kyle Reese", 30),
	)

	results := make([]testPerson, 0)
	err := collection.Query(&results, exp.All(), option.SortAsc("age"))

	require.NoError(t, err)
	require.Len(t, results, 3)
	assert.Equal(t, 20, results[0].Age)
	assert.Equal(t, 30, results[1].Age)
	assert.Equal(t, 45, results[2].Age)
}

func TestCollection_Query_MaxRows(t *testing.T) {

	collection := getTestCollection(t)
	seedPeople(t, collection,
		newTestPerson("John Connor", 20),
		newTestPerson("Sarah Connor", 45),
		newTestPerson("Kyle Reese", 30),
	)

	results := make([]testPerson, 0)
	err := collection.Query(&results, exp.All(), option.MaxRows(2))

	require.NoError(t, err)
	assert.Len(t, results, 2)
}

/******************************************
 * Delete() - Virtual
 ******************************************/

func TestCollection_Delete(t *testing.T) {

	collection := getTestCollection(t)
	person := newTestPerson("John Connor", 20)
	seedPeople(t, collection, person)

	err := collection.Delete(person, "deleting")
	require.NoError(t, err)

	// Virtual delete marks the in-memory object as deleted...
	assert.True(t, person.IsDeleted())

	// ...and the record still exists in the database, but flagged as deleted.
	loaded := testPerson{}
	err = collection.Load(exp.Equal("name", "John Connor"), &loaded)
	require.NoError(t, err)
	assert.True(t, loaded.IsDeleted())
}

// A brand-new object cannot be deleted (there is nothing in the database yet).
func TestCollection_Delete_NewObject(t *testing.T) {

	collection := getTestCollection(t)
	person := newTestPerson("Never Saved", 99)

	err := collection.Delete(person, "deleting")
	require.Error(t, err)
}

// When the underlying Save fails, Delete wraps the error while preserving both
// the 500 status code and the original error in the chain.
func TestCollection_Delete_SaveError(t *testing.T) {

	collection := getTestCollection(t)

	object := &badIDObject{}
	object.markOld() // not new => Delete proceeds to Save, which fails on the bad ID

	err := collection.Delete(object, "deleting")

	require.Error(t, err)
	assert.Equal(t, http.StatusInternalServerError, derp.ErrorCode(err)) // status preserved

	// The original driver error is still reachable through the chain (Delete ->
	// Save -> ObjectIDFromHex).  Before the H2 fix it was flattened to a string.
	assert.ErrorIs(t, err, primitive.ErrInvalidHex)
}

/******************************************
 * HardDelete()
 ******************************************/

func TestCollection_HardDelete(t *testing.T) {

	collection := getTestCollection(t)
	seedPeople(t, collection,
		newTestPerson("John Connor", 20),
		newTestPerson("Sarah Connor", 45),
	)

	// Physically remove a single matching record.
	err := collection.HardDelete(exp.Equal("name", "John Connor"))
	require.NoError(t, err)

	count, err := collection.Count(exp.All())
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// The removed record is truly gone (not a virtual delete).
	err = collection.Load(exp.Equal("name", "John Connor"), &testPerson{})
	require.Error(t, err)
	assert.True(t, derp.IsNotFound(err))
}

func TestCollection_HardDelete_All(t *testing.T) {

	collection := getTestCollection(t)
	seedPeople(t, collection,
		newTestPerson("John Connor", 20),
		newTestPerson("Sarah Connor", 45),
	)

	err := collection.HardDelete(exp.All())
	require.NoError(t, err)

	count, err := collection.Count(exp.All())
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

/******************************************
 * Error Paths
 ******************************************/

// A target that is not a pointer to a slice cannot be unmarshaled, so Query
// returns a wrapped error.
func TestCollection_Query_UnmarshalError(t *testing.T) {

	collection := getTestCollection(t)
	seedPeople(t, collection, newTestPerson("John Connor", 20))

	err := collection.Query(map[string]any{}, exp.All())
	require.Error(t, err)
}

/******************************************
 * Slow-Query Logging
 ******************************************/

// timeoutError reports a slow query. Calling it directly gives deterministic
// coverage of the reporting path without relying on query timing.
func TestCollection_TimeoutError(t *testing.T) {

	collection := getTestCollection(t)

	assert.NotPanics(t, func() {
		collection.timeoutError("test.location", time.Now().UnixMilli()-50, "extra", "context")
	})
}

// With slow-query logging enabled, every operation must still succeed. A tiny
// threshold also exercises the "timeout exceeded" branch in each method.
func TestCollection_SlowQueryLogging(t *testing.T) {

	collection := getTestCollection(t)
	seedPeople(t, collection, newTestPerson("John Connor", 20))

	restoreLogTimeout(t)
	SetLogTimeout(1) // 1ms: a live round-trip is reported as "slow"

	_, err := collection.Count(exp.All())
	require.NoError(t, err)

	results := make([]testPerson, 0)
	require.NoError(t, collection.Query(&results, exp.All()))

	iterator, err := collection.Iterator(exp.All())
	require.NoError(t, err)
	require.NoError(t, iterator.Close())

	loaded := testPerson{}
	require.NoError(t, collection.Load(exp.Equal("name", "John Connor"), &loaded))

	person := newTestPerson("Sarah Connor", 45)
	require.NoError(t, collection.Save(person, "slow save"))
	require.NoError(t, collection.Delete(person, "slow delete"))
	require.NoError(t, collection.HardDelete(exp.Equal("name", "Sarah Connor")))
}
