package mongodb

import (
	"testing"

	"github.com/benpate/exp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

/******************************************
 * Regression & Behavior Tests
 *
 * Most tests here pin a defect that this package
 * actually shipped. The TestBehavior_* ones pin
 * a property that nothing else enforces, and that
 * a plausible "cleanup" would quietly reverse.
 *
 * They share a file because what they have in
 * common is their history, not their subject.
 ******************************************/

// An empty AND/OR used to translate into a NIL bson.M.  ExpressionToBSON is
// exported, so a caller who added a condition of their own to the result --
// the obvious use for a "convert to pure bson" helper -- panicked with
// "assignment to entry in nil map", but only when the criteria happened to be
// empty.  exp.All() is the common spelling that triggered it.
func TestRegression_EmptyExpressionIsAWritableMap(t *testing.T) {

	for _, criteria := range []exp.Expression{
		exp.All(),
		exp.And(),
		exp.Or(),
		exp.Empty(),
	} {
		filter := ExpressionToBSON(criteria)

		require.NotNil(t, filter, "%T must not translate to a nil map", criteria)

		// The real regression: this line panicked.
		require.NotPanics(t, func() { filter["deleteDate"] = 0 }, "%T", criteria)

		assert.Equal(t, bson.M{"deleteDate": 0}, filter)
	}
}

// An empty expression means "match everything" -- confirmed against a live
// server, so that the nil-to-empty change above cannot have altered meaning.
func TestRegression_EmptyExpressionMatchesEverything(t *testing.T) {

	collection := getTestCollection(t)
	seedPeople(t, collection, newTestPerson("Sarah", 30), newTestPerson("John", 40))

	for _, criteria := range []exp.Expression{exp.All(), exp.And(), exp.Or(), exp.Empty()} {
		count, err := collection.Count(criteria)
		require.NoError(t, err, "%T", criteria)
		assert.Equal(t, int64(2), count, "%T must match every record", criteria)
	}
}

// Iterator.Next used to swallow the error from cursor.Decode and simply return
// FALSE, which is indistinguishable from a clean end of iteration.  A record
// that would not decode silently truncated the result set, and Error() reported
// nothing at all.
func TestRegression_IteratorReportsDecodeErrors(t *testing.T) {

	collection := getTestCollection(t)

	seedPeople(t, collection,
		newTestPerson("Alice", 1),
		newTestPerson("Bob", 2),
		newTestPerson("Carol", 3),
	)

	// A fourth record whose "age" is a string will not decode into testPerson.Age.
	_, err := collection.Mongo().InsertOne(collection.Context(), bson.M{
		"name": "Mallory",
		"age":  "not-a-number",
	})
	require.NoError(t, err)

	iterator, err := collection.Iterator(exp.All())
	require.NoError(t, err)
	t.Cleanup(func() { _ = iterator.Close() })

	walked := 0
	for person := (testPerson{}); iterator.Next(&person); {
		walked++
	}

	// The short read itself is expected -- iteration stops at the bad record.
	assert.Less(t, walked, 4, "the undecodable record should stop iteration")

	// This is the regression: the caller must be TOLD.
	require.Error(t, iterator.Error(), "a decode failure must be reported by Error()")
}

// A clean iterator reports no error, so the check above cannot pass by always
// returning one.
func TestRegression_IteratorReportsNoErrorWhenClean(t *testing.T) {

	collection := getTestCollection(t)
	seedPeople(t, collection, newTestPerson("Alice", 1), newTestPerson("Bob", 2))

	iterator, err := collection.Iterator(exp.All())
	require.NoError(t, err)
	t.Cleanup(func() { _ = iterator.Close() })

	walked := 0
	for person := (testPerson{}); iterator.Next(&person); {
		walked++
	}

	require.Equal(t, 2, walked)
	require.NoError(t, iterator.Error())
}

// Iterator.Count reports the CURRENT BATCH, not the total.  Its doc comment
// used to claim the total, which is only true while the result set fits in one
// batch -- so every small test agreed with the wrong description.  This pins
// the real behavior with a result set that does not fit.
//
// NOTE: this is a documented deviation from the data.Iterator interface, which
// specifies "the number of records contained by this iterator" (data-mock
// returns the true total).  Reporting the total here would cost an extra round
// trip to the server on every call.
func TestRegression_IteratorCountIsBatchNotTotal(t *testing.T) {

	collection := getTestCollection(t)

	// The driver's default first batch is 101 documents.
	const recordCount = 250

	for i := range recordCount {
		require.NoError(t, collection.Save(newTestPerson("Person", i), "seed"))
	}

	total, err := collection.Count(exp.All())
	require.NoError(t, err)
	require.Equal(t, int64(recordCount), total)

	iterator, err := collection.Iterator(exp.All())
	require.NoError(t, err)
	t.Cleanup(func() { _ = iterator.Close() })

	assert.Less(t, iterator.Count(), recordCount,
		"Count reports one batch, so it must be less than the %d records available", recordCount)

	// ...and the iterator still yields every record when walked.
	walked := 0
	for person := (testPerson{}); iterator.Next(&person); {
		walked++
	}

	assert.Equal(t, recordCount, walked)
	require.NoError(t, iterator.Error())
}

// A value of the wrong Go type for a string-matching or EXISTS operator falls
// through to an empty comparison, which MongoDB matches against NOTHING.  That
// is the safe direction, and it is what the sibling exp-builder package relies
// on: a malformed value must narrow a query, never widen it.  Rewriting the
// fall-through to mirror the default "$eq" branch would silently reverse this.
func TestBehavior_TypeMismatchFailsClosed(t *testing.T) {

	collection := getTestCollection(t)
	seedPeople(t, collection,
		newTestPerson("Sarah", 30),
		newTestPerson("John", 40),
		newTestPerson("Mary", 50),
	)

	mismatched := map[string]exp.Expression{
		"CONTAINS with an int":   exp.New("name", exp.OperatorContains, 42),
		"BEGINS with an int":     exp.New("name", exp.OperatorBeginsWith, 42),
		"ENDS with a nil":        exp.New("name", exp.OperatorEndsWith, nil),
		"EXISTS with a string":   exp.New("name", exp.OperatorExists, "true"),
		"CONTAINS with a []byte": exp.New("name", exp.OperatorContains, []byte("Sarah")),
	}

	for label, criteria := range mismatched {
		count, err := collection.Count(criteria)
		require.NoError(t, err, label)
		assert.Zero(t, count, "%s must match NO records, not every record", label)
	}

	// The control: a correctly-typed value still matches.
	count, err := collection.Count(exp.New("name", exp.OperatorContains, "Sarah"))
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

// Nobody here but us chickens
