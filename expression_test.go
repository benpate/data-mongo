package mongodb

import (
	"encoding/json"
	"regexp"
	"testing"

	"github.com/benpate/exp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestExpression(t *testing.T) {

	// toJSON converts values into an easy-to-test JSON string
	toJSON := func(value any) string {

		result, err := json.Marshal(value)

		if err != nil {
			return err.Error()
		}

		return string(result)
	}

	{
		// Test combining operators into a single bson.M
		pred := exp.GreaterThan("age", 42)
		assert.Equal(t, toJSON(ExpressionToBSON(pred)), `{"age":{"$gt":42}}`)

		pred2 := pred.AndEqual("createDate", 10)
		assert.Equal(t, toJSON(ExpressionToBSON(pred2)), `{"$and":[{"age":{"$gt":42}},{"createDate":{"$eq":10}}]}`)

		pred3 := pred2.And(exp.LessThan("createDate", 20))
		assert.Equal(t, toJSON(ExpressionToBSON(pred3)), `{"$and":[{"age":{"$gt":42}},{"createDate":{"$eq":10}},{"createDate":{"$lt":20}}]}`)
	}

	{
		pred4 := exp.Or(
			exp.New("name", "=", "John Connor").AndEqual("favorite_color", "blue"),
			exp.New("name", "=", "Sara Connor").AndEqual("favorite_color", "green"),
		)

		assert.Equal(t, toJSON(ExpressionToBSON(pred4)), `{"$or":[{"$and":[{"name":{"$eq":"John Connor"}},{"favorite_color":{"$eq":"blue"}}]},{"$and":[{"name":{"$eq":"Sara Connor"}},{"favorite_color":{"$eq":"green"}}]}]}`)
	}

	{
		pred5 := exp.New("name", "=", "John Connor").Or(exp.New("favorite_color", "=", "blue"))
		assert.Equal(t, toJSON(ExpressionToBSON(pred5)), `{"$or":[{"name":{"$eq":"John Connor"}},{"favorite_color":{"$eq":"blue"}}]}`)
	}

	{
		pred6 := exp.And(
			exp.New("name", "=", "John Connor").Or(exp.New("favorite_color", "=", "blue")),
			exp.New("name", "=", "Sara Connor").Or(exp.New("favorite_color", "=", "green")),
		)

		assert.Equal(t, toJSON(ExpressionToBSON(pred6)), `{"$and":[{"$or":[{"name":{"$eq":"John Connor"}},{"favorite_color":{"$eq":"blue"}}]},{"$or":[{"name":{"$eq":"Sara Connor"}},{"favorite_color":{"$eq":"green"}}]}]}`)

	}
}

/******************************************
 * ExpressionToBSON() - Special Cases
 ******************************************/

// A full-text predicate is rewritten into MongoDB's $text/$search form.
func TestExpressionToBSON_FullText(t *testing.T) {
	result := ExpressionToBSON(exp.Equal("$fullText", "hello world"))

	assert.Equal(t, bson.M{"$text": bson.M{"$search": "hello world"}}, result)
}

// An empty AND expression (exp.All) produces a nil filter, which the driver
// treats as "match everything".
func TestExpressionToBSON_EmptyAnd(t *testing.T) {
	assert.Nil(t, ExpressionToBSON(exp.And()))
	assert.Nil(t, ExpressionToBSON(exp.All()))
}

// An empty OR expression also produces a nil filter.
func TestExpressionToBSON_EmptyOr(t *testing.T) {
	assert.Nil(t, ExpressionToBSON(exp.Or()))
}

// An unrecognized expression type falls through to an empty filter.
func TestExpressionToBSON_UnknownType(t *testing.T) {
	assert.Equal(t, bson.M{}, ExpressionToBSON(nil))
}

/******************************************
 * operatorBSON()
 ******************************************/

func TestOperatorBSON_Comparisons(t *testing.T) {

	// check confirms that a single operator maps to the expected BSON.
	check := func(operator string, expected bson.M) {
		assert.Equal(t, expected, operatorBSON(operator, 42), "operator=%s", operator)
	}

	check(exp.OperatorEqual, bson.M{"$eq": 42})
	check(exp.OperatorNotEqual, bson.M{"$ne": 42})
	check(exp.OperatorLessThan, bson.M{"$lt": 42})
	check(exp.OperatorLessOrEqual, bson.M{"$lte": 42})
	check(exp.OperatorGreaterOrEqual, bson.M{"$gte": 42})
	check(exp.OperatorGreaterThan, bson.M{"$gt": 42})

	// An unrecognized operator defaults to equality.
	check("unknown-operator", bson.M{"$eq": 42})
}

func TestOperatorBSON_Sets(t *testing.T) {
	values := []any{1, 2, 3}

	assert.Equal(t, bson.M{"$in": values}, operatorBSON(exp.OperatorIn, values))
	assert.Equal(t, bson.M{"$nin": values}, operatorBSON(exp.OperatorNotIn, values))
	assert.Equal(t, bson.M{"$all": values}, operatorBSON(exp.OperatorInAll, values))
}

func TestOperatorBSON_StringMatching(t *testing.T) {

	assert.Equal(t,
		bson.M{"$regex": primitive.Regex{Pattern: "^John", Options: "i"}},
		operatorBSON(exp.OperatorBeginsWith, "John"))

	assert.Equal(t,
		bson.M{"$regex": primitive.Regex{Pattern: "Connor", Options: "i"}},
		operatorBSON(exp.OperatorContains, "Connor"))

	assert.Equal(t,
		bson.M{"$regex": primitive.Regex{Pattern: "Connor$", Options: "i"}},
		operatorBSON(exp.OperatorEndsWith, "Connor"))
}

// Regex metacharacters in the value are escaped so the operators match a literal
// substring and untrusted input cannot inject a pathological pattern (ReDoS).
func TestOperatorBSON_StringMatchingEscapesMetacharacters(t *testing.T) {

	// check confirms the value is embedded as its QuoteMeta-escaped form, with
	// the operator's anchor applied OUTSIDE the escaped value.
	check := func(operator string, value string, expectedPattern string) {
		expected := bson.M{"$regex": primitive.Regex{Pattern: expectedPattern, Options: "i"}}
		assert.Equal(t, expected, operatorBSON(operator, value), "operator=%s value=%q", operator, value)
	}

	// A representative spread of regex metacharacters, plus injection payloads.
	values := []string{
		"a.b", "a*b", "a+b", "(a)", "[a-z]", "{1,9}", "a|b", "^a", "a$",
		"a?b", `a\b`, ".*", "(a+)+$", "(.*)+", `\d{10}`, "100% off",
	}

	for _, value := range values {
		check(exp.OperatorContains, value, regexp.QuoteMeta(value))
		check(exp.OperatorBeginsWith, value, "^"+regexp.QuoteMeta(value))
		check(exp.OperatorEndsWith, value, regexp.QuoteMeta(value)+"$")
	}

	// Spot-check one result spelled out, so the test documents the actual effect.
	check(exp.OperatorContains, "a.b", `a\.b`)
	check(exp.OperatorBeginsWith, "a.b", `^a\.b`)
	check(exp.OperatorEndsWith, "a.b", `a\.b$`)

	// A value with no metacharacters is unchanged (regression guard).
	check(exp.OperatorContains, "Connor", "Connor")
}

// The string-matching operators only apply to string values; anything else
// falls through to an empty filter.
func TestOperatorBSON_StringMatchingNonString(t *testing.T) {
	assert.Equal(t, bson.M{}, operatorBSON(exp.OperatorBeginsWith, 42))
	assert.Equal(t, bson.M{}, operatorBSON(exp.OperatorContains, 42))
	assert.Equal(t, bson.M{}, operatorBSON(exp.OperatorEndsWith, 42))
}

func TestOperatorBSON_Exists(t *testing.T) {
	assert.Equal(t, bson.M{"$exists": true}, operatorBSON(exp.OperatorExists, true))
	assert.Equal(t, bson.M{"$exists": false}, operatorBSON(exp.OperatorExists, false))

	// A non-boolean value falls through to an empty filter.
	assert.Equal(t, bson.M{}, operatorBSON(exp.OperatorExists, "yes"))
}

func TestOperatorBSON_Geo(t *testing.T) {
	shape := map[string]any{"type": "Point", "coordinates": []float64{1, 2}}

	assert.Equal(t,
		bson.M{"$geoWithin": bson.M{"$geometry": shape}},
		operatorBSON(exp.OperatorGeoWithin, shape))

	assert.Equal(t,
		bson.M{"$geoIntersects": bson.M{"$geometry": shape}},
		operatorBSON(exp.OperatorGeoIntersects, shape))
}

// A predicate routed through ExpressionToBSON nests the operator under the field.
func TestExpressionToBSON_Predicate(t *testing.T) {
	result := ExpressionToBSON(exp.GreaterThan("age", 21))

	require.Equal(t, bson.M{"age": bson.M{"$gt": 21}}, result)
}
