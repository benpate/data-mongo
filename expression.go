package mongodb

import (
	"regexp"

	"github.com/benpate/exp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// ExpressionToBSON converts an exp.Expression into a pure bson query filter.
func ExpressionToBSON(criteria exp.Expression) bson.M {

	switch c := criteria.(type) {

	case exp.Predicate:

		switch c.Field {

		// Special case for full-text search (ick)
		case "$fullText":
			return bson.M{
				"$text": bson.M{
					"$search": c.Value,
				},
			}

		default:
			result := bson.M{}
			result[c.Field] = operatorBSON(c.Operator, c.Value)
			return result
		}

	case exp.AndExpression:

		// An empty AND places no constraint, which is the same "match everything"
		// filter that exp.Empty() produces.  It must be an EMPTY map and never a
		// nil one: this function is exported, and a caller who adds a condition of
		// their own to the result would panic on a nil map.
		if len(c) == 0 {
			return bson.M{}
		}

		array := bson.A{}

		for _, exp := range c {
			array = append(array, ExpressionToBSON(exp))
		}

		return bson.M{"$and": array}

	case exp.OrExpression:

		// Empty for the same reason as an empty AND, above.
		if len(c) == 0 {
			return bson.M{}
		}

		array := bson.A{}

		for _, exp := range c {
			array = append(array, ExpressionToBSON(exp))
		}

		return bson.M{"$or": array}
	}

	return bson.M{}
}

// operatorBSON converts one of the exp operator constants into the comparison
// document that mongodb expects.  An unrecognized operator compares for equality.
func operatorBSON(operator string, value any) bson.M {

	switch operator {

	case exp.OperatorEqual:
		return bson.M{"$eq": value}

	case exp.OperatorNotEqual:
		return bson.M{"$ne": value}

	case exp.OperatorLessThan:
		return bson.M{"$lt": value}

	case exp.OperatorLessOrEqual:
		return bson.M{"$lte": value}

	case exp.OperatorGreaterOrEqual:
		return bson.M{"$gte": value}

	case exp.OperatorGreaterThan:
		return bson.M{"$gt": value}

	case exp.OperatorIn:
		return bson.M{"$in": value}

	case exp.OperatorNotIn:
		return bson.M{"$nin": value}

	case exp.OperatorInAll:
		return bson.M{"$all": value}

	// The string-matching operators below match a literal substring, so the
	// value is escaped with regexp.QuoteMeta.  Embedding it raw would let
	// metacharacters change the match (a "." matching any character) and let
	// untrusted input inject a pathological regex (ReDoS).

	case exp.OperatorBeginsWith:
		if valueString, isString := value.(string); isString {
			return bson.M{"$regex": primitive.Regex{Pattern: "^" + regexp.QuoteMeta(valueString), Options: "i"}}
		}

	case exp.OperatorContains:
		if valueString, isString := value.(string); isString {
			return bson.M{"$regex": primitive.Regex{Pattern: regexp.QuoteMeta(valueString), Options: "i"}}
		}

	case exp.OperatorEndsWith:
		if valueString, isString := value.(string); isString {
			return bson.M{"$regex": primitive.Regex{Pattern: regexp.QuoteMeta(valueString) + "$", Options: "i"}}
		}

	case exp.OperatorExists:
		if valueBool, isBool := value.(bool); isBool {
			return bson.M{"$exists": valueBool}
		}

	case exp.OperatorGeoWithin:
		return bson.M{"$geoWithin": bson.M{"$geometry": value}}

	case exp.OperatorGeoIntersects:
		return bson.M{"$geoIntersects": bson.M{"$geometry": value}}

	default:
		return bson.M{"$eq": value}
	}

	return bson.M{}
}
