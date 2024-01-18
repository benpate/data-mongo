package mongodb

import (
	"github.com/benpate/exp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// ExpressionToBSON converts a data.Expression value into pure bson.
func ExpressionToBSON(criteria exp.Expression) bson.M {

	switch c := criteria.(type) {

	case exp.Predicate:

		result := bson.M{}
		result[c.Field] = operatorBSON(c.Operator, c.Value)
		return result

	case exp.AndExpression:

		if len(c) == 0 {
			return nil
		}

		array := bson.A{}

		for _, exp := range c {
			array = append(array, ExpressionToBSON(exp))
		}

		return bson.M{"$and": array}

	case exp.OrExpression:

		if len(c) == 0 {
			return nil
		}

		array := bson.A{}

		for _, exp := range c {
			array = append(array, ExpressionToBSON(exp))
		}

		return bson.M{"$or": array}
	}

	return bson.M{}
}

// operatorBSON converts a standard data.Operator into the operators used by mongodb
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

	case exp.OperatorBeginsWith:
		if valueString, ok := value.(string); ok {
			return bson.M{"$regex": primitive.Regex{Pattern: "^" + valueString, Options: "i"}}
		}

	case exp.OperatorContains:
		if valueString, ok := value.(string); ok {
			return bson.M{"$regex": primitive.Regex{Pattern: valueString, Options: "i"}}
		}

	case exp.OperatorEndsWith:
		if valueString, ok := value.(string); ok {
			return bson.M{"$regex": primitive.Regex{Pattern: valueString + "$", Options: "i"}}
		}

	default:
		return bson.M{"$eq": value}
	}

	return bson.M{}
}
