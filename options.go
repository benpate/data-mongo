package mongodb

import (
	dataOption "github.com/benpate/data/option"
	bson "go.mongodb.org/mongo-driver/bson"
	mongoOptions "go.mongodb.org/mongo-driver/mongo/options"
)

func findOptions(options ...dataOption.Option) *mongoOptions.FindOptions {

	if len(options) == 0 {
		return nil
	}

	result := mongoOptions.Find()

	for _, option := range options {

		switch opt := option.(type) {

		case dataOption.FirstRowOption:
			result.SetLimit(1)

		case dataOption.MaxRowsOption:
			if opt > 0 {
				result.SetLimit(opt.MaxRows())
			}

		case dataOption.FieldsOption:
			result.SetProjection(fieldsProjection(opt.Fields()))

		case dataOption.SortOption:
			result.SetSort(bson.D{{Key: opt.FieldName, Value: sortDirection(opt.Direction)}})

		case dataOption.CaseSensitiveOption:
			result.SetCollation(caseCollation(opt.CaseSensitive()))
		}
	}

	return result
}

func findOneOptions(options ...dataOption.Option) *mongoOptions.FindOneOptions {

	if len(options) == 0 {
		return nil
	}

	result := mongoOptions.FindOne()

	for _, option := range options {

		switch opt := option.(type) {

		case dataOption.FieldsOption:
			result.SetProjection(fieldsProjection(opt.Fields()))

		case dataOption.CaseSensitiveOption:
			result.SetCollation(caseCollation(opt.CaseSensitive()))
		}
	}

	return result
}

// countOptions translates the standard data options that are meaningful to a
// count into mongodb CountOptions.  Only MaxRows (Limit) and CaseSensitive
// (Collation) affect a count; Fields and Sort are intentionally ignored.
func countOptions(options ...dataOption.Option) *mongoOptions.CountOptions {

	if len(options) == 0 {
		return nil
	}

	result := mongoOptions.Count()

	for _, option := range options {

		switch opt := option.(type) {

		case dataOption.MaxRowsOption:
			if opt > 0 {
				result.SetLimit(opt.MaxRows())
			}

		case dataOption.CaseSensitiveOption:
			result.SetCollation(caseCollation(opt.CaseSensitive()))
		}
	}

	return result
}

// caseCollation returns the mongodb Collation implementing the given case
// sensitivity: Strength 3 is case-sensitive, Strength 2 is case-insensitive.
func caseCollation(caseSensitive bool) *mongoOptions.Collation {

	strength := 2
	if caseSensitive {
		strength = 3
	}

	return &mongoOptions.Collation{
		Locale:   "en",
		Strength: strength,
	}
}

// fieldsProjection builds a mongodb projection including the named fields,
// skipping any empty field names.
func fieldsProjection(fields []string) bson.D {

	projection := make(bson.D, 0, len(fields))

	for _, field := range fields {
		if field != "" {
			projection = append(projection, bson.E{Key: field, Value: 1})
		}
	}

	return projection
}

func sortDirection(direction string) int {
	if direction == dataOption.SortDirectionDescending {
		return -1
	}

	return 1
}
