package mongodb

import (
	dataOption "github.com/benpate/data/option"
	"github.com/davecgh/go-spew/spew"
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
			fields := opt.Fields()
			projection := make(bson.D, 0, len(fields))
			for _, field := range fields {
				if field != "" {
					projection = append(projection, bson.E{Key: field, Value: 1})
				}
			}
			result.SetProjection(projection)

		case dataOption.SortOption:
			result.SetSort(bson.D{{Key: opt.FieldName, Value: sortDirection(opt.Direction)}})

		case dataOption.CaseSensitiveOption:

			if opt.CaseSensitive() {
				result.SetCollation(&mongoOptions.Collation{
					Locale:   "en",
					Strength: 3,
				})
			} else {
				result.SetCollation(&mongoOptions.Collation{
					Locale:   "en",
					Strength: 2,
				})
			}
		}
	}

	return result
}

func findOneOptions(options ...dataOption.Option) *mongoOptions.FindOneOptions {

	spew.Dump(options)

	if len(options) == 0 {
		return nil
	}

	result := mongoOptions.FindOne()

	for _, option := range options {

		switch opt := option.(type) {

		case dataOption.FieldsOption:
			fields := opt.Fields()
			projection := make(bson.D, 0, len(fields))
			for _, field := range fields {
				if field != "" {
					projection = append(projection, bson.E{Key: field, Value: 1})
				}
			}
			result.SetProjection(projection)

		case dataOption.CaseSensitiveOption:

			if opt.CaseSensitive() {
				result.SetCollation(&mongoOptions.Collation{
					Locale:   "en",
					Strength: 3,
				})
				continue
			}

			result.SetCollation(&mongoOptions.Collation{
				Locale:   "en",
				Strength: 2,
			})
		}
	}

	return result
}

func sortDirection(direction string) int {
	if direction == dataOption.SortDirectionDescending {
		return -1
	}

	return 1
}
