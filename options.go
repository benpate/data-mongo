package mongodb

import (
	dataOption "github.com/benpate/data/option"
	bson "go.mongodb.org/mongo-driver/bson"
	mongoOptions "go.mongodb.org/mongo-driver/mongo/options"
)

func convertOptions(options ...dataOption.Option) *mongoOptions.FindOptions {

	if len(options) == 0 {
		return nil
	}

	result := mongoOptions.Find()

	for _, option := range options {

		switch opt := option.(type) {

		case dataOption.FirstRowConfig:
			result.SetLimit(1)

		case dataOption.MaxRowsConfig:
			if opt > 0 {
				result.SetLimit(opt.MaxRows())
			}

		case dataOption.FieldsConfig:
			fields := opt.Fields()
			projection := make(bson.D, len(fields))
			for _, field := range fields {
				projection = append(projection, bson.E{Key: field, Value: 1})
			}
			result.SetProjection(projection)

		case dataOption.SortConfig:
			result.SetSort(bson.D{{opt.FieldName, sortDirection(opt.Direction)}})

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
