package mongodb

import (
	"testing"

	"github.com/benpate/exp"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type testPolygon [][]float64

func (polygon testPolygon) GeoJSON() map[string]any {
	return map[string]any{
		"type":        "Polygon",
		"coordinates": [][][]float64{[][]float64(polygon)},
	}
}

func TestPolygon(t *testing.T) {
	polygon := testPolygon([][]float64{{1, 2}, {3, 4}, {5, 6}, {7, 8}})
	actual := operatorBSON(exp.OperatorGeoIntersects, polygon.GeoJSON())

	expected := bson.M{
		"$geoIntersects": primitive.M{
			"$geometry": map[string]any{
				"type": "Polygon",
				"coordinates": [][][]float64{
					{{1, 2}, {3, 4}, {5, 6}, {7, 8}},
				},
			},
		},
	}

	require.Equal(t, expected, actual)
}
