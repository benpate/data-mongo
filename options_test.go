package mongodb

import (
	"testing"

	"github.com/benpate/data/option"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

/******************************************
 * findOptions()
 ******************************************/

// With no options, findOptions returns nil so the driver uses its defaults.
func TestFindOptions_Empty(t *testing.T) {
	assert.Nil(t, findOptions())
}

func TestFindOptions_FirstRow(t *testing.T) {
	result := findOptions(option.FirstRow())

	require.NotNil(t, result)
	require.NotNil(t, result.Limit)
	assert.Equal(t, int64(1), *result.Limit)
}

func TestFindOptions_MaxRows(t *testing.T) {
	result := findOptions(option.MaxRows(25))

	require.NotNil(t, result)
	require.NotNil(t, result.Limit)
	assert.Equal(t, int64(25), *result.Limit)
}

// A MaxRows value of zero (or less) means "no limit", so Limit stays unset.
func TestFindOptions_MaxRowsZero(t *testing.T) {
	result := findOptions(option.MaxRows(0))

	require.NotNil(t, result)
	assert.Nil(t, result.Limit)
}

func TestFindOptions_Fields(t *testing.T) {
	result := findOptions(option.Fields("name", "age"))

	require.NotNil(t, result)
	assert.Equal(t, bson.D{{Key: "name", Value: 1}, {Key: "age", Value: 1}}, result.Projection)
}

// Empty field names are skipped when building the projection.
func TestFindOptions_FieldsSkipsEmpty(t *testing.T) {
	result := findOptions(option.Fields("name", "", "age"))

	require.NotNil(t, result)
	assert.Equal(t, bson.D{{Key: "name", Value: 1}, {Key: "age", Value: 1}}, result.Projection)
}

func TestFindOptions_SortAscending(t *testing.T) {
	result := findOptions(option.SortAsc("name"))

	require.NotNil(t, result)
	assert.Equal(t, bson.D{{Key: "name", Value: 1}}, result.Sort)
}

func TestFindOptions_SortDescending(t *testing.T) {
	result := findOptions(option.SortDesc("name"))

	require.NotNil(t, result)
	assert.Equal(t, bson.D{{Key: "name", Value: -1}}, result.Sort)
}

func TestFindOptions_CaseSensitive(t *testing.T) {
	result := findOptions(option.CaseSensitive(true))

	require.NotNil(t, result)
	require.NotNil(t, result.Collation)
	assert.Equal(t, "en", result.Collation.Locale)
	assert.Equal(t, 3, result.Collation.Strength)
}

func TestFindOptions_CaseInsensitive(t *testing.T) {
	result := findOptions(option.CaseSensitive(false))

	require.NotNil(t, result)
	require.NotNil(t, result.Collation)
	assert.Equal(t, "en", result.Collation.Locale)
	assert.Equal(t, 2, result.Collation.Strength)
}

// Multiple options should all be applied to the same result.
func TestFindOptions_Combined(t *testing.T) {
	result := findOptions(option.MaxRows(10), option.SortDesc("age"), option.Fields("name"))

	require.NotNil(t, result)
	require.NotNil(t, result.Limit)
	assert.Equal(t, int64(10), *result.Limit)
	assert.Equal(t, bson.D{{Key: "age", Value: -1}}, result.Sort)
	assert.Equal(t, bson.D{{Key: "name", Value: 1}}, result.Projection)
}

/******************************************
 * findOneOptions()
 ******************************************/

func TestFindOneOptions_Empty(t *testing.T) {
	assert.Nil(t, findOneOptions())
}

func TestFindOneOptions_Fields(t *testing.T) {
	result := findOneOptions(option.Fields("name", "age"))

	require.NotNil(t, result)
	assert.Equal(t, bson.D{{Key: "name", Value: 1}, {Key: "age", Value: 1}}, result.Projection)
}

func TestFindOneOptions_FieldsSkipsEmpty(t *testing.T) {
	result := findOneOptions(option.Fields("", "age"))

	require.NotNil(t, result)
	assert.Equal(t, bson.D{{Key: "age", Value: 1}}, result.Projection)
}

func TestFindOneOptions_CaseSensitive(t *testing.T) {
	result := findOneOptions(option.CaseSensitive(true))

	require.NotNil(t, result)
	require.NotNil(t, result.Collation)
	assert.Equal(t, 3, result.Collation.Strength)
}

func TestFindOneOptions_CaseInsensitive(t *testing.T) {
	result := findOneOptions(option.CaseSensitive(false))

	require.NotNil(t, result)
	require.NotNil(t, result.Collation)
	assert.Equal(t, 2, result.Collation.Strength)
}

// Options that only apply to multi-row queries (like Sort) are ignored here,
// but must not prevent a non-nil result from being returned.
func TestFindOneOptions_IgnoresUnsupported(t *testing.T) {
	result := findOneOptions(option.SortAsc("name"))

	require.NotNil(t, result)
	assert.Nil(t, result.Sort)
}

/******************************************
 * sortDirection()
 ******************************************/

func TestSortDirection(t *testing.T) {
	assert.Equal(t, 1, sortDirection(option.SortDirectionAscending))
	assert.Equal(t, -1, sortDirection(option.SortDirectionDescending))
	assert.Equal(t, 1, sortDirection("anything else defaults to ascending"))
}
