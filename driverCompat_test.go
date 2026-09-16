package mongodb

import (
	"bytes"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bsonV1 "go.mongodb.org/mongo-driver/bson"
	bsontypeV1 "go.mongodb.org/mongo-driver/bson/bsontype"
	primitiveV1 "go.mongodb.org/mongo-driver/bson/primitive"
	bsonV2 "go.mongodb.org/mongo-driver/v2/bson"
)

/******************************************
 * Driver compatibility harness (BUG-122)
 *
 * The two driver majors are separate module paths, so one
 * package can import both and diff them directly. This pins
 * what the v1 -> v2 upgrade does and does not change.
 ******************************************/

// encodeV1 marshals a value with the v1 driver and returns it as hex.
func encodeV1(t *testing.T, value any) string {

	t.Helper()

	data, err := bsonV1.Marshal(value)
	require.NoError(t, err)

	return hex.EncodeToString(data)
}

// encodeV2 marshals a value with the v2 driver and returns it as hex.
func encodeV2(t *testing.T, value any) string {

	t.Helper()

	data, err := bsonV2.Marshal(value)
	require.NoError(t, err)

	return hex.EncodeToString(data)
}

// TestDriverCompat_Encoding asserts that every shape this package writes encodes
// to identical bytes under both driver majors.
func TestDriverCompat_Encoding(t *testing.T) {

	// same encodes one value that is written the same way by both drivers
	same := func(name string, value any) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			require.Equal(t, encodeV1(t, value), encodeV2(t, value),
				"v1 and v2 must write identical bytes for this shape")
		})
	}

	// sameTwins encodes a pair of values that differ ONLY in the driver type they
	// carry, because an ObjectID cannot be spelled the same way in both majors.
	sameTwins := func(name string, valueV1 any, valueV2 any) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			require.Equal(t, encodeV1(t, valueV1), encodeV2(t, valueV2),
				"v1 and v2 must write identical bytes for this shape")
		})
	}

	type containers struct {
		NilSlice   []string       `bson:"nilSlice"`
		EmptySlice []string       `bson:"emptySlice"`
		NilMap     map[string]any `bson:"nilMap"`
		EmptyMap   map[string]any `bson:"emptyMap"`
		NilPointer *string        `bson:"nilPointer"`
	}

	type omitted struct {
		Slice   []string        `bson:"slice,omitempty"`
		Map     map[string]any  `bson:"map,omitempty"`
		Pointer *int            `bson:"pointer,omitempty"`
		Text    string          `bson:"text,omitempty"`
		Number  int64           `bson:"number,omitempty"`
		Struct  struct{ X int } `bson:"struct,omitempty"`
	}

	type numbers struct {
		Int     int     `bson:"int"`
		Int8    int8    `bson:"int8"`
		Int32   int32   `bson:"int32"`
		Int64   int64   `bson:"int64"`
		Uint    uint    `bson:"uint"`
		Float64 float64 `bson:"float64"`
	}

	type timestamps struct {
		Moment time.Time `bson:"moment"`
	}

	same("empty struct", struct{}{})
	same("nil and empty containers", containers{EmptySlice: []string{}, EmptyMap: map[string]any{}})
	same("omitempty variants", omitted{})
	same("numeric widths", numbers{Int: 1, Int8: 2, Int32: 3, Int64: 4, Uint: 5, Float64: 6.5})
	same("negative and boundary numbers", numbers{Int: -1, Int8: -128, Int32: -2147483648, Int64: -9223372036854775808})
	same("time.Time", timestamps{Moment: time.Unix(1700000000, 123456789).UTC()})
	same("zero time.Time", timestamps{})

	// The journal shape every data.Object embeds
	same("journal fields", struct {
		CreateDate int64  `bson:"createDate"`
		UpdateDate int64  `bson:"updateDate"`
		DeleteDate int64  `bson:"deleteDate"`
		Note       string `bson:"note"`
		Revision   int64  `bson:"signature"`
	}{CreateDate: 1, UpdateDate: 2, DeleteDate: 3, Note: "created", Revision: 4})

	// The query shapes ExpressionToBSON produces
	sameTwins("bson.M filter", bsonV1.M{"b": 1, "a": 2}, bsonV2.M{"b": 1, "a": 2})
	sameTwins("bson.D sort", bsonV1.D{{Key: "createDate", Value: 1}}, bsonV2.D{{Key: "createDate", Value: 1}})
	sameTwins("empty bson.M", bsonV1.M{}, bsonV2.M{})
	sameTwins("nested operator", bsonV1.M{"parentId": bsonV1.M{"$in": bsonV1.A{1, 2}}}, bsonV2.M{"parentId": bsonV2.M{"$in": bsonV2.A{1, 2}}})

	objectIDV1, err := primitiveV1.ObjectIDFromHex("507f1f77bcf86cd799439011")
	require.NoError(t, err)
	objectIDV2, err := bsonV2.ObjectIDFromHex("507f1f77bcf86cd799439011")
	require.NoError(t, err)

	sameTwins("ObjectID", bsonV1.M{"_id": objectIDV1}, bsonV2.M{"_id": objectIDV2})
	sameTwins("zero ObjectID", bsonV1.M{"_id": primitiveV1.NilObjectID}, bsonV2.M{"_id": bsonV2.NilObjectID})
}

// TestDriverCompat_Decoding asserts that values decoded from identical bytes are
// equal under both driver majors.
func TestDriverCompat_Decoding(t *testing.T) {

	// same decodes one document into a fresh copy of the same target type under
	// each driver, and requires the two results to agree.
	same := func(name string, source []byte, newTarget func() any) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			first, second := newTarget(), newTarget()
			require.NoError(t, bsonV1.Unmarshal(source, first))
			require.NoError(t, bsonV2.Unmarshal(source, second))
			require.Equal(t, first, second)
		})
	}

	// sameError requires both drivers to reject a document with the same message,
	// because Emissary reports decode failures rather than tolerating them.
	sameError := func(name string, source []byte, newTarget func() any) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			errV1 := bsonV1.Unmarshal(source, newTarget())
			errV2 := bsonV2.Unmarshal(source, newTarget())
			require.Error(t, errV1)
			require.Error(t, errV2)
			require.Equal(t, errV1.Error(), errV2.Error())
		})
	}

	type record struct {
		Text   string  `bson:"text"`
		Number int     `bson:"number"`
		Amount float64 `bson:"amount"`
	}

	newRecord := func() any { return &record{} }

	populated, err := bsonV1.Marshal(bsonV1.M{"text": "hello", "number": int64(42), "amount": 3.5})
	require.NoError(t, err)

	missing, err := bsonV1.Marshal(bsonV1.M{"other": 1})
	require.NoError(t, err)

	nulls, err := bsonV1.Marshal(bsonV1.M{"text": nil, "number": nil})
	require.NoError(t, err)

	mistyped, err := bsonV1.Marshal(bsonV1.M{"number": "not a number"})
	require.NoError(t, err)

	truncating, err := bsonV1.Marshal(bsonV1.M{"number": 3.9})
	require.NoError(t, err)

	same("populated record", populated, newRecord)
	same("missing fields stay zero", missing, newRecord)
	same("null decodes as zero", nulls, newRecord)

	sameError("type mismatch", mistyped, newRecord)
	sameError("float truncation refused", truncating, newRecord)
}

// TestDriverCompat_NestedDocumentTypeChanged pins the ONE decode difference found
// between the majors: the Go type a nested document lands in.
func TestDriverCompat_NestedDocumentTypeChanged(t *testing.T) {

	type holder struct {
		Data map[string]any `bson:"data"`
	}

	source, err := bsonV1.Marshal(bsonV1.M{"data": bsonV1.M{"nested": bsonV1.M{"key": "value"}}})
	require.NoError(t, err)

	// v1 hands back a plain map, which is what the rosetta accessors match on
	first := holder{}
	require.NoError(t, bsonV1.Unmarshal(source, &first))
	_, isMap := first.Data["nested"].(map[string]any)
	require.True(t, isMap, "v1 decodes a nested document as map[string]any")

	// RULE: v2's DEFAULT hands back an ordered bson.D instead, and a type
	// assertion to map[string]any simply reports false -- no error is raised
	// anywhere, so the value silently reads as absent.
	second := holder{}
	require.NoError(t, bsonV2.Unmarshal(source, &second))
	_, isStillMap := second.Data["nested"].(map[string]any)
	require.False(t, isStillMap, "v2 default decodes a nested document as bson.D")

	// DefaultDocumentMap restores the v1 shape, and is what the upgrade must set
	// on the client. DefaultDocumentM is NOT equivalent: bson.M is a named type,
	// so it fails the same assertion.
	third := holder{}
	decoder := bsonV2.NewDecoder(bsonV2.NewDocumentReader(bytes.NewReader(source)))
	decoder.DefaultDocumentMap()
	require.NoError(t, decoder.Decode(&third))
	_, isMapAgain := third.Data["nested"].(map[string]any)
	require.True(t, isMapAgain, "DefaultDocumentMap restores the v1 shape")

	// and a document decoded that way re-encodes to the bytes it came from
	reEncoded, err := bsonV2.Marshal(third)
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(source), hex.EncodeToString(reEncoded))
}

// legacyValueMarshaler is written against the V1 ValueMarshaler signature, which
// returns a bsontype.Type rather than the plain byte v2 expects.
type legacyValueMarshaler struct {
	Longitude float64
	Latitude  float64
}

// MarshalBSONValue serializes this value as a coordinate array.
func (value legacyValueMarshaler) MarshalBSONValue() (bsontypeV1.Type, []byte, error) {
	return bsonV1.MarshalValue(bsonV1.A{value.Longitude, value.Latitude})
}

// TestDriverCompat_ValueMarshalerSignatureChanged pins the migration's most
// dangerous difference: a custom marshaller written for v1 stops being
// recognized by v2, and nothing reports it.
func TestDriverCompat_ValueMarshalerSignatureChanged(t *testing.T) {

	// RULE: the interfaces are satisfied structurally, so the v1 spelling simply
	// stops matching. This is not a compile error at the type's own definition.
	_, satisfiesV2 := any(legacyValueMarshaler{}).(bsonV2.ValueMarshaler)
	require.False(t, satisfiesV2,
		"a v1-signature MarshalBSONValue does not satisfy v2.ValueMarshaler")

	wrapper := struct {
		Position legacyValueMarshaler `bson:"position"`
	}{Position: legacyValueMarshaler{Longitude: -104.9, Latitude: 39.7}}

	dataV1, err := bsonV1.Marshal(wrapper)
	require.NoError(t, err)

	dataV2, err := bsonV2.Marshal(wrapper)
	require.NoError(t, err)

	// v1 uses the custom marshaller and writes an ARRAY
	require.Equal(t,
		`{"position": [{"$numberDouble":"-104.9"},{"$numberDouble":"39.7"}]}`,
		bsonV1.Raw(dataV1).String())

	// v2 falls through to the default struct codec and writes a DOCUMENT, with
	// no error on either side. Every type in this position needs a compile-time
	// assertion; see BUG-122 and geo/AGENTS.md.
	require.Equal(t,
		`{"position": {"longitude": {"$numberDouble":"-104.9"},"latitude": {"$numberDouble":"39.7"}}}`,
		bsonV1.Raw(dataV2).String())
}
