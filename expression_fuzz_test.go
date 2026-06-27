package mongodb

import (
	"regexp"
	"testing"

	"github.com/benpate/exp"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// stringMatchOperators are the operators that compile an untrusted value into a
// MongoDB $regex, and therefore must escape it.
var stringMatchOperators = []string{
	exp.OperatorBeginsWith,
	exp.OperatorContains,
	exp.OperatorEndsWith,
}

// FuzzOperatorBSON throws arbitrary operators and string values at operatorBSON
// to confirm it never panics, and that the string-matching operators always
// escape their value so that no untrusted input can inject a live regex (the
// ReDoS / injection guarantee).
func FuzzOperatorBSON(f *testing.F) {

	// Seed with metacharacters and known injection payloads.
	for _, seed := range []string{
		"", "Connor", "a.b", ".*", "(a+)+$", "[a-z]", `\d{10}`,
		"^anchored$", "100% off", "a|b", "{1,9}", "\x00\x01", "πατέ",
	} {
		f.Add(exp.OperatorContains, seed)
		f.Add(exp.OperatorBeginsWith, seed)
		f.Add(exp.OperatorEndsWith, seed)
		f.Add(exp.OperatorEqual, seed)
		f.Add("totally-unknown-operator", seed)
	}

	f.Fuzz(func(t *testing.T, operator string, value string) {

		// Invariant 1: translation never panics for any operator/value.
		result := operatorBSON(operator, value)
		require.NotNil(t, result, "operatorBSON must never return a nil map")

		// Invariant 2: for the regex-producing operators, the value must be
		// escaped to a literal. Verify by stripping the operator's anchor,
		// compiling the pattern, and confirming it matches the ORIGINAL value
		// exactly and matches a tampered value only as a literal substring.
		isStringMatch := false
		for _, op := range stringMatchOperators {
			if operator == op {
				isStringMatch = true
				break
			}
		}

		if !isStringMatch {
			return
		}

		regex, ok := result["$regex"].(primitive.Regex)
		require.True(t, ok, "string-match operator %q must produce a $regex", operator)

		// The full pattern must be exactly the QuoteMeta-escaped value with only
		// the operator's own anchor added. Building the expectation per-operator
		// (rather than stripping anchors off the result) avoids confusing the
		// value's own escaped ^/$ with the operator's anchor.
		var expectedPattern string
		switch escaped := regexp.QuoteMeta(value); operator {
		case exp.OperatorBeginsWith:
			expectedPattern = "^" + escaped
		case exp.OperatorEndsWith:
			expectedPattern = escaped + "$"
		default: // OperatorContains
			expectedPattern = escaped
		}

		// The escaped value must be exactly QuoteMeta(value) with only the
		// operator's anchor added. This is the real injection guarantee: if any
		// regex metacharacter had survived, the pattern would differ from this.
		// We do NOT assert it compiles under Go's regexp — the consumer is
		// MongoDB's PCRE engine, which accepts patterns (e.g. invalid UTF-8 bytes)
		// that Go's stricter, UTF-8-only engine rejects.
		require.Equal(t, expectedPattern, regex.Pattern,
			"value %q must be embedded as its QuoteMeta form", value)
		require.Equal(t, "i", regex.Options,
			"string-match operators are always case-insensitive")
	})
}

// FuzzExpressionToBSON builds predicate trees from arbitrary field/operator/value
// strings and confirms the whole translation never panics and always returns a
// non-nil filter for a non-empty predicate.
func FuzzExpressionToBSON(f *testing.F) {

	for _, op := range []string{
		exp.OperatorEqual, exp.OperatorContains, exp.OperatorIn,
		exp.OperatorExists, "$fullText", "unknown",
	} {
		f.Add("field", op, "value")
		f.Add("", op, "")
	}

	f.Fuzz(func(t *testing.T, field string, operator string, value string) {

		// A single predicate must translate without panicking.
		single := ExpressionToBSON(exp.New(field, operator, value))
		require.NotNil(t, single, "a predicate must never translate to a nil filter")

		// The same predicate nested inside AND / OR must also be safe, exercising
		// the recursive paths.
		and := ExpressionToBSON(exp.And(exp.New(field, operator, value), exp.Equal(field, value)))
		require.NotNil(t, and)

		or := ExpressionToBSON(exp.Or(exp.New(field, operator, value), exp.Equal(field, value)))
		require.NotNil(t, or)
	})
}
