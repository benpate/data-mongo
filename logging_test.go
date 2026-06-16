package mongodb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// restoreLogTimeout captures the current global logTimeout and restores it when
// the test ends, so these tests don't leak state into one another.
func restoreLogTimeout(t *testing.T) {
	t.Helper()
	original := logTimeout
	t.Cleanup(func() { logTimeout = original })
}

func TestSetLogTimeout(t *testing.T) {
	restoreLogTimeout(t)

	SetLogTimeout(500)
	assert.Equal(t, 500, logTimeout)
}

// A negative timeout is clamped to zero (logging disabled).
func TestSetLogTimeout_NegativeClampsToZero(t *testing.T) {
	restoreLogTimeout(t)

	SetLogTimeout(-100)
	assert.Equal(t, 0, logTimeout)
}

func TestSetLogTimeout_Zero(t *testing.T) {
	restoreLogTimeout(t)

	SetLogTimeout(0)
	assert.Equal(t, 0, logTimeout)
}

/******************************************
 * isTimeoutExceeded()
 ******************************************/

// When logging is disabled (logTimeout <= 0), nothing is ever "exceeded".
func TestIsTimeoutExceeded_Disabled(t *testing.T) {
	restoreLogTimeout(t)
	logTimeout = 0

	assert.False(t, isTimeoutExceeded(time.Now().UnixMilli()-9999))
}

// A start time far in the past exceeds the configured threshold.
func TestIsTimeoutExceeded_Exceeded(t *testing.T) {
	restoreLogTimeout(t)
	logTimeout = 10

	startTime := time.Now().UnixMilli() - 1000 // 1 second ago, threshold is 10ms
	assert.True(t, isTimeoutExceeded(startTime))
}

// A start time of "right now" does not exceed the threshold.
func TestIsTimeoutExceeded_NotExceeded(t *testing.T) {
	restoreLogTimeout(t)
	logTimeout = 10_000 // 10 second threshold

	assert.False(t, isTimeoutExceeded(time.Now().UnixMilli()))
}
