package mongodb

import (
	"sync/atomic"
	"time"
)

// logTimeout is the threshold (in milliseconds) above which slow queries are
// logged; zero disables logging.  It is accessed atomically because
// SetLogTimeout may run concurrently with in-flight queries.
var logTimeout atomic.Int64

// SetLogTimeout configures the threshold (in milliseconds) above which slow
// queries are logged.  A value of zero or less disables slow-query logging.
func SetLogTimeout(timeout int) {
	if timeout < 0 {
		timeout = 0
	}
	logTimeout.Store(int64(timeout))
}

// startTimer returns the current time in epoch-milliseconds when slow-query
// logging is enabled, or 0 when it is disabled.
func startTimer() int64 {
	if logTimeout.Load() > 0 {
		return time.Now().UnixMilli()
	}
	return 0
}

// isTimeoutExceeded reports whether the elapsed time since startTime has passed
// the configured slow-query threshold.  It is always false when logging is off.
func isTimeoutExceeded(startTime int64) bool {

	threshold := logTimeout.Load()

	if threshold <= 0 {
		return false
	}

	return time.Now().UnixMilli()-startTime > threshold
}
