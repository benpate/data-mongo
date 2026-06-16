package mongodb

import "time"

// Default timeout for logging slow queries (0 = do not log)
var logTimeout int

// SetLogTimeout configures the threshold (in milliseconds) above which slow
// queries are logged.  A value of zero or less disables slow-query logging.
func SetLogTimeout(timeout int) {
	if timeout < 0 {
		timeout = 0
	}
	logTimeout = timeout
}

// startTimer returns the current time in epoch-milliseconds when slow-query
// logging is enabled, or 0 when it is disabled.
func startTimer() int64 {
	if logTimeout > 0 {
		return time.Now().UnixMilli()
	}
	return 0
}

func isTimeoutExceeded(startTime int64) bool {

	if logTimeout <= 0 {
		return false
	}

	if time.Now().UnixMilli()-startTime > int64(logTimeout) {
		return true
	}

	return false
}
