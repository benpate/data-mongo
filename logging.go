package mongodb

import "time"

// Default timeout for logging slow queries (0 = do not log)
var logTimeout int = 0

func SetLogTimeout(timeout int) {
	if timeout < 0 {
		timeout = 0
	}
	logTimeout = timeout
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
