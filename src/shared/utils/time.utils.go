package utils

import "time"

func NowUTC() time.Time {
	return time.Now().UTC()
}

func FormatRFC3339UTC(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

