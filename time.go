package microrm

import "time"

type Time struct{}

func (t Time) Now() time.Time {
	return time.Now()
}
