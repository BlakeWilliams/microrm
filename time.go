package dbmap

import "time"

type realClock struct{}

func (t realClock) Now() time.Time {
	return time.Now()
}
