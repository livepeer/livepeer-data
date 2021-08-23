package data

import (
	"encoding/json"
	"time"
)

const nanosInMillis = int64(time.Millisecond / time.Nanosecond)

type UnixMillisTime struct{ time.Time }

func NewUnixMillisTime(unixMillis int64) UnixMillisTime {
	return UnixMillisTime{time.Unix(0, unixMillis*nanosInMillis).UTC()}
}

func (u UnixMillisTime) UnixMillis() int64 {
	return u.UnixNano() / nanosInMillis
}

func (u UnixMillisTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.UnixMillis())
}

func (u *UnixMillisTime) UnmarshalJSON(data []byte) error {
	var unixMillis int64
	if err := json.Unmarshal(data, &unixMillis); err != nil {
		return err
	}
	*u = NewUnixMillisTime(unixMillis)
	return nil
}
