package data

import (
	"encoding/json"
	"time"
)

type UnixNanoTime struct{ time.Time }

func (u UnixNanoTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.UnixNano())
}

func (u *UnixNanoTime) UnmarshalJSON(data []byte) error {
	var unixNano int64
	if err := json.Unmarshal(data, &unixNano); err != nil {
		return err
	}
	u.Time = time.Unix(0, unixNano).UTC()
	return nil
}
