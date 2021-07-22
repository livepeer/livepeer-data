package stats

import (
	"fmt"
	"time"
)

type Window struct{ time.Duration }

func (w Window) MarshalText() ([]byte, error) {
	str := fmt.Sprintf(`%gm`, w.Minutes())
	return []byte(str), nil
}

func (w *Window) UnmarshalText(b []byte) error {
	dur, err := time.ParseDuration(string(b))
	if err != nil {
		return err
	}
	w.Duration = dur
	return nil
}
