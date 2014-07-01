package pghelper

import (
	"database/sql/driver"
	"github.com/lib/pq"
	"time"
)

type NullTime struct {
	Time  time.Time
	Valid bool // Valid is true if Time is not NULL
}

// Scan implements the Scanner interface.
func (nt *NullTime) Scan(value interface{}) error {
	switch tv := value.(type) {
	case NullTime:
		*nt = tv
	default:
		vv := &pq.NullTime{}
		if err := vv.Scan(value); err != nil {
			return err
		}
		nt.Time, nt.Valid = vv.Time, vv.Valid
	}
	return nil
}

// Value implements the driver Valuer interface.
func (nt NullTime) Value() (driver.Value, error) {
	if !nt.Valid {
		return nil, nil
	}
	return nt.Time, nil
}
func (n NullTime) GetValue() interface{} {
	if n.Valid {
		return n.Time
	} else {
		return nil
	}
}
func (n *NullTime) SetValue(value interface{}) {
	if value == nil {
		n.Valid = false
		n.Time = time.Time{}
	} else {
		n.Valid = true
		n.Time = value.(time.Time)
	}
}
