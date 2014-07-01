package pghelper

import (
	"database/sql"
	"database/sql/driver"
)

type NullInt64 struct {
	Int64 int64
	Valid bool // Valid is true if Int64 is not NULL
}

// Scan implements the Scanner interface.
func (n *NullInt64) Scan(value interface{}) error {
	switch tv := value.(type) {
	case NullInt64:
		*n = tv
	default:

		newv := &sql.NullInt64{}
		if err := newv.Scan(value); err != nil {
			return err
		}
		n.Int64, n.Valid = newv.Int64, newv.Valid
	}
	return nil
}

// Value implements the driver Valuer interface.
func (n NullInt64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Int64, nil
}
func (n NullInt64) GetValue() interface{} {
	if n.Valid {
		return n.Int64
	} else {
		return nil
	}
}
func (n *NullInt64) SetValue(value interface{}) {
	if value == nil {
		n.Valid = false
		n.Int64 = 0
	} else {
		n.Valid = true
		n.Int64 = value.(int64)
	}
}
