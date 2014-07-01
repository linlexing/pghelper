package pghelper

import (
	"database/sql"
	"database/sql/driver"
)

type NullFloat64 struct {
	Valid   bool
	Float64 float64
}

// Scan implements the Scanner interface.
func (n *NullFloat64) Scan(value interface{}) error {
	switch tv := value.(type) {
	case NullFloat64:
		*n = tv
	default:

		newV := &sql.NullFloat64{}
		err := newV.Scan(value)
		if err != nil {
			return err
		}
		n.Valid, n.Float64 = newV.Valid, newV.Float64
	}
	return nil
}

// Value implements the driver Valuer interface.
func (n NullFloat64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Float64, nil
}
func (n NullFloat64) GetValue() interface{} {
	if n.Valid {
		return n.Float64
	} else {
		return nil
	}
}
func (n *NullFloat64) SetValue(value interface{}) {
	if value == nil {
		n.Valid = false
		n.Float64 = 0
	} else {
		n.Valid = true
		n.Float64 = value.(float64)
	}
}
