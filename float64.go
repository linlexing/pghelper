package pghelper

import (
	"database/sql"
	"database/sql/driver"
)

type NullFloat64 struct {
	Valid   bool
	Float64 float64
}

func (this NullFloat64) IsNull() bool {
	return !this.Valid
}

// Scan implements the Scanner interface.
func (n *NullFloat64) Scan(value interface{}) error {
	newV := &sql.NullFloat64{}
	err := newV.Scan(value)
	if err != nil {
		return err
	}
	n.Valid, n.Float64 = newV.Valid, newV.Float64
	return nil
}

// Value implements the driver Valuer interface.
func (n NullFloat64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Float64, nil
}

// NullBool represents a bool that ma
