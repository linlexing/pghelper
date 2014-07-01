package pghelper

import (
	"database/sql"
	"database/sql/driver"
)

type NullBool struct {
	Bool  bool
	Valid bool // Valid is true if Bool is not NULL
}

// Scan implements the Scanner interface.
func (n *NullBool) Scan(value interface{}) error {
	newv := &sql.NullBool{}
	if err := newv.Scan(value); err != nil {
		return err
	}
	n.Bool, n.Valid = newv.Bool, newv.Valid
	return nil
}

// Value implements the driver Valuer interface.
func (n NullBool) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Bool, nil
}
func (this NullBool) IsNull() bool {
	return !this.Valid
}
