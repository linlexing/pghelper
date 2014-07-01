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
	switch tv := value.(type) {
	case NullBool:
		*n = tv
	default:

		newv := &sql.NullBool{}
		if err := newv.Scan(value); err != nil {
			return err
		}
		n.Bool, n.Valid = newv.Bool, newv.Valid
	}
	return nil
}

// Value implements the driver Valuer interface.
func (n NullBool) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Bool, nil
}
func (n NullBool) GetValue() interface{} {
	if n.Valid {
		return n.Bool
	} else {
		return nil
	}
}
func (n *NullBool) SetValue(value interface{}) {
	if value == nil {
		n.Valid = false
		n.Bool = false
	} else {
		n.Valid = true
		n.Bool = value.(bool)
	}
}
