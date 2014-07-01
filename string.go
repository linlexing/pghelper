package pghelper

import (
	"database/sql"
	"database/sql/driver"
)

type NullString struct {
	String string
	Valid  bool // Valid is true if String is not NULL
}

// Scan implements the Scanner interface.
func (ns *NullString) Scan(value interface{}) error {
	switch tv := value.(type) {
	case NullString:
		*ns = tv
	default:
		newv := &sql.NullString{}
		if err := newv.Scan(value); err != nil {
			return err
		}
		ns.String, ns.Valid = newv.String, newv.Valid
	}
	return nil
}

// Value implements the driver Valuer interface.
func (ns NullString) Value() (driver.Value, error) {
	if !ns.Valid {
		return nil, nil
	}
	return ns.String, nil
}
func (n NullString) GetValue() interface{} {
	if n.Valid {
		return n.String
	} else {
		return nil
	}
}
func (n *NullString) SetValue(value interface{}) {
	if value == nil {
		n.Valid = false
		n.String = ""
	} else {
		n.Valid = true
		n.String = value.(string)
	}
}
