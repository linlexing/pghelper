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
	newv := &sql.NullString{}
	if err := newv.Scan(value); err != nil {
		return err
	}
	ns.String, ns.Valid = newv.String, newv.Valid
	return nil
}

// Value implements the driver Valuer interface.
func (ns NullString) Value() (driver.Value, error) {
	if !ns.Valid {
		return nil, nil
	}
	return ns.String, nil
}

func (this NullString) IsNull() bool {
	return !this.Valid
}
