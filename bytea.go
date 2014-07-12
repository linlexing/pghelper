package pghelper

import (
	"database/sql/driver"
)

type Bytea []byte

func (b *Bytea) Scan(value interface{}) error {
	switch t := value.(type) {
	case []byte:
		*b = t
		return nil
	case string:
		*b = []byte(t)
		return nil
	case Bytea:
		*b = t
		return nil
	default:
		return ERROR_Convert(value, b)

	}
}
func (b Bytea) Value() (driver.Value, error) {
	return []byte(b), nil
}

type NullBytea struct {
	Bytea []byte
	Valid bool
}

func (f *NullBytea) Scan(value interface{}) error {
	if value == nil {
		f.Valid = false
		f.Bytea = nil
	} else {
		f.Bytea, f.Valid = value.([]byte)
	}

	return nil
}
func (f NullBytea) Value() (driver.Value, error) {
	if !f.Valid {
		return nil, nil
	}
	return f.Bytea, nil
}
func (n NullBytea) GetValue() interface{} {
	if n.Valid {
		return n.Bytea
	} else {
		return nil
	}
}
func (n *NullBytea) SetValue(value interface{}) {
	if value == nil {
		n.Valid = false
		n.Bytea = nil
	} else {
		n.Valid = true
		n.Bytea = value.(Bytea)
	}
}
