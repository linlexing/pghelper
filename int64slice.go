package pghelper

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
)

type Int64Slice []int64

func (f *Int64Slice) Scan(value interface{}) error {
	switch t := value.(type) {
	case []byte:
		tmp := parsePGArray(string(t))
		rev := make([]int64, len(tmp))
		for i, tv := range tmp {
			var err error
			rev[i], err = strconv.ParseInt(tv, 10, 64)
			if err != nil {
				return err
			}
		}
		*f = rev
		return nil
	case Int64Slice:
		*f = t
		return nil
	case []int64:
		*f = t
		return nil
	default:
		return ERROR_Convert(value, f)
	}

}
func (f Int64Slice) Value() (driver.Value, error) {
	if len(f) == 0 {
		return nil, nil
	}
	rev := make([]string, len(f))
	for i, v := range f {
		rev[i] = fmt.Sprintf("%d", v)
	}
	return "{" + strings.Join(rev, ",") + "}", nil
}

type NullInt64Slice struct {
	Slice Int64Slice
	Valid bool
}

func (f *NullInt64Slice) Scan(value interface{}) error {
	switch t := value.(type) {
	case NullInt64Slice:
		*f = t
		return nil
	case nil:
		f.Valid = false
		f.Slice = nil
		return nil
	default:
		f.Valid = true
		return (&f.Slice).Scan(value)
	}
}
func (f NullInt64Slice) Value() (driver.Value, error) {
	if !f.Valid {
		return nil, nil
	} else {
		return f.Slice.Value()
	}
}
func (n NullInt64Slice) GetValue() interface{} {
	if n.Valid {
		return n.Slice
	} else {
		return nil
	}
}
func (n *NullInt64Slice) SetValue(value interface{}) {
	if value == nil {
		n.Valid = false
		n.Slice = nil
	} else {
		n.Valid = true
		n.Slice = value.(Int64Slice)
	}
}
