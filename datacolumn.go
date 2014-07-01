package pghelper

import (
	"database/sql"
	"fmt"
	"github.com/linlexing/datatable.go"
	"reflect"
)

type DataColumn struct {
	*datatable.DataColumn
	PGType  *PGType
	Default string
	Desc    PGDesc
}

func (d *DataColumn) OriginName() string {
	return safeToString(d.Desc["OriginName"])
}

func (d *DataColumn) Clone() *DataColumn {
	result := DataColumn{}
	result = *d
	result.PGType = d.PGType.Clone()
	return &result
}
func NewColumnT(name string, dt *PGType, def string) *DataColumn {
	return &DataColumn{
		datatable.NewDataColumn(name, dt.ReflectType()),
		dt,
		def,
		PGDesc{},
	}

}
func NewColumn(name string, dataType PGTypeType, param ...interface{}) *DataColumn {
	dt := NewPGType(dataType, 0, false)
	if len(param) > 0 {
		dt.NotNull = param[0].(bool)
	}
	if len(param) > 1 {
		dt.MaxSize = param[1].(int)

	}
	def := ""
	if len(param) > 2 {
		def = param[2].(string)
	}
	if len(param) > 3 {
		panic("too much param")
	}
	return NewColumnT(name, dt, def)
}
func (d *DataColumn) String(value interface{}) (string, error) {
	return d.PGType.EncodeString(value)
}
func (d *DataColumn) Parse(value string) (interface{}, error) {
	return d.PGType.DecodeString(value)
}

//convert the Null* Value to true type value or nil(NULL*.Valid==false)
func (d *DataColumn) Null2Nil(value interface{}) (interface{}, error) {
	if !d.PGType.NotNull {
		switch tv := value.(type) {
		case NullString:
			if tv.Valid {
				return tv.String, nil
			} else {
				return nil, nil
			}
		case NullBool:
			if tv.Valid {
				return tv.Bool, nil
			} else {
				return nil, nil
			}
		case NullInt64:
			if tv.Valid {
				return tv.Int64, nil
			} else {
				return nil, nil
			}
		case NullFloat64:
			if tv.Valid {
				return tv.Float64, nil
			} else {
				return nil, nil
			}
		case NullTime:
			if tv.Valid {
				return tv.Time, nil
			} else {
				return nil, nil
			}
		case NullBytea:
			if tv.Valid {
				return tv.Bytea, nil
			} else {
				return nil, nil
			}
		case NullStringSlice:
			if tv.Valid {
				return tv.Slice, nil
			} else {
				return nil, nil
			}
		case NullBoolSlice:
			if tv.Valid {
				return tv.Slice, nil
			} else {
				return nil, nil
			}
		case NullInt64Slice:
			if tv.Valid {
				return tv.Slice, nil
			} else {
				return nil, nil
			}
		case NullFloat64Slice:
			if tv.Valid {
				return tv.Slice, nil
			} else {
				return nil, nil
			}
		case NullTimeSlice:
			if tv.Valid {
				return tv.Slice, nil
			} else {
				return nil, nil
			}
		case NullJSON:
			if tv.Valid {
				return tv.Json, nil
			} else {
				return nil, nil
			}
		case NullJSONSlice:
			if tv.Valid {
				return tv.Slice, nil
			} else {
				return nil, nil
			}

		default:
			return nil, fmt.Errorf("invalid type %T", value)
		}
	} else {
		return value, nil
	}
}
func (d *DataColumn) Nil2NULL(value interface{}) (interface{}, error) {
	if !d.PGType.NotNull {
		tv := d.PtrZeroValue()
		if err := tv.(sql.Scanner).Scan(value); err != nil {
			return nil, err
		}
		return reflect.ValueOf(tv).Elem().Interface(), nil
	} else {
		return value, nil
	}
}
