package pghelper

import (
	"database/sql/driver"
	"github.com/linlexing/datatable.go"
)

type IsNull interface {
	driver.Valuer
	IsNull() bool
}

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
	return encodeString(d.PGType, value)
}
func (d *DataColumn) Parse(value string) (interface{}, error) {
	return decodeString(d.PGType, value)
}
