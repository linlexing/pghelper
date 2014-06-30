// datatable project datatable.go
package pghelper

import (
	"database/sql"
	"fmt"
	"github.com/linlexing/datatable.go"

	"reflect"
	"strings"
)

type Index struct {
	Define string
	Desc   string
}

func (i *Index) Clone() *Index {
	return &Index{
		Define: i.Define,
		Desc:   i.Desc,
	}
}

type DataTable struct {
	*datatable.DataTable
	Columns []*DataColumn
	Indexes map[string]*Index
	Desc    string
}

func NewIndex(define string) *Index {
	return &Index{Define: define, Desc: ""}
}

func NewDataTable(name string) *DataTable {
	return &DataTable{
		datatable.NewDataTable(name),
		nil,
		map[string]*Index{},
		"",
	}
}
func (d *DataTable) PrimaryKeys() []*DataColumn {
	pks := d.DataTable.PrimaryKeys()
	rev := make([]*DataColumn, len(pks))
	for i, v := range pks {
		rev[i] = d.Columns[v.Index()]
	}
	return rev
}

//Assign each column empty value pointer,General used by database/sql scan
func (d *DataTable) NewPtrValues() []interface{} {
	result := make([]interface{}, d.ColumnCount())
	for i, c := range d.Columns {
		result[i] = c.PtrZeroValue()
	}
	return result
}
func safeToString(s interface{}) string {

	if s == nil {
		return ""
	}
	switch r := s.(type) {
	case string:
		return r
	case []byte:
		return string(r)
	default:
		return fmt.Sprintf("%v", s)
	}

}
func nullToNil(value ...interface{}) []interface{} {
	rev := make([]interface{}, len(value))
	for i, v := range value {
		switch tv := v.(type) {
		case IsNull:
			if tv.IsNull() {
				rev[i] = nil
			}
			tmp, err := tv.Value()
			if err != nil {
				panic(err)
			}
			rev[i] = tmp
		default:
			rev[i] = tv
		}
	}
	return rev
}
func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func (d *DataTable) AsTabText(columns ...string) string {
	result := []string{}
	if len(columns) > 0 {
		result = append(result, strings.Join(columns, "\t"))
	} else {
		result = append(result, strings.Join(d.ColumnNames(), "\t"))
	}
	for i := 0; i < d.RowCount(); i++ {
		r := d.GetRow(i)
		line := []string{}
		for j := 0; j < d.ColumnCount(); j++ {
			c := d.Columns[j]
			if len(columns) > 0 && !stringInSlice(c.Name, columns) {
				continue
			}
			if r[c.Name] == nil {
				line = append(line, "")
			} else {
				line = append(line, fmt.Sprintf("%v", r[c.Name]))
			}
		}
		result = append(result, strings.Join(line, "\t"))
	}
	return strings.Join(result, "\n")
}

//convert NULL to nil
func (d *DataTable) GetValue(rowIndex, colIndex int) interface{} {
	return nullToNil(d.DataTable.GetValue(rowIndex, colIndex))[0]
}
func (d *DataTable) GetColumnValues(columnIndex int) []interface{} {
	newValues := make([]interface{}, d.RowCount())
	for i := 0; i < d.RowCount(); i++ {
		newValues[i] = d.GetValue(i, columnIndex)
	}
	return newValues
}
func (d *DataTable) GetColumnStrings(columnIndex int) []string {
	rev := make([]string, d.RowCount())
	for i, v := range d.GetColumnValues(columnIndex) {
		rev[i] = safeToString(v)
	}
	return rev
}
func (d *DataTable) nilToNULL(row []interface{}) ([]interface{}, error) {
	rev := make([]interface{}, len(row))
	for i, col := range d.Columns {
		tmp := col.PtrZeroValue()
		switch t := tmp.(type) {
		case sql.Scanner:
			err := t.Scan(row[i])
			if err != nil {
				return nil, err
			}
			rev[i] = reflect.ValueOf(tmp).Elem().Interface()
		default:
			if row[i] == nil {
				panic(fmt.Errorf("nil --> %s error", col.DataType.String()))
			}
			rev[i] = row[i]
		}
	}
	return rev, nil
}
func (d *DataTable) getSequenceValues(r map[string]interface{}) []interface{} {
	vals := make([]interface{}, d.ColumnCount())
	for i, col := range d.Columns {
		var ok bool
		if vals[i], ok = r[col.Name]; !ok {
			panic(fmt.Errorf("can't find column:[%s] at %v", col.Name, r))
		}

	}
	return vals

}
func (d *DataTable) AddRow(r map[string]interface{}) error {
	return d.AddValues(d.getSequenceValues(r)...)
}
func (d *DataTable) NewRow() map[string]interface{} {
	result := map[string]interface{}{}
	for _, col := range d.Columns {
		result[col.Name] = nullToNil(col.ZeroValue())[0]
	}
	return result
}
func (d *DataTable) GetRow(rowIndex int) map[string]interface{} {
	vals := d.GetValues(rowIndex)
	result := map[string]interface{}{}
	for i, col := range d.Columns {
		result[col.Name] = vals[i]
	}
	return result
}
func (d *DataTable) Rows() []map[string]interface{} {
	rev := []map[string]interface{}{}
	for i := 0; i < d.RowCount(); i++ {
		vals := d.GetValues(i)
		result := map[string]interface{}{}
		for i, col := range d.Columns {
			result[col.Name] = vals[i]
		}
		rev = append(rev, result)
	}
	return rev
}
func (d *DataTable) UpdateRow(rowIndex int, r map[string]interface{}) error {
	return d.SetValues(rowIndex, d.getSequenceValues(r)...)
}
func (d *DataTable) AddValues(vs ...interface{}) (err error) {
	v, err := d.nilToNULL(vs)
	if err != nil {
		return err
	}
	return d.DataTable.AddValues(v...)
}
func (d *DataTable) SetValues(rowIndex int, values ...interface{}) (err error) {
	vs, err := d.nilToNULL(values)
	if err != nil {
		return err
	}
	return d.DataTable.SetValues(rowIndex, vs...)
}
func (d *DataTable) GetValues(rowIndex int) []interface{} {
	return nullToNil(d.DataTable.GetValues(rowIndex)...)
}
func (d *DataTable) AddColumn(col *DataColumn) *DataColumn {

	d.DataTable.AddColumn(col.DataColumn)
	d.Columns = append(d.Columns, col)
	return col
}
