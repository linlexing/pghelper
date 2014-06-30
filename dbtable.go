package pghelper

import (
	"database/sql"
	"fmt"
	"github.com/linlexing/dbgo/oftenfun"
	"strings"
)

type DBTable struct {
	*DataTable
	dbhelp *PGHelp
}

func valueToStringSlice(value []interface{}) StringSlice {
	if len(value) == 0 {
		return nil
	}
	rev := make(StringSlice, len(value))
	for i, v := range value {
		rev[i] = oftenfun.SafeToString(v)
	}
	return rev
}
func NewDBTable(dbhelp *PGHelp, table *DataTable) *DBTable {
	return &DBTable{table, dbhelp}
}
func (t *DBTable) Fill(strSql string, params ...interface{}) (result_err error) {
	//convert params,every one type is []interface{},will to first element'type array
	vv := make([]interface{}, len(params))
	for i, v := range params {
		if tv, ok := v.([]interface{}); ok {
			vv[i] = valueToStringSlice(tv)
		} else {
			vv[i] = v
		}

	}
	result_err = t.dbhelp.Query(func(rows *sql.Rows) (err error) {
		err = internalRowsFillTable(rows, t.DataTable)
		return
	}, strSql, vv...)
	return
}
func (t *DBTable) FillByID(ids ...interface{}) (err error) {
	strSql := buildSelectSql(t.DataTable)
	err = t.Fill(strSql, ids...)
	return
}
func (t *DBTable) FillWhere(strWhere string, params ...interface{}) (err error) {
	return t.Fill(fmt.Sprintf("SELECT %s from %s WHERE %s",
		strings.Join(t.ColumnNames(), ","), t.TableName, strWhere), params...)
}
func (t *DBTable) Save() (rcount int64, result_err error) {
	if t.dbhelp.tx == nil {
		rcount, result_err = internalUpdateTable(t.dbhelp.connectionString, t.DataTable)
	} else {
		rcount, result_err = internalUpdateTableTx(t.dbhelp.tx, t.DataTable)
	}
	return
}
