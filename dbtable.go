package pghelper

import (
	"database/sql"
	"fmt"
	"strings"
)

type DBTable struct {
	*DataTable
	dbhelp *PGHelper
}

func valueToStringSlice(value []interface{}) StringSlice {
	if len(value) == 0 {
		return nil
	}
	rev := make(StringSlice, len(value))
	for i, v := range value {
		rev[i] = safeToString(v)
	}
	return rev
}
func NewDBTable(dbhelp *PGHelper, table *DataTable) *DBTable {
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
		_, err = internalRowsFillTable(rows, t.DataTable, 0, false)
		return
	}, strSql, vv...)
	return
}
func (t *DBTable) BatchFill(callBack func(table *DBTable, eof bool) error, batchRow int64, strSql string, params ...interface{}) (result_err error) {
	//convert params,every one type is []interface{},will to first element'type array
	vv := make([]interface{}, len(params))
	for i, v := range params {
		if tv, ok := v.([]interface{}); ok {
			vv[i] = valueToStringSlice(tv)
		} else {
			vv[i] = v
		}

	}
	result_err = t.dbhelp.Query(func(rows *sql.Rows) error {
		for {
			t.Clear()
			eof, err := internalRowsFillTable(rows, t.DataTable, batchRow, false)
			if err != nil {
				return err
			}
			err = callBack(t, eof)
			if err != nil {
				return err
			}
			if eof {
				break
			}
		}
		return nil
	}, strSql, vv...)
	return
}
func (t *DBTable) FillByID(ids ...interface{}) (err error) {
	strSql := buildSelectSql(t.DataTable)
	err = t.Fill(strSql, ids...)
	return
}
func (t *DBTable) FillWhere(strWhere string, params ...interface{}) (err error) {
	if strWhere != "" {
		strWhere = "WHERE " + strWhere
	}
	return t.Fill(fmt.Sprintf("SELECT %s from %s %s",
		strings.Join(t.ColumnNames(), ","), t.TableName, strWhere), params...)
}
func (t *DBTable) Count(strWhere string, params ...interface{}) (count int64, err error) {
	if strWhere != "" {
		strWhere = "WHERE " + strWhere
	}
	err = t.dbhelp.QueryOne(fmt.Sprintf("SELECT COUNT(*) FROM %s %s", t.TableName, strWhere), append(params, &count)...)
	return
}
func (t *DBTable) BatchFillWhere(callBack func(table *DBTable, eof bool) error, batchRow int64, strWhere string, params ...interface{}) (err error) {
	if strWhere != "" {
		strWhere = "WHERE " + strWhere
	}
	return t.BatchFill(callBack, batchRow, fmt.Sprintf("SELECT %s from %s %s",
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
