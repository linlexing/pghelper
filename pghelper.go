package pghelper

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"reflect"
	"runtime/debug"
	"strings"
	"text/template"
)

var (
	ERROR_TableNameIsEmpty = errors.New("the table name is empty")
	ERROR_NoRecord         = errors.New("can't found record")
)

type ERROR_NotFoundTable struct {
	TableName string
}

func (e ERROR_NotFoundTable) Error() string {
	return fmt.Sprintf("can't find the table [%s]", e.TableName)
}
func ERROR_DataTypeInvalid(t interface{}) error {
	return fmt.Errorf("the type %T: %s invalid", t, t)
}

type SqlError struct {
	sql    string
	params interface{}
	err    error
}

func (s *SqlError) Error() string {
	return fmt.Sprintf("%v:\n%v\nparams:%v\n", s.err, s.sql, s.params)
}
func NewSqlError(strSql string, err error, params ...interface{}) *SqlError {
	return &SqlError{
		sql:    strSql,
		params: params,
		err:    err,
	}
}

type PGHelper struct {
	tx               *sql.Tx
	connectionString string
}

func NewPGHelper(dburl string) *PGHelper {
	return &PGHelper{connectionString: dburl}
}
func RunAtTrans(dburl string, txFunc func(help *PGHelper) error) (result_err error) {
	help := NewPGHelper(dburl)
	var db *sql.DB
	if db, result_err = sql.Open("postgres", dburl); result_err != nil {
		return
	}
	defer func() {
		if result_err == nil {
			result_err = db.Close()
		} else {
			db.Close()
		}
	}()
	if help.tx, result_err = db.Begin(); result_err != nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			fmt.Print("recover at RunAtTrans\n", string(debug.Stack()))
			switch p := p.(type) {
			case error:
				result_err = p
			default:
				result_err = fmt.Errorf("%s,recover", p)
			}
		}
		if result_err != nil {
			help.tx.Rollback()
			return
		}
		result_err = help.tx.Commit()
	}()
	return txFunc(help)
}

func (p *PGHelper) Schema() (*PGSchema, error) {
	//获取当前用户默认的schema信息
	if tab, err := p.GetDataTable(SQL_GetCurrentSchemaAndDesc); err != nil {
		return nil, err
	} else {
		schema := &PGSchema{
			Name: tab.GetValue(0, 0).(string),
			Desc: &PGSchemaDesc{}}
		json.Unmarshal([]byte(tab.GetValues(0)[1].(string)), schema.Desc)
		return schema, nil
	}

}

func (p *PGHelper) DbUrl() string {
	return p.connectionString
}
func (p *PGHelper) GetDataTable(strSql string, params ...interface{}) (table *DataTable, result_err error) {
	result_err = p.Query(func(rows *sql.Rows) (err error) {
		table, err = internalRows2DataTable(rows)
		return
	}, strSql, params...)
	return
}
func decodePQDesc(descStr string) map[string]interface{} {
	desc := map[string]interface{}{}
	if err := json.Unmarshal([]byte(descStr), &desc); err != nil {
		return map[string]interface{}{}
	}
	return desc

}
func (p *PGHelper) QueryOne(strSql string, params ...interface{}) (result_err error) {
	result_err = p.QueryRow(strSql, params[:len(params)-1], params[len(params)-1])
	return
}
func (p *PGHelper) QueryRow(strSql string, params []interface{}, dest ...interface{}) (result_err error) {
	return p.Query(func(rows *sql.Rows) error {
		if !rows.Next() {
			return ERROR_NoRecord
		}
		if err := rows.Scan(dest...); err != nil {
			return err
		}
		return nil
	}, strSql, params...)

}
func (p *PGHelper) Query(callBack func(rows *sql.Rows) error, strSql string, params ...interface{}) (result_err error) {
	defer func() {
		if result_err != nil {
			result_err = NewSqlError(strSql, result_err, params...)
		}
	}()
	if p.tx == nil {
		result_err = internalQuery(p.connectionString, callBack, strSql, params...)
	} else {
		result_err = internalQueryTx(p.tx, callBack, strSql, params...)
	}
	return
}
func (p *PGHelper) QueryBatch(callBack func(rows *sql.Rows) error, strSql string, params ...[]interface{}) (result_err error) {
	defer func() {
		if result_err != nil {
			ps := make([]interface{}, len(params))
			for i, v := range params {
				ps[i] = v
			}
			result_err = NewSqlError(strSql, result_err, ps...)
		}
	}()
	if p.tx == nil {
		result_err = internalQueryBatch(p.connectionString, callBack, strSql, params...)
	} else {
		result_err = internalQueryBatchTx(p.tx, callBack, strSql, params...)
	}
	return
}

func (p *PGHelper) GetSafeString(strSQL string, params ...interface{}) string {
	v, err := p.GetString(strSQL, params...)
	if err != nil {
		return ""
	} else {
		return v
	}
}
func (p *PGHelper) GetString(strSQL string, params ...interface{}) (string, error) {
	v := sql.NullString{}
	if err := p.QueryOne(strSQL, append(params, &v)...); err != nil {
		return "", err
	} else {
		return v.String, nil
	}
}
func (p *PGHelper) GetInt(strSQL string, params ...interface{}) int {
	return int(p.GetInt64(strSQL, params))
}
func (p *PGHelper) GetInt64(strSQL string, params ...interface{}) int64 {
	v := sql.NullInt64{}
	if err := p.QueryOne(strSQL, append(params, &v)...); err != nil {
		return 0
	} else {
		return v.Int64
	}
}
func (p *PGHelper) GetBool(strSQL string, params ...interface{}) bool {
	v := sql.NullBool{}
	if err := p.QueryOne(strSQL, append(params, &v)...); err != nil {
		return false
	} else {
		return v.Bool
	}
}
func (p *PGHelper) getTableDesc(tname string) PGDesc {
	str := p.GetSafeString(SQL_GetTableDesc, tname)
	rev := PGDesc{}
	if str == "" {
		return rev
	} else {
		rev.Parse(str)
	}
	return rev
}
func (p *PGHelper) alterTableDesc(tname string, desc PGDesc) error {

	return p.ExecuteSql(fmt.Sprintf(SQL_AlterTableDesc, tname, pqSignStr(desc.String())))
}
func (p *PGHelper) TableExists(tablename string) bool {
	b := p.GetBool(SQL_TableExists, tablename)
	return b
}

func (p *PGHelper) Table(tablename string) (*DBTable, error) {
	result := NewDataTable(tablename)
	if !p.TableExists(tablename) {
		return nil, ERROR_NotFoundTable{tablename}
	}
	//获取描述
	result.Desc = p.getTableDesc(tablename)
	//获取字段

	tMeta, err := p.GetDataTable(SQL_TableColumns, tablename)
	if err != nil {
		return nil, err
	}
	for i := 0; i < tMeta.RowCount(); i++ {
		oneRow := tMeta.GetRow(i)
		dt := PGType{}
		if err := dt.SetDBType(oneRow["datatype"].(string)); err != nil {
			return nil, err
		}
		dt.NotNull = oneRow["notnull"].(bool)
		newColumn := NewColumnT(oneRow["columnname"].(string), &dt, safeToString(oneRow["def"]))
		newColumn.Desc.Parse(safeToString(oneRow["desc"]))
		result.AddColumn(newColumn)
	}
	//获取主键
	tPks, err := p.GetDataTable(SQL_TablePrimaryKeys, tablename)
	if err != nil {
		return nil, err
	}
	pkColumns := []string{}
	if tPks.RowCount() > 0 {
		pkColumns = tPks.GetColumnStrings(0)
		result.PKConstraintName = tPks.GetColumnStrings(1)[0]
		result.SetPK(pkColumns...)
	}
	//获取索引
	tIndexes, err := p.GetDataTable(SQL_TableIndexes, tablename)
	if err != nil {
		return nil, err
	}
	for i := 0; i < tIndexes.RowCount(); i++ {
		oneRow := tIndexes.GetRow(i)
		oneIndex := NewIndex(oneRow["define"].(string))
		oneIndex.Desc.Parse(safeToString(oneRow["desc"]))
		result.AddIndex(oneRow["indexname"].(string), oneIndex)
	}

	return &DBTable{result, p}, nil
}

func (p *PGHelper) ExecuteSql(strSql string, params ...interface{}) (result_err error) {
	if strings.Trim(strSql, " \t\n\r") == "" {
		return nil
	}
	defer func() {
		if result_err != nil {
			result_err = NewSqlError(strSql, result_err, params...)
		}
	}()
	if p.tx == nil {
		result_err = internalExec(p.connectionString, strSql, params...)
	} else {
		result_err = internalExecTx(p.tx, strSql, params...)
	}
	return
}
func (p *PGHelper) ExecuteBatch(strSql string, params ...[]interface{}) (result_err error) {
	defer func() {
		if result_err != nil {
			ps := make([]interface{}, len(params))
			for i, v := range params {
				ps[i] = v
			}
			result_err = NewSqlError(strSql, result_err, ps...)
		}
	}()
	if p.tx == nil {
		result_err = internalBatchExec(p.connectionString, strSql, params...)
	} else {
		result_err = internalBatchExecTx(p.tx, strSql, params...)
	}
	return
}
func (p *PGHelper) GetDataTableBatch(strSql string, params ...[]interface{}) (table *DataTable, result_err error) {
	result_err = p.QueryBatch(func(rows *sql.Rows) (err error) {
		if table == nil {
			table, result_err = internalRows2DataTable(rows)
			if result_err != nil {
				return
			}
		} else {
			_, result_err = internalRowsFillTable(rows, table, 0, false)
			if result_err != nil {
				return
			}
		}
		return
	}, strSql, params...)
	return
}
func (p *PGHelper) alterColumnDesc(tname, cname string, desc PGDesc) error {
	return p.ExecuteSql(fmt.Sprintf(SQL_AlterColumnDesc, tname, cname, pqSignStr(desc.String())))
}
func (p *PGHelper) dropConstraint(tname, cname string) error {
	if tname == "" {
		return fmt.Errorf("the tablename is empty,at dropConstraint")
	}
	if cname == "" {
		return fmt.Errorf("the constraint name is emtpy,at dropConstraint")
	}

	return p.ExecuteSql(fmt.Sprintf(SQL_DropConstraint, tname, cname))
}
func (p *PGHelper) createColumn(tname, cname string, dt *PGType, def string) error {
	defstr := ""
	if len(def) > 0 {
		defstr = "DEFAULT " + def
	}
	return p.ExecuteSql(fmt.Sprintf(SQL_CreateColumn, tname, cname, dt.DBString(), defstr))
}
func (p *PGHelper) createTable(tname string) error {
	return p.ExecuteSql(fmt.Sprintf(SQL_CreateTable, tname))
}
func (p *PGHelper) createTempTable(tname string) error {
	return p.ExecuteSql(fmt.Sprintf(SQL_CreateTempTable, tname))
}
func (p *PGHelper) alterIndexDesc(name string, desc PGDesc) error {
	return p.ExecuteSql(fmt.Sprintf(SQL_AlterIndexDesc, name, pqSignStr(desc.String())))
}
func (p *PGHelper) createPrimaryKey(tname string, cname []string) error {
	return p.ExecuteSql(fmt.Sprintf(SQL_CreatePrimaryKey, tname, strings.Join(cname, ",")))
}
func (p *PGHelper) renameColumn(tname, oldName, newName string) error {
	return p.ExecuteSql(fmt.Sprintf(SQL_RenameColumn, tname, oldName, newName))
}
func (p *PGHelper) dropIndex(name string) error {
	return p.ExecuteSql(fmt.Sprintf(SQL_DropIndex, name))
}
func (p *PGHelper) dropColumns(tname string, columns ...string) error {
	for _, v := range columns {
		if err := p.ExecuteSql(fmt.Sprintf(SQL_DropColumn, tname, v)); err != nil {
			return err
		}
	}
	return nil
}

func (p *PGHelper) alterColumnType(tname, cname string, dt *PGType) error {
	return p.ExecuteSql(fmt.Sprintf(SQL_AlterColumnType, tname, cname, dt.DBString()))
}
func (p *PGHelper) dropColumnNotNull(tname, cname string) error {
	return p.ExecuteSql(fmt.Sprintf(SQL_DropColumnNotNull, tname, cname))
}
func (p *PGHelper) setColumnNotNull(tname, cname string) error {
	return p.ExecuteSql(fmt.Sprintf(SQL_SetColumnNotNull, tname, cname))
}
func (p *PGHelper) dropColumnDefault(tname, cname string) error {
	return p.ExecuteSql(fmt.Sprintf(SQL_DropColumnDefault, tname, cname))
}
func (p *PGHelper) setColumnDefault(tname, cname, def string) error {
	return p.ExecuteSql(fmt.Sprintf(SQL_SetColumnDefault, tname, cname, def))
}
func (p *PGHelper) UpdateStruct(oldStruct, newStruct *DataTable) error {
	if len(newStruct.TableName) == 0 {
		return ERROR_TableNameIsEmpty
	}
	tablename := newStruct.TableName
	if oldStruct == nil {
		if newStruct.Temp {
			p.createTempTable(newStruct.TableName)

		} else {
			p.createTable(newStruct.TableName)
		}
		oldStruct = NewDataTable(tablename)
	}

	//首先判断主关键字是否有变化
	bKeyChange := false
	if !reflect.DeepEqual(oldStruct.PK, newStruct.PK) {
		bKeyChange = true
	}
	if !bKeyChange {
		//判断主键的数据类型是否变化
		oldPks := oldStruct.PK
		newPks := newStruct.PK
		for i := 0; i < len(oldPks); i++ {
			if !reflect.DeepEqual(oldStruct.Columns[oldStruct.ColumnIndex(oldPks[i])].PGType, newStruct.Columns[newStruct.ColumnIndex(newPks[i])].PGType) {
				bKeyChange = true
				break
			}
		}
	}
	if bKeyChange && oldStruct.HasPrimaryKey() {
		//删除主键
		if err := p.dropConstraint(tablename, oldStruct.PKConstraintName); err != nil {
			return err
		}
	}
	//找出相对应的一对字段
	oldColumns := oldStruct.Columns
	newColumns := []*DataColumn{}
	for _, v := range newStruct.Columns {
		newColumns = append(newColumns, v)
	}
	type FoundColumn struct {
		OldColumn *DataColumn
		NewColumn *DataColumn
	}
	foundColumns := []FoundColumn{}

	for _, vNew := range newColumns {
		trueNewName := vNew.Name

		if vNew.OriginName() != "" && vNew.Name != vNew.OriginName() {
			trueNewName = vNew.OriginName()
		}
		for _, vOld := range oldColumns {
			if vOld.Name == trueNewName {
				foundColumns = append(foundColumns, FoundColumn{vOld, vNew})
			}
		}
	}
	//删除字段
	for _, oldColumn := range oldColumns {
		bFound := false
		for _, foundColumn := range foundColumns {
			if oldColumn == foundColumn.OldColumn {
				bFound = true
				break
			}
		}
		//找不到的需要删除
		if !bFound {
			if err := p.dropColumns(tablename, oldColumn.Name); err != nil {
				return err
			}
		}
	}

	//修改字段类型或者重命名
	for _, column := range foundColumns {
		//改名
		if column.NewColumn.Name != column.OldColumn.Name {
			if err := p.renameColumn(tablename, column.OldColumn.Name, column.NewColumn.Name); err != nil {
				return err
			}
		}
		//改类型
		if column.OldColumn.PGType.Type != column.NewColumn.PGType.Type ||
			column.OldColumn.PGType.MaxSize != column.NewColumn.PGType.MaxSize {
			if err := p.alterColumnType(tablename, column.NewColumn.Name, column.NewColumn.PGType); err != nil {
				return err
			}
		}
		//去除NotNull
		if column.OldColumn.PGType.NotNull && !column.NewColumn.PGType.NotNull {
			if err := p.dropColumnNotNull(tablename, column.NewColumn.Name); err != nil {
				return err
			}
		}
		//设置NotNull
		if !column.OldColumn.PGType.NotNull && column.NewColumn.PGType.NotNull {
			if err := p.setColumnNotNull(tablename, column.NewColumn.Name); err != nil {
				return err
			}
		}
		//去除Default
		if len(column.OldColumn.Default) > 0 && len(column.NewColumn.Default) == 0 {
			if err := p.dropColumnDefault(tablename, column.NewColumn.Name); err != nil {
				return err
			}
		}
		//设置Default
		if column.OldColumn.Default != column.NewColumn.Default && len(column.NewColumn.Default) > 0 {
			if err := p.setColumnDefault(tablename, column.NewColumn.Name, column.NewColumn.Default); err != nil {
				return err
			}
		}
		//改描述
		if !column.NewColumn.Desc.Equal(column.OldColumn.Desc) {
			if err := p.alterColumnDesc(tablename, column.NewColumn.Name, column.NewColumn.Desc); err != nil {
				return err
			}
		}
	}
	//新增字段
	for _, newColumn := range newColumns {
		bFound := false
		for _, foundColumn := range foundColumns {
			if newColumn == foundColumn.NewColumn {
				bFound = true
				break
			}
		}
		if !bFound {
			if err := p.createColumn(tablename, newColumn.Name, newColumn.PGType, newColumn.Default); err != nil {
				return err
			}
			//有必要就加描述
			if len(newColumn.Desc) > 0 {
				if err := p.alterColumnDesc(tablename, newColumn.Name, newColumn.Desc); err != nil {
					return err
				}
			}
		}
	}
	if bKeyChange && newStruct.HasPrimaryKey() {
		//创建主键
		if err := p.createPrimaryKey(tablename, newStruct.PK); err != nil {
			return err
		}
	}
	//处理索引
	//删除不存在的,并修改存在的
	for idxName, oldIdx := range oldStruct.Indexes {
		if newIdx, ok := newStruct.Indexes[idxName]; ok {
			if oldIdx.Define != newIdx.Define {
				if err := p.dropIndex(idxName); err != nil {
					return err
				}
				if err := p.ExecuteSql(newIdx.Define, newStruct.TableName, idxName); err != nil {
					return err
				}
			}
			if !oldIdx.Desc.Equal(newIdx.Desc) {
				if err := p.alterIndexDesc(idxName, newIdx.Desc); err != nil {
					return err
				}
			}
		} else {
			if err := p.dropIndex(idxName); err != nil {
				return err
			}
		}
	}
	//新增索引
	for idxName, newIdx := range newStruct.Indexes {
		if _, ok := oldStruct.Indexes[idxName]; !ok {
			if err := p.ExecuteSql(newIdx.Define, newStruct.TableName, idxName); err != nil {
				return err
			}
			if err := p.alterIndexDesc(idxName, newIdx.Desc); err != nil {
				return err
			}
		}
	}
	//处理表的描述
	if !oldStruct.Desc.Equal(newStruct.Desc) {
		if err := p.alterTableDesc(newStruct.TableName, newStruct.Desc); err != nil {
			return err
		}
	}
	return nil
}
func (p *PGHelper) Merge(dest, source string, colNames []string, pkColumns []string, autoRemove bool, sqlWhere string) error {
	if len(pkColumns) == 0 {
		return fmt.Errorf("the primary keys is empty")
	}
	if len(colNames) == 0 {
		return fmt.Errorf("the columns is empty")
	}
	tmp := template.New("sql")
	tmp.Funcs(template.FuncMap{
		"Join": func(value []string, sep, prefix string) string {
			if prefix == "" {
				return strings.Join(value, sep)
			} else {
				rev := make([]string, len(value))
				for i, v := range value {
					rev[i] = prefix + v
				}
				return strings.Join(rev, sep)
			}
		},
		"First": func(value []string) string {
			return value[0]
		},
	})
	tmp, err := tmp.Parse(`
WITH updated as ({{if gt (len .updateColumns) 0}}
        UPDATE {{.destTable}} dest SET
            ({{Join .updateColumns "," ""}}) = ({{Join .updateColumns "," "src."}})
        FROM {{.sourceTable}} src
        WHERE {{range $idx,$colName :=.pkColumns}}
            {{if gt $idx 0}}AND {{end}}dest.{{$colName}}=src.{{$colName}}{{end}}
        RETURNING {{Join .pkColumns "," "src."}}{{else}}
        SELECT {{Join .pkColumns "," "src."}} FROM {{.destTable}} dest JOIN {{.sourceTable}} src USING({{Join .pkColumns "," ""}})
        {{end}}
    ){{if .autoRemove}},
    deleted as (
        DELETE FROM {{.destTable}} dest WHERE{{if ne .sqlWhere ""}}
            ({{.sqlWhere}}) AND {{end}}
            NOT EXISTS(
                SELECT 1 FROM {{.sourceTable}} src WHERE{{range $idx,$colName :=.pkColumns}}
                    {{if gt $idx 0}}AND {{end}}dest.{{$colName}}=src.{{$colName}}{{end}}
            )
    ){{end}}
INSERT INTO {{.destTable}}(
    {{Join .colNames ",\n    " ""}}
)
SELECT
    {{Join .colNames ",\n    " "src."}}
FROM
    {{.sourceTable}} src LEFT JOIN updated USING({{Join .pkColumns "," ""}})
WHERE updated.{{First .pkColumns}} IS NULL`)
	if err != nil {
		return err
	}
	var b bytes.Buffer
	//primary key not update
	updateColumns := []string{}

	for _, v := range colNames {
		bFound := false
		for _, pv := range pkColumns {
			if v == pv {
				bFound = true
				break
			}
		}
		if !bFound {
			updateColumns = append(updateColumns, v)
		}
	}

	param := map[string]interface{}{
		"destTable":     dest,
		"sourceTable":   source,
		"updateColumns": updateColumns,
		"colNames":      colNames,
		"autoRemove":    autoRemove,
		"sqlWhere":      sqlWhere,
		"pkColumns":     pkColumns,
	}
	if err := tmp.Execute(&b, param); err != nil {
		return err
	}
	return p.ExecuteSql(b.String())
}
