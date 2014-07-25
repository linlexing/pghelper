package pghelper

import (
	"bytes"
	"fmt"
	"github.com/linlexing/datatable.go"
	"github.com/linlexing/dbhelper"
	"regexp"
	"strconv"
	"strings"
)

type PgMeta struct {
	*dbhelper.RootMeta
}

var regVarchar = regexp.MustCompile(`^character varying\((\d+)\)$`)

func NewPgMeta() *PgMeta {
	return &PgMeta{&dbhelper.RootMeta{}}
}
func (m *PgMeta) ParamPlaceholder(strSql string, num int) string {
	if num == 0 {
		return strSql
	}
	pp := make([]interface{}, num)
	for i := 0; i < num; i++ {
		pp[i] = fmt.Sprintf("$%d", i)
	}
	return fmt.Sprintf(strSql, pp...)
}

func (p *PgMeta) TableExists(tablename string) (bool, error) {
	return p.DBHelper.Exists(fmt.Sprintf(`
	    SELECT 1
	    FROM information_schema.tables
	    WHERE
	      table_schema = current_schema AND
	      table_name = %s`, tablename))
}
func (p *PgMeta) DropPrimaryKey(tablename, pkConstraintName string) error {
	_, err := p.DBHelper.Exec(fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", tablename, pkConstraintName))
	return err
}
func (p *PgMeta) DropIndex(tablename, indexname string) error {
	_, err := p.DBHelper.Exec(fmt.Sprintf("DROP INDEX %s", indexname))
	return err
}
func (p *PgMeta) getColumnDefine(dataType datatable.ColumnType, maxSize int) string {
	rev := ""
	switch dataType {
	case datatable.String:
		if maxSize > 0 {
			rev = fmt.Sprintf("character varying(%d)", maxSize)
		} else {
			rev = "text"
		}
	case datatable.Bool:
		rev = "boolean"
	case datatable.Int64:
		rev = "bigint"
	case datatable.Float64:
		rev = "double precision"
	case datatable.Time:
		rev = "timestamp without time zone"
	case datatable.Bytea:
		rev = "bytea"
	default:
		panic(fmt.Errorf("the type %s invalid", dataType))
	}
	return rev
}
func (p *PgMeta) StringExpress(value string) string {
	var rev bytes.Buffer

	for _, c := range value {
		if c == 0 {
			rev.WriteString(`\0`)
		} else {
			switch c {
			case '\'':
				rev.WriteString(`\'`)
			case '\b':
				rev.WriteString(`\b`)
			case '\n':
				rev.WriteString(`\n`)
			case '\r':
				rev.WriteString(`\r`)
			case '\t':
				rev.WriteString(`\t`)
			case '\\':
				rev.WriteString(`\\`)
			default:
				rev.WriteRune(c)
			}
		}
	}
	return "E'" + rev.String() + "'"
}
func getDefault(t datatable.ColumnType) string {
	switch t {
	case datatable.String:
		return "''"
	case datatable.Bool:
		return "false"
	case datatable.Int64, datatable.Float64:
		return "0"
	case datatable.Time:
		return "'allballs'::time"
	}
	return "NULL"

}
func (p *PgMeta) AlterColumn(tablename string, oldColumn, newColumn *dbhelper.TableColumn) error {
	if oldColumn.Name != newColumn.Name {
		if _, err := p.DBHelper.Exec(fmt.Sprintf("ALTER TABLE %s RENAME %s TO %s", tablename, oldColumn.Name, newColumn.Name)); err != nil {
			return err
		}
	}
	if oldColumn.Type != newColumn.Type || oldColumn.MaxSize != newColumn.MaxSize {
		if _, err := p.DBHelper.Exec(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s", tablename, newColumn.Name, p.getColumnDefine(newColumn.Type, newColumn.MaxSize))); err != nil {
			return err
		}
	}
	if oldColumn.NotNull != newColumn.NotNull {
		if newColumn.NotNull {
			if _, err := p.DBHelper.Exec(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s", tablename, newColumn.Name, getDefault(newColumn.Type))); err != nil {
				return err
			}
			if _, err := p.DBHelper.Exec(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL", tablename, newColumn.Name)); err != nil {
				return err
			}
		} else {
			if _, err := p.DBHelper.Exec(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL", tablename, newColumn.Name)); err != nil {
				return err
			}
		}
	}
	if !oldColumn.Desc.Equal(newColumn.Desc) {
		if newColumn.Desc.IsEmpty() {
			if _, err := p.DBHelper.Exec(fmt.Sprintf("COMMENT ON COLUMN %s.%s IS NULL", tablename, newColumn.Name)); err != nil {
				return err
			}
		} else {
			if _, err := p.DBHelper.Exec(fmt.Sprintf("COMMENT ON COLUMN %s.%s IS %s", tablename, newColumn.Name, p.StringExpress(newColumn.Desc.String()))); err != nil {
				return err
			}
		}
	}
	return nil
}
func (p *PgMeta) AlterTableDesc(tablename string, desc dbhelper.DBDesc) error {
	if desc.IsEmpty() {
		_, err := p.DBHelper.Exec(fmt.Sprintf("COMMENT ON TABLE %v IS NULL", tablename))
		return err

	} else {
		_, err := p.DBHelper.Exec(fmt.Sprintf("COMMENT ON TABLE %v IS %s", tablename, p.StringExpress(desc.String())))
		return err
	}
}
func (p *PgMeta) AlterIndex(tablename, indexname string, oldIndex, newIndex *dbhelper.Index) error {
	if err := p.DropIndex(tablename, indexname); err != nil {
		return err
	}
	if err := p.CreateIndex(tablename, indexname, newIndex.Columns, newIndex.Unique, newIndex.Desc); err != nil {
		return err
	}
	return nil
}
func (p *PgMeta) CreateIndex(tableName, indexName string, columns []string, unique bool, desc dbhelper.DBDesc) error {
	uniqueStr := ""
	if unique {
		uniqueStr = "UNIQUE"
	}
	if _, err := p.DBHelper.Exec(fmt.Sprintf("CREATE %sINDEX %s ON %s(%s)", uniqueStr, indexName, tableName, strings.Join(columns, ","))); err != nil {
		return err
	}
	if desc.IsEmpty() {
		_, err := p.DBHelper.Exec(fmt.Sprintf("COMMENT ON INDEX %s IS NULL", indexName))
		return err
	} else {
		_, err := p.DBHelper.Exec(fmt.Sprintf("COMMENT ON INDEX %s IS %s", indexName, p.StringExpress(desc.String())))
		return err
	}
}
func (p *PgMeta) CreateTable(table *dbhelper.DataTable) error {
	creates := make([]string, table.ColumnCount())
	for i, c := range table.Columns {
		nullStr := ""
		if c.NotNull {
			nullStr = fmt.Sprintf(" NOT NULL DEFAULT %s", getDefault(c.DataType))
		}
		creates[i] = fmt.Sprintf("%s %s %s", c.Name, p.getColumnDefine(c.DataType, c.MaxSize), nullStr)
	}
	if table.HasPrimaryKey() {
		creates = append(creates, fmt.Sprintf("PRIMARY KEY(%s)", strings.Join(table.PK, ",")))
	}
	if table.Temporary {
		_, err := p.DBHelper.Exec(fmt.Sprintf("CREATE TEMPORARY TABLE %s(\n%s\n) ON COMMIT DROP", table.TableName, strings.Join(creates, ",")))
		return err
	} else {
		_, err := p.DBHelper.Exec(fmt.Sprintf("CREATE TABLE %s(\n%s\n)", table.TableName, strings.Join(creates, ",")))
		return err
	}
	for _, c := range table.Columns {
		if !c.Desc.IsEmpty() {
			if _, err := p.DBHelper.Exec(fmt.Sprintf("COMMENT ON COLUMN %s.%s IS %s", table.TableName, c.Name, p.StringExpress(c.Desc.String()))); err != nil {
				return err
			}
		}
	}
	if !table.Desc.IsEmpty() {
		p.AlterTableDesc(table.TableName, table.Desc)
	}
	return nil
}
func (p *PgMeta) AddColumn(tablename string, column *dbhelper.TableColumn) error {
	nullStr := ""
	if column.NotNull {
		nullStr = " NOT NULL DEFAULT " + getDefault(column.Type)
	}
	if _, err := p.DBHelper.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s%s", tablename, column.Name, p.getColumnDefine(column.Type, column.MaxSize), nullStr)); err != nil {
		return err

	}
	if !column.Desc.IsEmpty() {
		if _, err := p.DBHelper.Exec(fmt.Sprintf("COMMENT ON COLUMN %s.%s IS %s", tablename, column.Name, p.StringExpress(column.Desc.String()))); err != nil {
			return err
		}
	}
	return nil
}
func (p *PgMeta) AddPrimaryKey(tablename string, pks []string) error {
	_, err := p.DBHelper.Exec(fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY(%s)", tablename, strings.Join(pks, ",")))
	return err
}
func (p *PgMeta) GetTableDesc(tablename string) (dbhelper.DBDesc, error) {
	rev, err := p.DBHelper.QueryOne("select obj_description(%s::regclass,'pg_class')", tablename)
	if err != nil {
		return nil, err
	}
	if rev == nil || rev.(string) == "" {
		return dbhelper.DBDesc{}, nil
	} else {
		v := dbhelper.DBDesc{}
		v.Parse(rev.(string))
		return v, nil
	}
}
func (p *PgMeta) GetIndexes(tablename string) ([]*dbhelper.TableIndex, error) {
	table, err := p.DBHelper.GetData(`
		select
		    i.relname as index_name,
			max(CAST(ix.indisunique AS integer)) as unique,
		    array_to_string(array_agg(a.attname), ',') as columns,
			obj_description(max(i.oid)) as index_desc
		from
		    pg_class t,
		    pg_class i,
		    pg_index ix,
		    pg_attribute a
		where
		    t.oid = ix.indrelid
		    and i.oid = ix.indexrelid
		    and a.attrelid = t.oid
		    and a.attnum = ANY(ix.indkey)
		    and t.relkind = 'r'
		    and t.relname = %s
			and ix.indisprimary = false
		group by
		    t.relname,
		    i.relname
		order by
		    t.relname,
		    i.relname`, tablename)
	if err != nil {
		return nil, err
	}
	rev := make([]*dbhelper.TableIndex, table.RowCount())
	for i := 0; i < table.RowCount(); i++ {
		row := table.Row(i)
		rev[i] = &dbhelper.TableIndex{}
		rev[i].Name = row["index_name"].(string)
		rev[i].Columns = strings.Split(row["columns"].(string), ",")
		if row["unique"].(int64) == 0 {
			rev[i].Unique = false
		} else {
			rev[i].Unique = true
		}
		rev[i].Desc = dbhelper.DBDesc{}
		if row["index_desc"] != nil {
			rev[i].Desc.Parse(row["index_desc"].(string))
		}
	}
	return rev, nil
}
func (p *PgMeta) GetColumns(tablename string) ([]*dbhelper.TableColumn, error) {
	table, err := p.DBHelper.GetData(`
		SELECT
		  a.attname as column_name,
		  a.attnotnull as notnull,
		  pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
		  col_description(b.oid,a.attnum) as column_desc
		FROM
		  pg_catalog.pg_attribute a join
		  (SELECT  c.oid
		   FROM    pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		   WHERE c.relname =%s AND (n.nspname) = current_schema
		  ) b on a.attrelid = b.oid left join
		  pg_catalog.pg_attrdef d ON (a.attrelid, a.attnum) = (d.adrelid,  d.adnum)
		WHERE

		  a.attnum > 0 AND
		  NOT a.attisdropped
		ORDER BY
		  a.attnum`, tablename)
	if err != nil {
		return nil, err
	}
	rev := make([]*dbhelper.TableColumn, table.RowCount())
	for i := 0; i < table.RowCount(); i++ {
		row := table.Row(i)
		rev[i] = &dbhelper.TableColumn{}
		rev[i].Name = row["column_name"].(string)
		t := row["data_type"].(string)
		switch {
		case t == "text":
			rev[i].Type = datatable.String
			rev[i].MaxSize = 0
		case t == "boolean":
			rev[i].Type = datatable.Bool
		case t == "bigint":
			rev[i].Type = datatable.Int64
		case t == "double precision":
			rev[i].Type = datatable.Float64
		case regVarchar.MatchString(t):
			rev[i].Type = datatable.String
			var err error

			if rev[i].MaxSize, err = strconv.Atoi(regVarchar.FindStringSubmatch(t)[1]); err != nil {
				return nil, err
			}
		case t == "timestamp without time zone" ||
			t == "timestamp with time zone" ||
			t == "date":
			rev[i].Type = datatable.Time
		case t == "bytea":
			rev[i].Type = datatable.Bytea

		default:
			return nil, fmt.Errorf("the column %q type %s invalid", row["column_name"], row["data_type"])
		}
		if row["notnull"].(bool) {
			rev[i].NotNull = true
		} else {
			rev[i].NotNull = false
		}
		if row["column_desc"] != nil || row["column_desc"].(string) != "" {
			desc := dbhelper.DBDesc{}
			desc.Parse(row["column_desc"].(string))
			rev[i].Desc = desc
		}
	}
	return rev, nil
}
func (p *PgMeta) GetPrimaryKeys(tablename string) ([]string, error) {
	pks, err := p.DBHelper.QueryOne(`
		SELECT
		  array_to_string(array_agg(pg_attribute.attname),',') as columns
		FROM pg_index, pg_class, pg_attribute ,pg_class idx
		WHERE
		  pg_class.oid = %s::regclass AND
		  pg_index.indrelid = pg_class.oid AND
		  pg_attribute.attrelid = pg_class.oid AND
		  pg_index.indexrelid = idx.oid and
		  pg_attribute.attnum = any(pg_index.indkey) AND
		  indisprimary`, tablename)
	if err != nil {
		return nil, err
	}
	return strings.Split(pks.(string), ","), nil
}
