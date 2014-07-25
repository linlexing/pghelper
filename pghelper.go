package pghelper

import (
	_ "github.com/lib/pq"
	"github.com/linlexing/dbhelper"
)

func NewPgHelper(dataSource string) *dbhelper.DBHelper {
	return dbhelper.NewDBHelper("postgres", dataSource, NewPgMeta())
}

/*func (p *PGHelper) Merge(dest, source string, colNames []string, pkColumns []string, autoRemove bool, sqlWhere string) error {
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
}*/
