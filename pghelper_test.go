package pghelper

import (
	"github.com/linlexing/datatable.go"
	"github.com/linlexing/dbhelper"
	"testing"
)

var (
	dns    string = "host=localhost database=postgres user=root password=123456 sslmode=disable"
	driver string = "postgres"
)

func GetTestTable() *dbhelper.DataTable {
	rev := dbhelper.NewDataTable("test")
	rev.AddColumn(dbhelper.NewDataColumn("pk1", datatable.String, 50, true))
	rev.AddColumn(dbhelper.NewDataColumn("pk2", datatable.Int64, 0, true))
	rev.AddColumn(dbhelper.NewDataColumn("str1", datatable.String, 300, false))
	rev.AddColumn(dbhelper.NewDataColumn("str2", datatable.String, 0, false))
	rev.AddColumn(dbhelper.NewDataColumn("num1", datatable.Float64, 0, true))
	rev.AddColumn(dbhelper.NewDataColumn("bool1", datatable.Bool, 0, true))
	rev.AddColumn(dbhelper.NewDataColumn("time1", datatable.Time, 0, false))
	rev.AddColumn(dbhelper.NewDataColumn("bys1", datatable.Bytea, 0, false))
	rev.SetPK("pk1", "pk2")
	return rev
}
func GetTestTable1() *dbhelper.DataTable {
	rev := dbhelper.NewDataTable("test")
	rev.AddColumn(dbhelper.NewDataColumn("pk1", datatable.String, 50, true))
	rev.AddColumn(dbhelper.NewDataColumn("pk2", datatable.Int64, 0, true))
	rev.AddColumn(dbhelper.NewDataColumn("str1", datatable.String, 200, false))
	rev.AddColumn(dbhelper.NewDataColumn("str2", datatable.String, 0, true))
	rev.AddColumn(dbhelper.NewDataColumn("num1", datatable.Float64, 0, true))
	rev.AddColumn(dbhelper.NewDataColumn("bool1", datatable.String, 0, true))
	rev.AddColumn(dbhelper.NewDataColumn("time1", datatable.String, 0, false))
	rev.AddColumn(dbhelper.NewDataColumn("bys1", datatable.Bytea, 0, false))
	rev.SetPK("pk1", "pk2")
	return rev
}
func TestCreateTable(t *testing.T) {
	ahelper := dbhelper.NewDBHelper(driver, dns)
	if err := ahelper.Open(); err != nil {
		t.Error(err)
	}
	defer ahelper.Close()
	if err := ahelper.DropTable("test"); err != nil {
		t.Error(err)
	}
	if err := ahelper.UpdateStruct(nil, GetTestTable()); err != nil {
		t.Error(err)
	}
}
func TestUpdateStruct(t *testing.T) {
	ahelper := dbhelper.NewDBHelper(driver, dns)
	if err := ahelper.Open(); err != nil {
		t.Error(err)
	}
	defer ahelper.Close()
	if err := ahelper.UpdateStruct(GetTestTable(), GetTestTable1()); err != nil {
		t.Error(err)
	}
}
func TestMerge(t *testing.T) {
	ahelper := dbhelper.NewDBHelper(driver, dns)
	if err := ahelper.Open(); err != nil {
		t.Error(err)
	}
	defer ahelper.Close()
	if err := ahelper.GoExec(`
drop table IF EXISTS a
go
drop table IF EXISTS b
go
create table a(
	id bigint not null,
	name varchar(200),
	style varchar(300),
	style2 varchar(30),
	primary key(id)
)
go
create table b(
	id bigint not null,
	name varchar(200),
	style varchar(300),
	style1 varchar(300),
	primary key(id)
)
go

insert into a(
	id,
	name,
	style,
	style2
)values(1,'name1','','test')
go
insert into a(
	id,
	name,
	style,
	style2
)values(2,'name2','','abc')
go
insert into b(
	id,
	name,
	style,
	style1
)values
	(1,'name11','0','a')
go
insert into b(
	id,
	name,
	style,
	style1
)values
(3,'name1','1','a')
go
insert into b(
	id,
	name,
	style,
	style1
)values
	(4,'name2','2','a')
	`); err != nil {
		t.Error(err)
	}
	if err := ahelper.Merge("a", "b", []string{"id", "name", "style"}, []string{"id"}, true, "id=2"); err != nil {
		t.Error(err)
	}
}
