package migrator

import (
	"fmt"
	dm "github.com/shopmonkeyus/eds-server/internal/model"
	"github.com/shopmonkeyus/eds-server/internal/util"
	"sort"
	"strings"
)

type SQL interface {
	SQL() string
}

type Action int

const (
	NoAction Action = iota
	DeleteAction
	AddAction
	UpdateAction
)

func (a Action) String() string {
	switch a {
	case NoAction:
		return "No Action"
	case DeleteAction:
		return "Delete"
	case AddAction:
		return "Add"
	case UpdateAction:
		return "Update"
	}
	return "Unknown"
}

type Index struct {
	Table     string
	Name      string
	Type      string
	Columns   []string
	Storing   []string
	Gin       []string
	OpClass   string
	TableType string
}

func (i Index) SQL() string {
	columns := make([]string, 0)
	for _, name := range i.Columns {
		columns = append(columns, fmt.Sprintf(`"%s"`, name))
	}
	if i.IsUnique() {
		return fmt.Sprintf(`CREATE UNIQUE INDEX "%s" ON "%s"(%s)`, i.Name, i.Table, strings.Join(columns, ", "))
	}
	if i.IsPrimaryKey() {
		return fmt.Sprintf(`ALTER TABLE "%s" ALTER PRIMARY KEY USING COLUMNS (%s)`, i.Table, strings.Join(columns, ", "))
	}
	return ""
}

func (i Index) IsPrimaryKey() bool {
	return i.Type == "PRIMARY KEY"
}

func (i Index) IsUnique() bool {
	return i.Type == "UNIQUE"
}

func (i Index) IsInverted() bool {
	return i.Type == "INVERTED"
}

type Constraint struct {
	Table            string
	Name             string
	Column           string
	ReferencedTable  string
	ReferencedColumn string
	UpdateRule       string
	DeleteRule       string
}

func (c Constraint) SQL() string {
	return fmt.Sprintf(
		`ALTER TABLE "%s" ADD CONSTRAINT "%s" FOREIGN KEY ("%s") REFERENCES "%s"("%s") ON DELETE %s ON UPDATE %s`,
		c.Table,
		c.Name,
		c.Column,
		c.ReferencedTable,
		c.ReferencedColumn,
		c.DeleteRule,
		c.UpdateRule,
	)
}

type Column struct {
	Table               string
	Name                string
	Default             *string
	IsNullable          bool
	IsHidden            bool
	DataType            string
	MaxLength           *string
	UserDefinedTypeName *string
	// CRDBType            string
	// Expression          *string
}

func NewColumnFromField(table string, field *dm.Field) Column {
	// var expr *string
	// if field.Computed != nil {
	// 	expr = &field.Computed.Expression
	// }
	return Column{
		Table: table,
		Name:  field.Name,
		// Default:    field.Default,
		IsNullable: true, // TODO: al
		DataType:   field.SQLTypePostgres(),
		// Expression: expr,
	}
}

func (c Column) GetDataType() string {
	return toPrismaType(c.DataType, c.UserDefinedTypeName, c.IsNullable)
}

func (c Column) AlterDefaultSQL(force bool) string {
	if c.Default == nil || force {
		return fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" DROP DEFAULT`, c.Table, c.Name)
	}
	return fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" SET DEFAULT %s`, c.Table, c.Name, *c.Default)
}

func (c Column) AlterNotNullSQL() string {
	if c.IsNullable {
		return fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" DROP NOT NULL`, c.Table, c.Name)
	}
	return fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" SET NOT NULL`, c.Table, c.Name)
}

func (c Column) AlterTypeSQL() string {
	dt := c.DataType
	i := strings.Index(dt, " ") // only take the type on alter
	if i > 0 {
		dt = dt[0 : i-1]
	}
	if c.MaxLength != nil {
		dt += fmt.Sprintf("(%s)", *c.MaxLength)
	}
	return fmt.Sprintf(`ALTER TABLE "%s" ALTER COLUMN "%s" TYPE %s`, c.Table, c.Name, dt)
}

func (c Column) DropSQL() string {
	return fmt.Sprintf(`ALTER TABLE "%s" DROP COLUMN "%s" CASCADE`, c.Table, c.Name)
}

func (c Column) SQL(quote bool) string {
	name := c.Name
	if quote {
		name = `"` + name + `"`
	}
	return fmt.Sprintf("%s %s", name, c.DataType)
}

func diffValues(oldValues []string, newValues []string) ([]string, []string) {
	add := make([]string, 0)
	remove := make([]string, 0)
	_current := make(map[string]bool)
	_new := make(map[string]bool)
	for _, v := range newValues {
		_new[v] = true
	}
	for _, v := range oldValues {
		_current[v] = true
	}
	for v := range _new {
		if !_current[v] {
			add = append(add, v)
		} else {
			delete(_current, v)
		}
	}
	for v := range _current {
		remove = append(remove, v)
	}
	sort.Strings(add)
	sort.Strings(remove)
	return add, remove
}

type Schema struct {
	Database    string
	Tables      map[string][]Column
	Indexes     map[string][]Index
	Constraints map[string][]Constraint
	TTLs        map[string]string
	Localities  map[string]string
	TableCounts map[string]int64
}

type IndexChange struct {
	Table      string
	Action     Action
	Index      Index
	Constraint *dm.Constraint
}

func (c IndexChange) CreateSQL() string {
	typeIndex := "INDEX"
	if c.Index.IsUnique() {
		typeIndex = "UNIQUE " + typeIndex
	}
	if c.Index.IsInverted() {
		typeIndex = "INVERTED " + typeIndex
	}
	var storing string
	var gin string
	if len(c.Index.Storing) > 0 {
		storing = fmt.Sprintf(" STORING (%s)", util.QuoteJoin(c.Index.Storing, `"`, ","))
	}
	if len(c.Index.Gin) > 0 {
		var op string
		if c.Index.OpClass != "" {
			op = " " + c.Index.OpClass
		}
		storing = fmt.Sprintf(" GIN (%s%s)", util.QuoteJoin(c.Index.Gin, `"`, ","), op)
	}
	return fmt.Sprintf(`CREATE %s "%s" ON "%s"(%s)%s%s`, typeIndex, c.Index.Name, c.Table, util.QuoteJoin(c.Index.Columns, `"`, ", "), storing, gin)
}

func (c IndexChange) DropSQL() string {
	return fmt.Sprintf(`DROP INDEX "%s"`, c.Index.Name)
}

type ModelChange struct {
	Table        string
	Action       Action
	Model        *dm.Model
	FieldChanges []FieldChange
	Destructive  bool
}

func (m ModelChange) SQL() string {
	var sql strings.Builder

	switch m.Action {
	case AddAction:
		sql.WriteString(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (`, m.Table) + "\n")
		pks := m.Model.PrimaryKey()
		for i, field := range m.Model.Fields {
			column := NewColumnFromField(m.Table, field)
			sql.WriteString(spacer + column.SQL(true))
			if i+1 < len(m.Model.Fields) || len(pks) > 0 {
				sql.WriteString(",\n")
			}
		}
		sql.WriteString("\n")
		if len(pks) > 0 {
			index := dm.GenerateIndexName(m.Model.Table, nil, "pkey")
			sql.WriteString(spacer + fmt.Sprintf(`CONSTRAINT "%s" PRIMARY KEY (%s));`, index, util.QuoteJoin(pks, `"`, ",")))
			sql.WriteString("\n")
		}
	case UpdateAction:
		for _, change := range m.FieldChanges {
			column := NewColumnFromField(m.Model.Table, change.Field)
			switch change.Action {
			case DeleteAction:
				sql.WriteString(column.DropSQL())
				sql.WriteString(";\n")
			case AddAction:
				sql.WriteString(fmt.Sprintf(`ALTER TABLE "%s" ADD COLUMN %s`, m.Model.Table, column.SQL(true)))
				sql.WriteString(";\n")
			case UpdateAction:
				if change.TypeChanged {
					sql.WriteString(column.AlterTypeSQL())
					sql.WriteString(";\n")

				}
			}
		}
	}

	return sql.String()
}

type FieldChange struct {
	Action          Action
	Name            string
	Field           *dm.Field
	Detail          string
	DefaultChanged  bool
	TypeChanged     bool
	OptionalChanged bool
}
