package sqlserver

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/util"
)

var needsQuote = regexp.MustCompile(`[A-Z0-9_\s]`)
var keywords = regexp.MustCompile(`(?i)\b(USER|SELECT|INSERT|UPDATE|DELETE|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|GROUP BY|ORDER BY|HAVING|AND|OR|CREATE|DROP|ALTER|TABLE|INDEX|ON|INTO|VALUES|SET|AS|DISTINCT|TYPE|DEFAULT|ORDER|GROUP|LIMIT|SUM|TOTAL|START|END|BEGIN|COMMIT|ROLLBACK|PRIMARY|PERCENT|AUTHORIZATION)\b`)

func quoteIdentifier(val string) string {
	if needsQuote.MatchString(val) || keywords.MatchString(val) {
		return `"` + val + `"`
	}
	return val
}

func toSQLFromObject(operation string, model *internal.Schema, table string, o map[string]any, diff []string) string {
	var sql strings.Builder

	var insertVals []string
	var updateValues []string
	jsonb := util.ToMapOfJSONColumns(model)
	if operation == "UPDATE" {
		sql.WriteString("UPDATE ")
		sql.WriteString(quoteIdentifier(table))
		sql.WriteString(" SET ")
		for _, name := range diff {
			if !util.SliceContains(model.Columns, name) || name == "id" {
				continue
			}
			if val, ok := o[name]; ok {
				v := util.ToJSONStringVal(name, quoteValue(val), jsonb)
				updateValues = append(updateValues, fmt.Sprintf("%s=%s", quoteIdentifier(name), v))
			} else {
				updateValues = append(updateValues, "NULL")
			}
		}
		sql.WriteString(strings.Join(updateValues, ","))
		sql.WriteString(" WHERE id=")
		sql.WriteString(quoteValue(o["id"]))
		sql.WriteString(";\n")

	} else {
		sql.WriteString("INSERT INTO ")

		sql.WriteString(quoteIdentifier(table))
		var columns []string
		for _, name := range model.Columns {
			columns = append(columns, quoteIdentifier(name))
		}
		sql.WriteString(" (")
		sql.WriteString(strings.Join(columns, ","))
		sql.WriteString(") VALUES (")
		for _, name := range model.Columns {

			if val, ok := o[name]; ok {
				v := util.ToJSONStringVal(name, quoteValue(val), jsonb)
				if name != "id" {

					v = handleSchemaProperty(model.Properties[name], v)
					updateValues = append(updateValues, fmt.Sprintf("%s=%s", quoteIdentifier(name), v))
				}
				insertVals = append(insertVals, v)
			} else {
				v := handleSchemaProperty(model.Properties[name], "NULL")
				updateValues = append(updateValues, v)
				insertVals = append(insertVals, v)
			}
		}
		sql.WriteString(strings.Join(insertVals, ","))
		sql.WriteString(");\n")
	}

	return sql.String()
}

func toSQL(c internal.DBChangeEvent, schema internal.SchemaMap) (string, error) {
	model := schema[c.Table]
	primaryKeys := model.PrimaryKeys
	if c.Operation == "DELETE" {
		var sql strings.Builder
		sql.WriteString("DELETE FROM ")
		sql.WriteString(quoteIdentifier(c.Table))
		sql.WriteString(" WHERE ")
		var predicate []string
		for i, pk := range primaryKeys {
			predicate = append(predicate, fmt.Sprintf("%s=%s", quoteIdentifier(pk), quoteValue(c.Key[1+i])))
		}
		sql.WriteString(strings.Join(predicate, " AND "))
		sql.WriteString(";\n")
		return sql.String(), nil
	} else {
		o := make(map[string]any)
		if err := json.Unmarshal(c.After, &o); err != nil {
			return "", err
		}
		return toSQLFromObject(c.Operation, model, c.Table, o, c.Diff), nil
	}
}

func propTypeToSQLType(property internal.SchemaProperty, isPrimaryKey bool) string {
	switch property.Type {
	case "string":
		if isPrimaryKey {
			return "VARCHAR(64)"
		}
		if property.Format == "date-time" {
			return "NVARCHAR(MAX)"
		}
		return "NVARCHAR(MAX)"
	case "integer":
		return "BIGINT"
	case "number":
		return "FLOAT"
	case "boolean":
		return "BIT"
	case "object":
		return "NVARCHAR(MAX)" // for JSON
	case "array":
		if property.Items != nil && property.Items.Enum != nil {
			return "VARCHAR(64)" // this is an enum but we want to represent it as a string
		}
		return "NVARCHAR(MAX)" // for JSON
	default:
		return "NVARCHAR(MAX)"
	}
}

func handleSchemaProperty(prop internal.SchemaProperty, v string) string {
	switch prop.Type {
	case "object":
		if prop.AdditionalProperties != nil && *prop.AdditionalProperties {
			return v
		}
	case "boolean":
		if strings.ToLower(v) == "true" || v == "1" {
			return "1"
		}
		if !prop.Nullable && v == "" || strings.ToLower(v) == "false" || strings.ToLower(v) == "null" {
			return "0"

		}
	case "integer":
		if v == "NULL" {
			return "0"
		}
	case "array":
		//Arrays are stored as varchar
		if !prop.Nullable && v == "NULL" {
			return "''"
		}
	default:
		return v
	}
	return v
}

func createSQL(s *internal.Schema) string {
	var sql strings.Builder
	sql.WriteString("DROP TABLE IF EXISTS ")
	sql.WriteString(quoteIdentifier((s.Table)))
	sql.WriteString(";\n")
	sql.WriteString("CREATE TABLE ")
	sql.WriteString(quoteIdentifier((s.Table)))
	sql.WriteString(" (\n")
	var columns []string
	for _, name := range s.Columns {
		if util.SliceContains(s.PrimaryKeys, name) {
			continue
		}
		columns = append(columns, name)
	}
	sort.Strings(columns)
	columns = append(s.PrimaryKeys, columns...)
	for _, name := range columns {
		prop := s.Properties[name]
		sql.WriteString("\t")
		sql.WriteString(quoteIdentifier(name))
		sql.WriteString(" ")
		sql.WriteString(propTypeToSQLType(prop, util.SliceContains(s.PrimaryKeys, name)))
		if util.SliceContains(s.Required, name) && !prop.Nullable {
			sql.WriteString(" NOT NULL")
		}
		sql.WriteString(",\n")
	}
	if len(s.PrimaryKeys) > 0 {
		sql.WriteString("\tPRIMARY KEY (")
		for i, pk := range s.PrimaryKeys {
			sql.WriteString(quoteIdentifier(pk))
			if i < len(s.PrimaryKeys)-1 {
				sql.WriteString(", ")
			}
		}
		sql.WriteString(")")
	}
	sql.WriteString("\n)")

	return sql.String()
}

func parseURLToDSN(urlstr string) (string, error) {
	// Example input: "sqlserver://sa:eds@localhost:11433/eds"
	// Desired output: "sqlserver://sa:eds@localhost:11433/database=eds?multiStatements=true"
	u, err := url.Parse(urlstr)
	if err != nil {
		return "", fmt.Errorf("error parsing url: %w", err)
	}
	vals := u.Query()
	vals.Set("multiStatements", "true")

	// Start building the DSN string
	var dsn strings.Builder
	dsn.WriteString("sqlserver") // Add the scheme (e.g., "sqlserver")
	dsn.WriteString("://")

	if u.User != nil {
		dsn.WriteString(u.User.String())
		dsn.WriteString("@")
	}

	dsn.WriteString(u.Host)

	if u.Path != "" {
		dsn.WriteString(u.Path)
	}

	if encoded := vals.Encode(); encoded != "" {
		dsn.WriteString("?")
		dsn.WriteString(encoded)
	}

	return dsn.String(), nil
}
