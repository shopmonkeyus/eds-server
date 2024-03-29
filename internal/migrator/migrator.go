package migrator

import (
	"bufio"
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/mitchellh/colorstring"
	"github.com/schollz/progressbar/v3"
	dm "github.com/shopmonkeyus/eds-server/internal/model"
	"github.com/shopmonkeyus/eds-server/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

type MigrateOpts struct {
	DryRun     bool
	Format     string
	DBName     string
	NoProgress bool
	NoConfirm  bool
	Quiet      bool
	ShowSQL    bool
	SkipCreate bool
}

func loadTableSchema(logger logger.Logger, db *sql.DB, tableName string, tableSchema string, dialect util.Dialect) ([]Column, error) {
	if tableName == "" {

		return nil, errors.New("table name string is empty, name is required")
	}
	if tableSchema == "" {

		return nil, errors.New("table schema string is empty, schema is required")
	}

	started := time.Now()
	query := buildTableQuerySchemaString(dialect)
	rows, err := db.Query(query, tableSchema, tableName)
	if err != nil {
		if strings.Contains(err.Error(), "does not exist") {
			// this means there is not a table made and we need to build one...
			return []Column{}, nil
		}
		return nil, fmt.Errorf("error fetching column metadata from the db: %w", err)
	}
	defer rows.Close()

	var table string
	var columns []Column
	for rows.Next() {
		var tn string
		var cn string
		var cd sql.NullString
		var isn string
		var dt string
		var cml sql.NullString

		if err := rows.Scan(&tn, &cn, &cd, &isn, &dt, &cml); err != nil {
			return nil, fmt.Errorf("error reading db row: %w", err)
		}
		if table != tn {
			columns = make([]Column, 0)
			table = tn
		}
		var colDef, maxlength, udtName *string
		if cd.Valid {
			colDef = &cd.String
		}
		if cml.Valid {
			maxlength = &cml.String
		}

		columns = append(columns, Column{
			Table:               table,
			Name:                cn,
			Default:             colDef,
			IsNullable:          isn == "YES",
			DataType:            dt,
			MaxLength:           maxlength,
			UserDefinedTypeName: udtName,
		})
	}
	logger.Trace("loaded up schema in %v", time.Since(started))
	return columns, nil
}

func buildTableQuerySchemaString(dialect util.Dialect) string {
	table_schema_placeholder := "?"
	table_name_placeholder := "?"
	switch dialect {
	case util.Sqlserver:
		table_schema_placeholder = "@p1"
		table_name_placeholder = "@p2"
	case util.Postgresql:
		table_schema_placeholder = "$1"
		table_name_placeholder = "$2"
	case util.Snowflake:
		table_schema_placeholder = "?"
		table_name_placeholder = "?"
	}
	query := `SELECT
	c.table_name,
	c.column_name,
	c.column_default,
	c.is_nullable,
	c.data_type,
	c.character_maximum_length
	FROM
	information_schema.columns c
	WHERE
	c.table_schema = ` + table_schema_placeholder + ` AND
	c.table_name = ` + table_name_placeholder + ` ORDER BY
	c.table_name, c.ordinal_position;`
	return query
}

type sqlWriter struct {
	sql     []string
	buf     bytes.Buffer
	showsql bool
}

var _ io.Writer = (*sqlWriter)(nil)

func (w *sqlWriter) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

var traceSQL bool
var multiSpaceRegexp = regexp.MustCompile(`\s{2,}`)

func (w *sqlWriter) runSQL(pb *progressbar.ProgressBar, logger logger.Logger, db *sql.DB, sql string, offset int, total int) error {
	if sql == "" || sql == ";" || sql == "\n" {
		return nil
	}
	started := time.Now()
	if _, err := db.Exec(sql); err != nil {
		if pb != nil {
			pb.Clear()
			pb.Close()
		}
		logger.Error("error executing: %s. %s", sql, err)
		return err
	}
	msg := strings.TrimSpace(strings.ReplaceAll(sql, "\n", " "))
	smsg := multiSpaceRegexp.ReplaceAllString(msg, " ")
	if len(smsg) > 70 {
		smsg = strings.TrimSpace(smsg[0:70])
	}

	if pb != nil {
		pb.Describe(fmt.Sprintf("[magenta][%d/%d][reset] %s", offset, total, smsg))
	}
	if traceSQL {
		logger.Trace("executed: %s, took: %v", msg, time.Since(started))
	}
	if w.showsql {
		colorstring.Fprintf(os.Stderr, "[magenta][%3d/%3d] [light_cyan]%-72s[light_green]%v[reset]\n", offset, total, smsg, time.Since(started).Round(time.Millisecond))
	}
	return nil
}

func (w *sqlWriter) run(logger logger.Logger, db *sql.DB) error {
	for _, buf := range strings.Split(w.buf.String(), ";") {
		sql := strings.TrimSpace(strings.ReplaceAll(buf, "\n", " "))
		if sql != "" {
			w.sql = append(w.sql, sql)
		}
	}

	total := len(w.sql)

	var bar *progressbar.ProgressBar

	var offset int
	for _, sql := range w.sql {
		if err := w.runSQL(bar, logger, db, sql, offset, total); err != nil {
			return err
		}
		offset++
		if bar != nil {
			bar.Add(1)
		}
	}

	return nil
}

// Migrate will run migration using model against db
func MigrateTable(logger logger.Logger, db *sql.DB, datamodel *dm.Model, tableName string, tableSchema string, dialect util.Dialect) error {

	schema, err := loadTableSchema(logger, db, tableName, tableSchema, dialect)
	if err != nil {
		return err
	}
	//Convert schema column dataTypes to sql dialect-specific dataTypes
	//TODO: Find where schemas are loaded into DB and convert them before loading
	stdout := bufio.NewWriter(os.Stdout)

	var output sqlWriter
	output.showsql = true

	// model diff
	_, modelDiff, err := diffModels(schema, datamodel, dialect)
	if err != nil {
		return err
	}
	if modelDiff == nil {
		logger.Error("ran into an error with figuring out change when finding model diffs")
	}
	newTables := make(map[string]bool)

	change := modelDiff
	if change.Action == AddAction {
		newTables[tableName] = true
	}

	change.Format(tableName, "sql", &output, dialect)

	stdout.Flush()

	started := time.Now()
	logger.Trace("running migrations ...")
	if err := output.run(logger, db); err != nil {
		return err
	}
	logger.Info("executed %d sql statements in %v", len(output.sql), time.Since(started))
	return nil
}
