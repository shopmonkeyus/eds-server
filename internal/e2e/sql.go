//go:build e2e
// +build e2e

package e2e

import (
	"database/sql"
	"fmt"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

type sqlDriverTransform interface {
	QuoteTable(table string) string
	QuoteColumn(column string) string
	QuoteValue(value string) string
}

type columnFormat func(string) string

func validateSQLEvent(logger logger.Logger, event internal.DBChangeEvent, driver string, url string, format sqlDriverTransform) error {
	db, err := sql.Open(driver, url)
	if err != nil {
		return fmt.Errorf("error opening database: %w", err)
	}
	defer db.Close()
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s = %s", format.QuoteTable(event.Table), format.QuoteColumn("id"), format.QuoteValue(event.GetPrimaryKey()))
	logger.Info("running query: %s", query)
	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("error running query: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return fmt.Errorf("error scanning row: %w", err)
		}
		kv, err := event.GetObject()
		if err != nil {
			return fmt.Errorf("error getting object: %w", err)
		}
		if id != kv["id"] {
			return fmt.Errorf("id values do not match, was: %s, expected: %s", id, kv["id"])
		}
		if name != kv["name"] {
			return fmt.Errorf("name values do not match, was: %s, expected: %s", name, kv["name"])
		}
		logger.Info("event validated: %s", util.JSONStringify(event))
	}
	return nil
}
