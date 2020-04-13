package util

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/Tesla-SCA/ratchet/data"
	"github.com/Tesla-SCA/ratchet/logger"
)

// PostgreSQLInsertData abstracts building and executing a SQL INSERT
// statement for the given Data object.
//
// Note that the Data must be a valid JSON object
// (or an array of valid objects all with the same keys),
// where the keys are column names and the
// the values are SQL values to be inserted into those columns.
//
// If onDupKeyUpdate is true, you must set an onDupKeyIndex. This translates
// to the conflict_target as specified in https://www.postgresql.org/docs/9.5/static/sql-insert.html
func PostgreSQLInsertData(db *sql.DB, d data.JSON, tableName string, onDupKeyUpdate bool, onDupKeyIndex string, onDupKeyFields []string, batchSize int) error {
	objects, err := data.ObjectsFromJSON(d)
	if err != nil {
		return err
	}

	if batchSize > 0 {
		for i := 0; i < len(objects); i += batchSize {
			maxIndex := i + batchSize
			if maxIndex > len(objects) {
				maxIndex = len(objects)
			}
			err = postgresInsertObjects(db, objects[i:maxIndex], tableName, onDupKeyUpdate, onDupKeyIndex, onDupKeyFields)
			if err != nil {
				return err
			}
		}
		return nil
	}

	return postgresInsertObjects(db, objects, tableName, onDupKeyUpdate, onDupKeyIndex, onDupKeyFields)
}

func postgresInsertObjects(db *sql.DB, objects []map[string]interface{}, tableName string, onDupKeyUpdate bool, onDupKeyIndex string, onDupKeyFields []string) error {
	logger.Info("PostgreSQLInsertData: building INSERT for len(objects) =", len(objects))
	insertSQL := buildPostgreSQLInsertSQL(objects, tableName, onDupKeyUpdate, onDupKeyIndex, onDupKeyFields)

	logger.Info("PostgreSQLInsertData:", insertSQL)

	res, err := db.Exec(insertSQL)
	if err != nil {
		return err
	}

	rowCnt, err := res.RowsAffected()
	if err != nil {
		return err
	}

	logger.Info(fmt.Sprintf("PostgreSQLInsertData: rows affected = %d", rowCnt))
	return nil
}

func buildPostgreSQLInsertSQL(objects []map[string]interface{}, tableName string, onDupKeyUpdate bool, onDupKeyIndex string, onDupKeyFields []string) (insertSQL string) {
	cols := sortedColumns(objects)

	// Format: INSERT INTO tablename(col1,col2) VALUES(?,?),(?,?)
	insertSQL = fmt.Sprintf("INSERT INTO %v(%v) VALUES", tableName, strings.Join(cols, ","))

	for i := 0; i < len(objects); i++ {
		row := "("

		if i > 0 {
			insertSQL += ", "
		}

		objectVals := objects[i]

		for j := 0; j < len(cols); j++ {
			if j > 0 {
				row += ","
			}

			var toInsert string
			value, ok := objectVals[cols[j]]
			if ok {
				toInsert = fmt.Sprintf("%v", value)
			} else {
				toInsert = "NULL"
			}

			row += toInsert
		}

		row += ")"
		insertSQL += row
	}

	return
}
