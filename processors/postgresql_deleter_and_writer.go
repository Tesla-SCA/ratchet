package processors

import (
	"database/sql"

	"github.com/Tesla-SCA/ratchet/data"
	"github.com/Tesla-SCA/ratchet/logger"
	"github.com/Tesla-SCA/ratchet/util"
)

// PostgreSQLDeleterWriter handles DELETING data and then INSERTing data.JSON into a
// specified SQL table. If an error occurs while deleting
// or executing the INSERT, the error will be sent to the killChan.
//
// deleteSql must be a string to just be executed.
//
// Note that the data.JSON must be a valid JSON object or a slice
// of valid objects, where the keys are column names and the
// the values are the SQL values to be inserted into those columns.
//
// For use-cases where a PostgreSQLDeleterWriter instance needs to write to
// multiple tables you can pass in SQLWriterData.
//
// Note that if `OnDupKeyUpdate` is true (the default), you *must*
// provide a value for `OnDupKeyIndex` (which is the PostgreSQL
// conflict target).
type PostgreSQLDeleterWriter struct {
	writeDB          *sql.DB
	deleteSql 		 string
	TableName        string
	OnDupKeyUpdate   bool
	OnDupKeyIndex    string // The conflict target: see https://www.postgresql.org/docs/9.5/static/sql-insert.html
	OnDupKeyFields   []string
	ConcurrencyLevel int // See ConcurrentDataProcessor
	BatchSize        int
	timesRun		 int
}

// NewPostgreSQLDeleterWriter returns a new PostgreSQLDeleterWriter
func NewPostgreSQLDeleterWriter(db *sql.DB, tableName string, deleteSql string) *PostgreSQLDeleterWriter {
	return &PostgreSQLDeleterWriter{
		writeDB: db,
		TableName: tableName,
		OnDupKeyUpdate: true,
		deleteSql: deleteSql,
	}
}

// ProcessData defers to util.PostgreSQLInsertData
func (s *PostgreSQLDeleterWriter) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	// handle panics a bit more gracefully
	defer func() {
		if err := recover(); err != nil {
			util.KillPipelineIfErr(err.(error), killChan)
		}
	}()

	tx, err := s.writeDB.Begin()
	util.KillPipelineIfErr(err, killChan)

	if s.timesRun == 0 {
		_, err := tx.Exec(s.deleteSql)
		util.KillPipelineIfErr(err, killChan)
	}
	s.timesRun += 1

	// First check for SQLWriterData
	var wd SQLWriterData
	err = data.ParseJSONSilent(d, &wd)
	logger.Info("PostgreSQLDeleterWriter: Writing data...")
	if err == nil && wd.TableName != "" && wd.InsertData != nil {
		logger.Debug("PostgreSQLDeleterWriter: SQLWriterData scenario")
		dd, err := data.NewJSON(wd.InsertData)
		util.KillPipelineIfErr(err, killChan)
		err = util.PostgreSQLTxInsertData(tx, dd, wd.TableName, s.OnDupKeyUpdate, s.OnDupKeyIndex, s.OnDupKeyFields, s.BatchSize)
		util.KillPipelineIfErr(err, killChan)
	} else {
		logger.Debug("PostgreSQLDeleterWriter: normal data scenario")
		err = util.PostgreSQLTxInsertData(tx, d, s.TableName, s.OnDupKeyUpdate, s.OnDupKeyIndex, s.OnDupKeyFields, s.BatchSize)
		util.KillPipelineIfErr(err, killChan)
	}
	err = tx.Commit()
	util.KillPipelineIfErr(err, killChan)

	logger.Info("PostgreSQLDeleterWriter: Write complete")
}

// Finish - see interface for documentation.
func (s *PostgreSQLDeleterWriter) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (s *PostgreSQLDeleterWriter) String() string {
	return "PostgreSQLDeleterWriter"
}

// Concurrency defers to ConcurrentDataProcessor
func (s *PostgreSQLDeleterWriter) Concurrency() int {
	return s.ConcurrencyLevel
}
