package processors

import (
	"database/sql"
	"errors"

	"github.com/Tesla-SCA/ratchet/data"
	"github.com/Tesla-SCA/ratchet/logger"
	"github.com/Tesla-SCA/ratchet/util"
)

// SQLPassthroughExecutor runs the given SQL AND passes original data through to the next stage.
//
// It can operate in 1 mode:
// 1) Static - runs the given SQL query and ignores any received data.
type SQLPassthroughExecutor struct {
	readDB       	*sql.DB
	query        	string
	timesMaxRun  	int
	timesRun	 	int
}

// NewSQLPassthroughExecutor returns a new SQLPassthroughExecutor
func NewSQLPassthroughExecutor(dbConn *sql.DB, sql string, timesMaxRun int) *SQLPassthroughExecutor {
	return &SQLPassthroughExecutor{readDB: dbConn, query: sql, timesMaxRun: timesMaxRun, timesRun: 0}
}

// ProcessData runs the SQL statements, deferring to util.ExecuteSQLQuery
func (s *SQLPassthroughExecutor) ProcessData(d data.JSON, outputChan chan data.JSON, killChan chan error) {
	// handle panics a bit more gracefully
	defer func() {
		if err := recover(); err != nil {
			util.KillPipelineIfErr(err.(error), killChan)
		}
	}()

	if s.timesRun >= s.timesMaxRun {
		outputChan <- d
		return
	}
	s.timesRun += 1

	sql := ""
	var err error
	if s.query != "" {
		sql = s.query
	} else {
		killChan <- errors.New("SQLPassthroughExecutor: must have static query")
	}

	logger.Debug("SQLPassthroughExecutor: Running - ", sql)
	// See sql.go
	err = util.ExecuteSQLQuery(s.readDB, sql)
	util.KillPipelineIfErr(err, killChan)
	logger.Info("SQLPassthroughExecutor: Query complete")

	logger.Info("SQLPassthroughExecutor: Allowing data to pass through after valid query")
	outputChan <- d
}

// Finish - see interface for documentation.
func (s *SQLPassthroughExecutor) Finish(outputChan chan data.JSON, killChan chan error) {
}

func (s *SQLPassthroughExecutor) String() string {
	return "SQLPassthroughExecutor"
}
