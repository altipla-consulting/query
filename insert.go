package query

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/juju/errors"
)

type Insert struct {
	table   string
	columns []string
	values  []interface{}
}

func NewInsert(table string) *Insert {
	return &Insert{table: table}
}

func (q *Insert) Clone() *Insert {
	return &Insert{
		table:   q.table,
		columns: q.columns,
		values:  q.values,
	}
}

func (q *Insert) Col(column string, value interface{}) *Insert {
	result := q.Clone()
	result.columns = append(result.columns, column)
	result.values = append(result.values, value)
	return result
}

func (q *Insert) SQL() (string, []interface{}) {
	cols := strings.Join(q.columns, ", ")
	vals := strings.Repeat("?, ", len(q.columns)-1)
	return fmt.Sprintf("INSERT INTO %s(%s) VALUES (%s?)", q.table, cols, vals), q.values
}

func (q *Insert) Exec(db *sql.DB) (sql.Result, error) {
	sql, values := q.SQL()
	result, err := db.Exec(sql, values...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return result, nil
}
