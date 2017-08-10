package query

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
)

type Select struct {
	table         string
	columns       []string
	conditions    []Condition
	orders        []Order
	limit, offset int64
}

func NewSelect(table string) *Select {
	return &Select{table: table}
}

func (q *Select) Clone() *Select {
	return &Select{
		table:      q.table,
		columns:    q.columns,
		conditions: q.conditions,
		orders:     q.orders,
		limit:      q.limit,
		offset:     q.offset,
	}
}

func (q *Select) Project(columns ...string) *Select {
	result := q.Clone()
	result.columns = columns
	return result
}

func (q *Select) Filter(column string, value interface{}) *Select {
	return q.Condition(Filter(column, value))
}

func (q *Select) Condition(condition Condition) *Select {
	result := q.Clone()
	result.conditions = append(result.conditions, condition)
	return result
}

func (q *Select) SortAsc(column string) *Select {
	return q.Order(Order{Column: column, Direction: OrderDirectionAsc})
}

func (q *Select) SortDesc(column string) *Select {
	return q.Order(Order{Column: column, Direction: OrderDirectionDesc})
}

func (q *Select) Order(orders ...Order) *Select {
	result := q.Clone()
	result.orders = orders
	return result
}

func (q *Select) Limit(limit int64) *Select {
	result := q.Clone()
	result.limit = limit
	return result
}

func (q *Select) Offset(offset int64) *Select {
	result := q.Clone()
	result.offset = offset
	return result
}

func (q *Select) SQL() (string, []interface{}, error) {
	sqlCols := "*"
	if len(q.columns) > 0 {
		sqlCols = strings.Join(q.columns, ", ")
	}

	var sqlConds string
	values := []interface{}{}
	if len(q.conditions) > 0 {
		conds := []string{}
		for _, cond := range q.conditions {
			sql, condValues, err := cond.SQL()
			if err != nil {
				return "", nil, errors.Trace(err)
			}

			conds = append(conds, sql)
			values = append(values, condValues...)
		}
		sqlConds = fmt.Sprintf(" WHERE %s", strings.Join(conds, " AND "))
	}

	var sqlOrder string
	if len(q.orders) > 0 {
		orders := []string{}
		for _, order := range q.orders {
			orders = append(orders, order.SQL())
		}
		sqlOrder = fmt.Sprintf(" ORDER BY %s", strings.Join(orders, ", "))
	}

	var sqlLimit string
	if q.limit > 0 {
		sqlLimit = fmt.Sprintf(" LIMIT %d, %d", q.offset, q.limit)
	} else if q.offset > 0 {
		sqlLimit = fmt.Sprintf(" LIMIT %d, 18446744073709551615", q.offset)
	}

	return fmt.Sprintf("SELECT %s FROM %s%s%s%s", sqlCols, q.table, sqlConds, sqlOrder, sqlLimit), values, nil
}

func (q *Select) GetAll(db *sql.DB, models interface{}) error {
	modelsType := reflect.TypeOf(models)
	modelsValue := reflect.ValueOf(models)
	if modelsType.Kind() != reflect.Ptr || modelsType.Elem().Kind() != reflect.Slice {
		return errors.New("models should be a pointer to a slice")
	}

	sql, values, err := q.SQL()
	if err != nil {
		return errors.Trace(err)
	}

	rows, err := db.Query(sql, values...)
	if err != nil {
		return errors.Annotate(err, sql)
	}
	defer rows.Close()

	sliceElemType := modelsType.Elem().Elem()
	elemType := sliceElemType.Elem()
	for rows.Next() {
		elemValue := reflect.New(elemType)
		if err := sqlx.StructScan(rows, elemValue.Interface()); err != nil {
			return errors.Trace(err)
		}
		modelsValue.Set(reflect.Append(modelsValue, elemValue))
	}

	return nil
}

func (q *Select) Count(db *sql.DB) (int64, error) {
	q = q.Project("COUNT(*)")
	sql, values, err := q.SQL()
	if err != nil {
		return 0, errors.Trace(err)
	}

	var n int64
	if err := db.QueryRow(sql, values...).Scan(&n); err != nil {
		return 0, errors.Trace(err)
	}

	return n, nil
}

func (q *Select) IsOrdered() bool {
	return len(q.orders) > 0
}
