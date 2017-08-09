package query

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
)

type Condition interface {
	SQL() (string, []interface{}, error)
}

type filterImpl struct {
	column string
	value  interface{}
}

func Filter(column string, value interface{}) *filterImpl {
	return &filterImpl{column, value}
}

func (impl *filterImpl) SQL() (string, []interface{}, error) {
	return fmt.Sprintf("%s ?", impl.column), []interface{}{impl.value}, nil
}

type betweenImpl struct {
	column      string
	valueBefore interface{}
	valueAfter  interface{}
}

func Between(column string, valueBefore, valueAfter interface{}) *betweenImpl {
	return &betweenImpl{column, valueBefore, valueAfter}
}

func (impl *betweenImpl) SQL() (string, []interface{}, error) {
	return fmt.Sprintf("%s BETWEEN ? AND ?", impl.column), []interface{}{impl.valueBefore, impl.valueAfter}, nil
}

type logicOrImpl struct {
	conditions []Condition
}

func LogicOr(conditions ...Condition) *logicOrImpl {
	return &logicOrImpl{conditions}
}

func (impl *logicOrImpl) SQL() (string, []interface{}, error) {
	sql := []string{}
	values := []interface{}{}
	for _, cond := range impl.conditions {
		sqlCond, valuesCond, err := cond.SQL()
		if err != nil {
			return "", nil, errors.Trace(err)
		}
		sql = append(sql, fmt.Sprintf("(%s)", sqlCond))
		values = append(values, valuesCond...)
	}

	return strings.Join(sql, " OR "), values, nil
}
