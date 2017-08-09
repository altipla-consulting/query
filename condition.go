package query

import (
	"fmt"
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
