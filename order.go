package query

import (
	"fmt"
)

type Order struct {
	Column    string
	Direction OrderDirection
}

func (order Order) SQL() string {
	return fmt.Sprintf("%s %s", order.Column, order.Direction)
}

type OrderDirection string

const (
	OrderDirectionDesc = OrderDirection("DESC")
	OrderDirectionAsc  = OrderDirection("ASC")
)
