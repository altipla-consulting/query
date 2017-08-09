package query_test

import (
	. "github.com/altipla-consulting/query"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Insert", func() {
	It("Should insert", func() {
		q := NewInsert("foo_table").
			Col("foo", "foo value").
			Col("bar", "bar value").
			Col("baz", 3)

		sql, values := q.SQL()
		Expect(sql).To(Equal("INSERT INTO foo_table(foo, bar, baz) VALUES (?, ?, ?)"))
		Expect(values).To(Equal([]interface{}{"foo value", "bar value", 3}))
	})
})
