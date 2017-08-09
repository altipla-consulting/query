package query_test

import (
	. "github.com/altipla-consulting/query"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Select", func() {
	It("Should generate a simple select", func() {
		q := NewSelect("foo_table")

		sql, values, err := q.SQL()
		Expect(err).To(Succeed())
		Expect(sql).To(Equal("SELECT * FROM foo_table"))
		Expect(values).To(Equal([]interface{}{}))
	})

	It("Should project columns", func() {
		q := NewSelect("foo_table").Project("foo", "bar")

		sql, values, err := q.SQL()
		Expect(err).To(Succeed())
		Expect(sql).To(Equal("SELECT foo, bar FROM foo_table"))
		Expect(values).To(Equal([]interface{}{}))
	})

	It("Should filter a column", func() {
		q := NewSelect("foo_table").Filter("foo =", "bar")

		sql, values, err := q.SQL()
		Expect(err).To(Succeed())
		Expect(sql).To(Equal("SELECT * FROM foo_table WHERE foo = ?"))
		Expect(values).To(Equal([]interface{}{"bar"}))
	})

	It("Should filter multiple columns", func() {
		q := NewSelect("foo_table").Filter("foo =", "bar").Filter("baz =", "qux")

		sql, values, err := q.SQL()
		Expect(err).To(Succeed())
		Expect(sql).To(Equal("SELECT * FROM foo_table WHERE foo = ? AND baz = ?"))
		Expect(values).To(Equal([]interface{}{"bar", "qux"}))
	})

	It("Should add a between filter", func() {
		q := NewSelect("foo_table").Condition(Between("foo", "bar", "baz"))

		sql, values, err := q.SQL()
		Expect(err).To(Succeed())
		Expect(sql).To(Equal("SELECT * FROM foo_table WHERE foo BETWEEN ? AND ?"))
		Expect(values).To(Equal([]interface{}{"bar", "baz"}))
	})

	It("Should sort asc a single column", func() {
		q := NewSelect("foo_table").SortAsc("foo")

		sql, values, err := q.SQL()
		Expect(err).To(Succeed())
		Expect(sql).To(Equal("SELECT * FROM foo_table ORDER BY foo ASC"))
		Expect(values).To(Equal([]interface{}{}))
	})

	It("Should sort desc a single column", func() {
		q := NewSelect("foo_table").SortDesc("foo")

		sql, values, err := q.SQL()
		Expect(err).To(Succeed())
		Expect(sql).To(Equal("SELECT * FROM foo_table ORDER BY foo DESC"))
		Expect(values).To(Equal([]interface{}{}))
	})

	It("Should add multiple sorts", func() {
		q := NewSelect("foo_table").Order(Order{"foo", OrderDirectionDesc}, Order{"bar", OrderDirectionAsc})

		sql, values, err := q.SQL()
		Expect(err).To(Succeed())
		Expect(sql).To(Equal("SELECT * FROM foo_table ORDER BY foo DESC, bar ASC"))
		Expect(values).To(Equal([]interface{}{}))
	})

	It("Should limit", func() {
		q := NewSelect("foo_table").Limit(10)

		sql, values, err := q.SQL()
		Expect(err).To(Succeed())
		Expect(sql).To(Equal("SELECT * FROM foo_table LIMIT 0, 10"))
		Expect(values).To(Equal([]interface{}{}))
	})

	It("Should offset", func() {
		q := NewSelect("foo_table").Offset(40)

		sql, values, err := q.SQL()
		Expect(err).To(Succeed())
		Expect(sql).To(Equal("SELECT * FROM foo_table LIMIT 40, 18446744073709551615"))
		Expect(values).To(Equal([]interface{}{}))
	})

	It("Should offset and limit", func() {
		q := NewSelect("foo_table").Limit(10).Offset(40)

		sql, values, err := q.SQL()
		Expect(err).To(Succeed())
		Expect(sql).To(Equal("SELECT * FROM foo_table LIMIT 40, 10"))
		Expect(values).To(Equal([]interface{}{}))
	})

	It("Should generate multiple alternative conditions", func() {
		q := NewSelect("foo_table").Condition(LogicOr(Filter("foo =", "bar"), Filter("baz =", 3)))

		sql, values, err := q.SQL()
		Expect(err).To(Succeed())
		Expect(sql).To(Equal("SELECT * FROM foo_table WHERE (foo = ?) OR (baz = ?)"))
		Expect(values).To(Equal([]interface{}{"bar", 3}))
	})
})
