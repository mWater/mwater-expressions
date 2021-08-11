// TODO: This file was created by bulk-decaffeinate.
// Sanity-check the conversion and remove this comment.
import { assert } from "chai"
import _ from "lodash"
import { default as Schema } from "../src/Schema"
import { default as ExprCleaner } from "../src/ExprCleaner"
import * as fixtures from "./fixtures"
import canonical from "canonical-json"

function compare(actual: any, expected: any) {
  return assert.equal(
    canonical(actual),
    canonical(expected),
    "\ngot: " + canonical(actual) + "\nexp: " + canonical(expected) + "\n"
  )
}

const variables = [
  {
    id: "varenum",
    name: { _base: "en", en: "Varenum" },
    type: "enum",
    enumValues: [
      { id: "a", name: { en: "A" } },
      { id: "b", name: { en: "B" } }
    ]
  },
  { id: "varnumber", name: { _base: "en", en: "Varnumber" }, type: "number" },
  { id: "varnumberexpr", name: { _base: "en", en: "Varnumberexpr" }, type: "number", table: "t1" },
  { id: "varid", name: { _base: "en", en: "Varid" }, type: "id", idTable: "t1" }
]

describe("ExprCleaner", function () {
  beforeEach(function () {
    this.schema = fixtures.simpleSchema()
    this.exprCleaner = new ExprCleaner(this.schema, variables)
    return this.clean = (expr: any, expected: any, options: any) => {
      return compare(this.exprCleaner.cleanExpr(expr, options), expected)
    };
  })

  describe("cleanExpr", function () {
    it("nulls if wrong table", function () {
      return assert.isNull(this.exprCleaner.cleanExpr({ type: "field", table: "t1", column: "text" }, { table: "t2" }))
    })

    it("nulls if wrong type", function () {
      const field = { type: "field", table: "t1", column: "enum" }
      return assert.isNull(this.exprCleaner.cleanExpr(field, { types: ["text"] }))
    })

    it("nulls if wrong idTable", function () {
      const field = { type: "id", table: "t1" }
      assert(this.exprCleaner.cleanExpr(field, { types: ["id"], idTable: "t1" }))
      return assert.isNull(this.exprCleaner.cleanExpr(field, { types: ["id"], idTable: "t2" }))
    })

    it("nulls if wrong enums", function () {
      const field = { type: "field", table: "t1", column: "enum" }
      assert.isNotNull(this.exprCleaner.cleanExpr(field, { enumValueIds: ["a", "b", "c"] }))
      return assert.isNull(this.exprCleaner.cleanExpr(field, { enumValueIds: ["a"] }))
    })

    it("nulls if wrong enums expression", function () {
      const field = { type: "field", table: "t1", column: "expr_enum" }
      assert.isNotNull(this.exprCleaner.cleanExpr(field, { enumValueIds: ["a", "b", "c"] }))
      return assert.isNull(this.exprCleaner.cleanExpr(field, { enumValueIds: ["a"] }))
    })

    it("nulls if missing variable", function () {
      assert.isNotNull(this.exprCleaner.cleanExpr({ type: "variable", variableId: "varnumber" }, { table: "t2" }))
      return assert.isNull(this.exprCleaner.cleanExpr({ type: "variable", variableId: "varxyz" }, { table: "t2" }))
    })

    it("allows variable if right id table", function () {
      assert.isNotNull(
        this.exprCleaner.cleanExpr({ type: "variable", variableId: "varid" }, { table: "t2", idTable: "t1" })
      )
      return assert.isNull(
        this.exprCleaner.cleanExpr({ type: "variable", variableId: "varid" }, { table: "t2", idTable: "t2" })
      )
    })

    it("nulls recursive field expr", function () {
      const table = this.schema.getTable("t1")
      table.contents.push({
        id: "expr_recursive",
        name: { en: "Expr Recursive" },
        type: "expr",
        expr: { type: "field", table: "t1", column: "expr_recursive" }
      })
      const schema = this.schema.addTable(table)
      const exprCleaner = new ExprCleaner(schema)

      return assert.isNull(exprCleaner.cleanExpr({ type: "field", table: "t1", column: "expr_recursive" }))
    })

    it("nulls if expr is invalid", function () {
      const table = this.schema.getTable("t1")
      table.contents.push(
        {
          id: "expr_invalid",
          name: { en: "Expr Invalid" },
          type: "expr",
          expr: { type: "field", table: "t1", column: "nonsuch" }
        },
        {
          id: "expr_valid",
          name: { en: "Expr Invalid" },
          type: "expr",
          expr: { type: "field", table: "t1", column: "enum" }
        }
      )
      const schema = this.schema.addTable(table)
      const exprCleaner = new ExprCleaner(schema)

      assert.isNull(exprCleaner.cleanExpr({ type: "field", table: "t1", column: "expr_invalid" }))
      return assert(exprCleaner.cleanExpr({ type: "field", table: "t1", column: "expr_valid" }))
    })

    it("cleans aggregate op", function () {
      const expr = {
        type: "op",
        table: "t1",
        op: "and",
        exprs: [
          { type: "op", op: "=", table: "t1", exprs: [{ type: "op", table: "t1", op: "count", exprs: [] }, null] },
          null
        ]
      }
      return compare(
        this.exprCleaner.cleanExpr(expr, { aggrStatuses: ["aggregate", "individual", "literal"], types: ["boolean"] }),
        expr
      )
    })

    describe("aggregation", function () {
      it("aggregates if required", function () {
        const field = { type: "field", table: "t1", column: "number" }
        return compare(this.exprCleaner.cleanExpr(field, { aggrStatuses: ["aggregate"] }), {
          type: "op",
          op: "last",
          table: "t1",
          exprs: [field]
        })
      })

      it("nulls if aggregate and should not be", function () {
        const field = { type: "field", table: "t1", column: "number" }
        assert(this.exprCleaner.cleanExpr(field, { aggrStatuses: ["individual"] }), "is individual")
        assert.isNull(this.exprCleaner.cleanExpr(field, { aggrStatuses: ["literal"] }))

        const aggr = { type: "op", table: "t1", op: "sum", exprs: [field] }
        assert.isNull(this.exprCleaner.cleanExpr(aggr), "is aggregate")
        assert.isNull(this.exprCleaner.cleanExpr(aggr, { aggrStatuses: ["literal"] }))
        return assert(this.exprCleaner.cleanExpr(aggr, { aggrStatuses: ["aggregate"] }), "should allow aggregate")
      })

      it("nulls inner expr if wrong aggregation status", function () {
        const field = { type: "field", table: "t1", column: "number" }
        const expr = { type: "op", table: "t1", op: "sum", exprs: [{ type: "op", op: "sum", exprs: [field] }] }

        return compare(this.exprCleaner.cleanExpr(expr, { aggrStatuses: ["aggregate"] }), {
          type: "op",
          op: "sum",
          table: "t1",
          exprs: [null]
        })
      })

      it("passes types through aggregation", function () {
        const field = { type: "field", table: "t1", column: "number" }
        const expr = { type: "op", table: "t1", op: "sum", exprs: [field] }

        compare(this.exprCleaner.cleanExpr(expr, { types: ["number"], aggrStatuses: ["aggregate"] }), expr)
        return compare(this.exprCleaner.cleanExpr(expr, { types: ["text"], aggrStatuses: ["aggregate"] }), null)
      })

      return it("allows no args for count", function () {
        const field = { type: "field", table: "t1", column: "number" }
        const expr = { type: "op", table: "t1", op: "count", exprs: [field] }

        return compare(this.exprCleaner.cleanExpr(expr, { types: ["number"], aggrStatuses: ["aggregate"] }), {
          type: "op",
          table: "t1",
          op: "count",
          exprs: []
        })
      })
    })

    describe("fixing expression types", function () {
      it("creates boolean from enum", function () {
        const expr = { type: "field", table: "t1", column: "enum" }
        return compare(this.exprCleaner.cleanExpr(expr, { types: ["boolean"] }), {
          type: "op",
          table: "t1",
          op: "= any",
          exprs: [expr, null]
        })
      })

      it("creates percent where from enum", function () {
        const expr = { type: "field", table: "t1", column: "enum" }
        return compare(
          this.exprCleaner.cleanExpr(expr, { types: ["number"], aggrStatuses: ["aggregate", "literal"] }),
          {
            type: "op",
            op: "percent where",
            table: "t1",
            exprs: [{ type: "op", table: "t1", op: "= any", exprs: [expr, null] }, null]
          }
        )
      })

      return it("creates boolean inside percent where from enum", function () {
        const expr = {
          type: "op",
          table: "t1",
          op: "percent where",
          exprs: [{ type: "field", table: "t1", column: "enum" }]
        }
        return compare(
          this.exprCleaner.cleanExpr(expr, { types: ["number"], aggrStatuses: ["aggregate", "literal"] }),
          {
            type: "op",
            op: "percent where",
            table: "t1",
            exprs: [
              { type: "op", table: "t1", op: "= any", exprs: [{ type: "field", table: "t1", column: "enum" }, null] },
              null
            ]
          }
        )
      })
    })

    describe("op", function () {
      it("preserves 'and' by cleaning child expressions with boolean type", function () {
        const expr = {
          type: "op",
          op: "and",
          table: "t1",
          exprs: [
            { type: "field", table: "t1", column: "enum" },
            { type: "field", table: "t1", column: "boolean" }
          ]
        }

        return compare(this.exprCleaner.cleanExpr(expr), {
          type: "op",
          op: "and",
          table: "t1",
          exprs: [
            // Booleanized
            { type: "op", table: "t1", op: "= any", exprs: [{ type: "field", table: "t1", column: "enum" }, null] },
            // Untouched
            { type: "field", table: "t1", column: "boolean" }
          ]
        })
      })

      it("simplifies and", function () {
        let expr = { type: "op", op: "and", table: "t1", exprs: [{ type: "field", table: "t1", column: "boolean" }] }
        compare(this.exprCleaner.cleanExpr(expr), { type: "field", table: "t1", column: "boolean" })

        expr = { type: "op", op: "and", table: "t1", exprs: [] }
        return compare(this.exprCleaner.cleanExpr(expr), null)
      })

      it("allows empty 'and' children", function () {
        const expr = { type: "op", op: "and", table: "t1", exprs: [{}, {}] }
        return compare(this.exprCleaner.cleanExpr(expr), expr)
      })

      it("allows empty '+' children", function () {
        const expr = { type: "op", op: "+", table: "t1", exprs: [{}, {}] }
        return compare(this.exprCleaner.cleanExpr(expr), expr)
      })

      it("nulls if wrong type", function () {
        const expr = { type: "op", op: "and", table: "t1", exprs: [{}, {}] }
        return compare(this.exprCleaner.cleanExpr(expr, { types: ["number"] }), null)
      })

      it("nulls if missing lhs of non-+/*/and/or expr", function () {
        let expr = { type: "op", op: "= any", table: "t1", exprs: [null, {}] }
        compare(this.exprCleaner.cleanExpr(expr), null)

        expr = { type: "op", op: "=", table: "t1", exprs: [null, {}] }
        compare(this.exprCleaner.cleanExpr(expr), null)

        expr = { type: "op", op: "=", table: "t1", exprs: [null, null] }
        return compare(this.exprCleaner.cleanExpr(expr), null)
      })

      it("allows math on aggregates", function () {
        const field = { type: "field", table: "t1", column: "number" }
        let expr = { type: "op", table: "t1", op: "sum", exprs: [field] }

        expr = { type: "op", op: "+", table: "t1", exprs: [expr, expr] }

        return compare(this.exprCleaner.cleanExpr(expr, { types: ["number"], aggrStatuses: ["aggregate"] }), expr)
      })

      it("allows building math on aggregates", function () {
        const field = { type: "field", table: "t1", column: "number" }
        let expr = { type: "op", table: "t1", op: "sum", exprs: [field] }

        expr = { type: "op", op: "+", table: "t1", exprs: [expr, null] }

        return compare(this.exprCleaner.cleanExpr(expr, { types: ["number"], aggrStatuses: ["aggregate"] }), expr)
      })

      it("does not allow enum = enumset", function () {
        const expr = {
          type: "op",
          op: "=",
          table: "t1",
          exprs: [
            { type: "field", table: "t1", column: "enum" },
            { type: "literal", valueType: "enumset", value: ["a"] }
          ]
        }
        return compare(this.exprCleaner.cleanExpr(expr).exprs[1], null)
      })

      it("defaults op if lhs changes", function () {
        const expr = {
          type: "op",
          op: "= any",
          table: "t1",
          exprs: [
            { type: "field", table: "t1", column: "number" },
            { type: "literal", valueType: "enumset", value: ["a"] }
          ]
        }
        return compare(this.exprCleaner.cleanExpr(expr), {
          type: "op",
          op: "=",
          table: "t1",
          exprs: [{ type: "field", table: "t1", column: "number" }, null]
        })
      })

      it("removes extra exprs", function () {
        const expr = {
          type: "op",
          op: "=",
          table: "t1",
          exprs: [{ type: "field", table: "t1", column: "number" }, null, null]
        }
        return compare(this.exprCleaner.cleanExpr(expr), {
          type: "op",
          op: "=",
          table: "t1",
          exprs: [{ type: "field", table: "t1", column: "number" }, null]
        })
      })

      it("adds missing exprs", function () {
        const expr = { type: "op", op: "=", table: "t1", exprs: [{ type: "field", table: "t1", column: "number" }] }
        return compare(this.exprCleaner.cleanExpr(expr), {
          type: "op",
          op: "=",
          table: "t1",
          exprs: [{ type: "field", table: "t1", column: "number" }, null]
        })
      })

      it("allows null=wildcard unary expressions", function () {
        const expr = {
          type: "op",
          op: "is null",
          table: "t1",
          exprs: [{ type: "field", table: "t1", column: "number" }, null]
        }
        return compare(this.exprCleaner.cleanExpr(expr), {
          type: "op",
          op: "is null",
          table: "t1",
          exprs: [{ type: "field", table: "t1", column: "number" }]
        })
      })

      it("removes invalid enums on rhs", function () {
        const expr = {
          type: "op",
          op: "= any",
          table: "t1",
          exprs: [
            { type: "field", table: "t1", column: "enum" },
            { type: "literal", valueType: "enumset", value: ["a", "x"] }
          ]
        }
        return compare(this.exprCleaner.cleanExpr(expr), {
          type: "op",
          op: "= any",
          table: "t1",
          exprs: [
            { type: "field", table: "t1", column: "enum" },
            { type: "literal", valueType: "enumset", value: ["a"] }
          ]
        })
      }) // x is gone

      it("removes invalid id table on rhs", function () {
        let expr = {
          type: "op",
          op: "=",
          table: "t1",
          exprs: [
            { type: "id", table: "t1" },
            { type: "literal", valueType: "id", idTable: "t1", value: "123" }
          ]
        }
        compare(this.exprCleaner.cleanExpr(expr), expr)

        expr = {
          type: "op",
          op: "=",
          table: "t1",
          exprs: [
            { type: "id", table: "t1" },
            { type: "literal", valueType: "id", idTable: "t2", value: "123" }
          ]
        }
        return compare(this.exprCleaner.cleanExpr(expr), {
          type: "op",
          op: "=",
          table: "t1",
          exprs: [{ type: "id", table: "t1" }, null]
        })
      })

      it("allows empty lhs for prefix-type expressions", function () {
        const expr = { type: "op", op: "distance", table: "t1", exprs: [] }
        return compare(this.exprCleaner.cleanExpr(expr), {
          type: "op",
          op: "distance",
          table: "t1",
          exprs: [null, null]
        })
      })

      it("allows null-type lhs for prefix-type expressions", function () {
        const expr = {
          type: "op",
          op: "distance",
          table: "t1",
          exprs: [{ type: "scalar", table: "t1", joins: ["1-2"], expr: null }, null]
        }
        return compare(this.exprCleaner.cleanExpr(expr), expr)
      })

      return it("removes invalid lhs", function () {
        const expr = {
          type: "op",
          op: "=",
          table: "t1",
          exprs: [{ type: "field", table: "t1", column: "NONSUCH" }, null]
        }
        return compare(this.exprCleaner.cleanExpr(expr), null)
      })
    })

    describe("case", function () {
      it("cleans else", function () {
        const expr = {
          type: "case",
          table: "t1",
          cases: [
            {
              when: { type: "literal", valueType: "boolean", value: true },
              then: { type: "literal", valueType: "number", value: 123 }
            }
          ],
          else: { type: "literal", valueType: "text", value: "abc" }
        }
        return compare(this.exprCleaner.cleanExpr(expr, { types: ["number"] }), {
          type: "case",
          table: "t1",
          cases: [
            {
              when: { type: "literal", valueType: "boolean", value: true },
              then: { type: "literal", valueType: "number", value: 123 }
            }
          ],
          else: null
        })
      })

      it("cleans whens as booleans", function () {
        const expr = {
          type: "case",
          table: "t1",
          cases: [
            {
              when: { type: "literal", valueType: "number", value: 123 },
              then: { type: "literal", valueType: "number", value: 123 }
            }
          ]
        }
        return compare(this.exprCleaner.cleanExpr(expr, { types: ["number"] }), {
          type: "case",
          table: "t1",
          cases: [
            {
              when: { type: "op", op: "=", exprs: [{ type: "literal", valueType: "number", value: 123 }, null] },
              then: { type: "literal", valueType: "number", value: 123 }
            }
          ],
          else: null
        })
      })

      it("simplifies if no cases", function () {
        const expr = {
          type: "case",
          table: "t1",
          cases: [],
          else: { type: "literal", valueType: "text", value: "abc" }
        }
        return compare(this.exprCleaner.cleanExpr(expr, { types: ["text"] }), {
          type: "literal",
          valueType: "text",
          value: "abc"
        })
      })

      it("cleans thens as specified type", function () {
        const expr = {
          type: "case",
          table: "t1",
          cases: [
            {
              when: { type: "literal", valueType: "boolean", value: true },
              then: { type: "literal", valueType: "number", value: 123 }
            }
          ],
          else: null
        }
        return compare(this.exprCleaner.cleanExpr(expr, { types: ["text"] }), {
          type: "case",
          table: "t1",
          cases: [{ when: { type: "literal", valueType: "boolean", value: true }, then: null }],
          else: null
        })
      })

      return it("cleans thens as specified enumValueIds", function () {
        const expr = {
          type: "case",
          table: "t1",
          cases: [
            {
              when: { type: "literal", valueType: "boolean", value: true },
              then: { type: "literal", valueType: "enum", value: "x" }
            }
          ],
          else: null
        }
        return compare(this.exprCleaner.cleanExpr(expr, { types: ["enum"], enumValueIds: ["a"] }), {
          type: "case",
          table: "t1",
          cases: [{ when: { type: "literal", valueType: "boolean", value: true }, then: null }],
          else: null
        })
      })
    })

    describe("score", function () {
      it("cleans input", function () {
        let expr = { type: "score", table: "t1", input: { type: "field", table: "t1", column: "text" }, scores: {} }
        this.clean(expr, { type: "score", table: "t1", input: null, scores: {} })

        expr = { type: "score", table: "t1", input: { type: "field", table: "t1", column: "enum" }, scores: {} }
        return this.clean(expr, expr)
      })

      it("removes invalid scores keys", function () {
        const expr = {
          type: "score",
          table: "t1",
          input: { type: "field", table: "t1", column: "enum" },
          scores: { a: 3, nonsuch: 4 }
        }
        return this.clean(expr, {
          type: "score",
          table: "t1",
          input: { type: "field", table: "t1", column: "enum" },
          scores: { a: 3 }
        })
      })

      it("cleans score values", function () {
        let expr = {
          type: "score",
          table: "t1",
          input: { type: "field", table: "t1", column: "enum" },
          scores: {
            a: { type: "field", table: "t1", column: "number" }
          }
        }
        // Untouched since was number
        this.clean(expr, expr)

        expr = {
          type: "score",
          table: "t1",
          input: { type: "field", table: "t1", column: "enum" },
          scores: {
            a: { type: "field", table: "t1", column: "text" }
          }
        }
        return this.clean(expr, {
          type: "score",
          table: "t1",
          input: { type: "field", table: "t1", column: "enum" },
          scores: {}
        })
      })

      return it("removes all scores if no input", function () {
        const expr = { type: "score", table: "t1", input: null, scores: { a: 3, nonsuch: 4 } }
        return this.clean(expr, { type: "score", table: "t1", input: null, scores: {} })
      })
    })

    describe("build enumset", function () {
      it("cleans values", function () {
        let expr = {
          type: "build enumset",
          table: "t1",
          values: { a: { type: "field", table: "t1", column: "boolean" } }
        }
        this.clean(expr, expr)

        expr = { type: "build enumset", table: "t1", values: { a: { type: "field", table: "t1", column: "xyz" } } }
        return this.clean(expr, { type: "build enumset", table: "t1", values: {} })
      })

      return it("removes invalid value keys", function () {
        const expr = {
          type: "build enumset",
          table: "t1",
          values: {
            a: { type: "literal", valueType: "boolean", value: true },
            b: { type: "literal", valueType: "boolean", value: false }
          }
        }
        return compare(this.exprCleaner.cleanExpr(expr, { types: ["enumset"], enumValueIds: ["a"] }), {
          type: "build enumset",
          table: "t1",
          values: {
            a: { type: "literal", valueType: "boolean", value: true }
          }
        })
      })
    })

    describe("literal", function () {
      it("cleans invalid literal enum valueIds", function () {
        const expr = { type: "literal", valueType: "enum", value: "a" }
        compare(this.exprCleaner.cleanExpr(expr, { enumValueIds: ["a", "b"] }), expr)
        compare(this.exprCleaner.cleanExpr(expr, { enumValueIds: ["b"] }), null)
        return compare(this.exprCleaner.cleanExpr(expr, { enumValueIds: ["a", "b", "c"] }), expr)
      })

      return it("cleans invalid field enum valueIds", function () {
        const expr = { type: "field", table: "t1", column: "enum" }
        compare(this.exprCleaner.cleanExpr(expr, { enumValueIds: ["a", "b"] }), expr)
        return compare(this.exprCleaner.cleanExpr(expr, { enumValueIds: ["b"] }), null)
      })
    })

    return describe("scalar", function () {
      it("leaves valid one alone", function () {
        const fieldExpr = {
          type: "op",
          table: "t2",
          op: "sum",
          exprs: [{ type: "field", table: "t2", column: "number" }]
        }
        const scalarExpr = { type: "scalar", table: "t1", joins: ["1-2"], expr: fieldExpr }

        return compare(scalarExpr, this.exprCleaner.cleanExpr(scalarExpr))
      })

      it("moves aggr to expr", function () {
        const fieldExpr = { type: "field", table: "t2", column: "number" }
        let scalarExpr = { type: "scalar", table: "t1", joins: ["1-2"], expr: fieldExpr, aggr: "sum" }
        scalarExpr = this.exprCleaner.cleanExpr(scalarExpr)
        return compare(scalarExpr, {
          type: "scalar",
          table: "t1",
          joins: ["1-2"],
          expr: { type: "op", op: "sum", table: "t2", exprs: [fieldExpr] }
        })
      })

      it("defaults aggr if needed", function () {
        const fieldExpr = { type: "field", table: "t2", column: "text" }
        let scalarExpr = { type: "scalar", table: "t1", joins: ["1-2"], expr: fieldExpr }
        scalarExpr = this.exprCleaner.cleanExpr(scalarExpr)
        return compare(scalarExpr, {
          type: "scalar",
          table: "t1",
          joins: ["1-2"],
          expr: { type: "op", op: "last", table: "t2", exprs: [fieldExpr] }
        })
      })

      it("strips where if wrong table", function () {
        const fieldExpr = {
          type: "op",
          op: "sum",
          table: "t2",
          exprs: [{ type: "field", table: "t2", column: "number" }]
        }
        const whereExpr = { type: "logical", table: "t1" }
        let scalarExpr = { type: "scalar", table: "t1", joins: ["1-2"], expr: fieldExpr }
        scalarExpr = this.exprCleaner.cleanExpr(scalarExpr)
        assert(scalarExpr.expr, "Should keep expr")
        return assert(!scalarExpr.where, "Should remove where")
      })

      it("strips if invalid join", function () {
        const fieldExpr = { type: "op", op: "sum", exprs: [{ type: "field", table: "t2", column: "number" }] }
        let scalarExpr = { type: "scalar", table: "t1", joins: ["xyz"], expr: fieldExpr }
        scalarExpr = this.exprCleaner.cleanExpr(scalarExpr)
        return assert(!scalarExpr)
      })

      it("simplifies if no joins", function () {
        const fieldExpr = { type: "field", table: "t1", column: "number" }
        let scalarExpr = { type: "scalar", table: "t1", joins: [], expr: fieldExpr }
        scalarExpr = this.exprCleaner.cleanExpr(scalarExpr)
        return compare(fieldExpr, scalarExpr)
      })

      return it("simplifies if id and single join", function () {
        const fieldExpr = { type: "id", table: "t2" }
        const scalarExpr = { type: "scalar", table: "t1", joins: ["1-2"], expr: fieldExpr }
        const expr = this.exprCleaner.cleanExpr(scalarExpr)
        return compare(expr, { type: "field", table: "t1", column: "1-2" })
      })
    })
  })

  // describe "spatial join", ->
  //   it "leaves valid one alone", ->
  //     expr = {
  //       type: "spatial join"
  //       valueExpr: { type: "op", op: "count", table: "t2", exprs: [] }
  //       table: "t1"
  //       toTable: "t2"
  //       fromGeometryExpr: { type: "field", table: "t1", column: "geometry" }
  //       toGeometryExpr: { type: "field", table: "t2", column: "geometry" }
  //       radiusExpr: { type: "literal", valueType: "number", value: 10 }
  //       filterExpr: { type: "field", table: "t2", column: "boolean" }
  //     }

  //     compare(expr, @exprCleaner.cleanExpr(expr))

  //   it "removes invalid filters", ->
  //     expr = {
  //       type: "spatial join"
  //       valueExpr: { type: "op", op: "count", table: "t2", exprs: [] }
  //       table: "t1"
  //       toTable: "t2"
  //       fromGeometryExpr: { type: "field", table: "t1", column: "geometry" }
  //       toGeometryExpr: { type: "field", table: "t2", column: "geometry" }
  //       radiusExpr: { type: "literal", valueType: "number", value: 10 }
  //       filterExpr: { type: "field", table: "t1", column: "boolean" }
  //     }

  //     compare(_.extend({}, expr, { filterExpr: null }), @exprCleaner.cleanExpr(expr))

  // Version 1 expression should be upgraded to version 2
  return describe("upgrade", function () {
    it("count becomes id", function () {
      return this.clean(
        { type: "scalar", table: "t1", aggr: "count", joins: ["1-2"], expr: { type: "count", table: "t2" } },
        { type: "scalar", table: "t1", joins: ["1-2"], expr: { type: "op", op: "count", table: "t2", exprs: [] } }
      )
    })

    it("scalar count becomes id", function () {
      return this.clean(
        { type: "scalar", table: "t1", expr: { type: "count", table: "t1" }, joins: [] },
        { type: "id", table: "t1" }
      )
    })

    it("scalar is simplified", function () {
      return this.clean(
        { type: "scalar", table: "t1", joins: [], expr: { type: "field", table: "t1", column: "number" } },
        { type: "field", table: "t1", column: "number" }
      )
    })

    it("logical becomes op", function () {
      return this.clean(
        {
          type: "logical",
          op: "and",
          table: "t1",
          exprs: [
            { type: "field", table: "t1", column: "boolean" },
            { type: "field", table: "t1", column: "boolean" }
          ]
        },
        {
          type: "op",
          op: "and",
          table: "t1",
          exprs: [
            { type: "field", table: "t1", column: "boolean" },
            { type: "field", table: "t1", column: "boolean" }
          ]
        }
      )
    })

    it("comparison becomes op", function () {
      return this.clean(
        {
          type: "comparison",
          table: "t1",
          lhs: { type: "field", table: "t1", column: "text" },
          op: "~*",
          rhs: { type: "literal", valueType: "text", value: "x" }
        },
        {
          type: "op",
          op: "~*",
          table: "t1",
          exprs: [
            { type: "field", table: "t1", column: "text" },
            { type: "literal", valueType: "text", value: "x" }
          ]
        }
      )
    })

    it("= true is simplified", function () {
      return this.clean(
        { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "boolean" }, op: "= true" },
        { type: "field", table: "t1", column: "boolean" }
      )
    })

    it("= false becomes 'not'", function () {
      return this.clean(
        { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "boolean" }, op: "= false" },
        { type: "op", op: "not", table: "t1", exprs: [{ type: "field", table: "t1", column: "boolean" }] }
      )
    })

    it("enum[] becomes enumset", function () {
      return this.clean(
        { type: "literal", valueType: "enum[]", value: ["a", "b"] },
        { type: "literal", valueType: "enumset", value: ["a", "b"] }
      )
    })

    it("between becomes 3 parameters date", function () {
      return this.clean(
        {
          type: "comparison",
          table: "t1",
          lhs: { type: "field", table: "t1", column: "date" },
          op: "between",
          rhs: { type: "literal", valueType: "daterange", value: ["2014-01-01", "2014-12-31"] }
        },
        {
          type: "op",
          op: "between",
          table: "t1",
          exprs: [
            { type: "field", table: "t1", column: "date" },
            { type: "literal", valueType: "date", value: "2014-01-01" },
            { type: "literal", valueType: "date", value: "2014-12-31" }
          ]
        }
      )
    })

    it("between becomes 3 parameters datetime", function () {
      return this.clean(
        {
          type: "comparison",
          table: "t1",
          lhs: { type: "field", table: "t1", column: "datetime" },
          op: "between",
          rhs: { type: "literal", valueType: "datetimerange", value: ["2014-01-01", "2014-12-31"] }
        },
        {
          type: "op",
          op: "between",
          table: "t1",
          exprs: [
            { type: "field", table: "t1", column: "datetime" },
            { type: "literal", valueType: "datetime", value: "2014-01-01" },
            { type: "literal", valueType: "datetime", value: "2014-12-31" }
          ]
        }
      )
    })

    it("between becomes 3 parameters date if were datetime on date", function () {
      return this.clean(
        {
          type: "comparison",
          table: "t1",
          lhs: { type: "field", table: "t1", column: "date" },
          op: "between",
          rhs: { type: "literal", valueType: "datetimerange", value: ["2014-01-01T01:02:04", "2014-12-31T01:02:04"] }
        },
        {
          type: "op",
          op: "between",
          table: "t1",
          exprs: [
            { type: "field", table: "t1", column: "date" },
            { type: "literal", valueType: "date", value: "2014-01-01" },
            { type: "literal", valueType: "date", value: "2014-12-31" }
          ]
        }
      )
    })

    it("upgrades legacy entity join references", function () {
      let schema = this.schema.addTable({
        id: "entities.wwmc_visit",
        contents: [{ id: "site", type: "join", join: { type: "n-1", toTable: "entities.surface_water" } }]
      })

      schema = schema.addTable({
        id: "entities.surface_water",
        contents: [{ id: "location", type: "geometry" }]
      })

      const exprCleaner = new ExprCleaner(schema)

      const clean = (expr: any, expected: any, options: any) => {
        return compare(exprCleaner.cleanExpr(expr, options), expected)
      }

      return clean(
        {
          type: "scalar",
          expr: { type: "field", table: "entities.surface_water", column: "location" },
          joins: ["entities.wwmc_visit.site"],
          table: "entities.wwmc_visit"
        },
        {
          type: "scalar",
          expr: { type: "field", table: "entities.surface_water", column: "location" },
          joins: ["site"],
          table: "entities.wwmc_visit"
        }
      )
    })

    return it("upgrades complex expression with legacy literals", function () {
      const expr1 = {
        type: "comparison",
        table: "t1",
        op: "=",
        lhs: { type: "field", table: "t1", column: "number" },
        rhs: { type: "literal", valueType: "integer", value: 4 }
      }
      const expr2 = {
        type: "comparison",
        table: "t1",
        op: "=",
        lhs: { type: "field", table: "t1", column: "number" },
        rhs: { type: "literal", valueType: "integer", value: 5 }
      }
      const value = { type: "logical", table: "t1", op: "and", exprs: [expr1, expr2] }

      return this.clean(value, {
        type: "op",
        op: "and",
        table: "t1",
        exprs: [
          {
            type: "op",
            table: "t1",
            op: "=",
            exprs: [
              { type: "field", table: "t1", column: "number" },
              { type: "literal", valueType: "number", value: 4 }
            ]
          },
          {
            type: "op",
            table: "t1",
            op: "=",
            exprs: [
              { type: "field", table: "t1", column: "number" },
              { type: "literal", valueType: "number", value: 5 }
            ]
          }
        ]
      })
    })
  });
})
