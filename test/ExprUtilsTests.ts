// TODO: This file was created by bulk-decaffeinate.
// Sanity-check the conversion and remove this comment.
import { assert } from "chai"
import * as fixtures from "./fixtures"
import _ from "lodash"
import { default as ExprUtils } from "../src/ExprUtils"
import { default as Schema } from "../src/Schema"
import canonical from "canonical-json"

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
  { id: "varid", name: { _base: "en", en: "Varid" }, type: "id", idTable: "t2" },
  { id: "varidexpr", name: { _base: "en", en: "Varidexpr" }, type: "id", table: "t1", idTable: "t2" }
]

function compare(actual: any, expected: any) {
  return assert.equal(
    canonical(actual),
    canonical(expected),
    "\ngot: " + canonical(actual) + "\nexp: " + canonical(expected) + "\n"
  )
}

describe("ExprUtils", function () {
  beforeEach(function () {
    return (this.exprUtils = new ExprUtils(fixtures.simpleSchema(), variables))
  })

  it("determines if multiple joins", function () {
    assert.isTrue(this.exprUtils.isMultipleJoins("t1", ["1-2"]))
    return assert.isFalse(this.exprUtils.isMultipleJoins("t2", ["2-1"]))
  })

  it("follows joins", function () {
    assert.equal(this.exprUtils.followJoins("t1", []), "t1")
    return assert.equal(this.exprUtils.followJoins("t1", ["1-2"]), "t2")
  })

  it("localizes strings", function () {
    assert.equal(this.exprUtils.localizeString("apple", "en"), "apple")
    assert.equal(this.exprUtils.localizeString({ en: "apple", fr: "pomme" }, "fr"), "pomme")
    assert.equal(this.exprUtils.localizeString({ en: "apple", fr: "pomme" }, null), "apple")
    assert.equal(this.exprUtils.localizeString({ _base: "fr", fr: "pomme" }, null), "pomme")
    assert.equal(
      this.exprUtils.localizeString({ _base: "fr", en: "apple", fr: "pomme" }, null),
      "pomme",
      "_base wins if no locale"
    )
    return assert.equal(
      this.exprUtils.localizeString({ _base: "fr", en: "apple", fr: "pomme" }, "en"),
      "apple",
      "_base wins if no locale"
    )
  })

  it("getExprTable", function () {
    return assert.equal(this.exprUtils.getExprTable({ table: "xyz", type: "id" }), "xyz")
  })

  describe("getExprIdTable", function () {
    it("gets for literal", function () {
      return assert.equal(
        this.exprUtils.getExprIdTable({ type: "literal", valueType: "id", idTable: "xyz", value: "123" }),
        "xyz"
      )
    })

    it("gets for id field", function () {
      return assert.equal(this.exprUtils.getExprIdTable({ table: "xyz", type: "id" }), "xyz")
    })

    it("gets for id expr field", function () {
      return assert.equal(this.exprUtils.getExprIdTable({ type: "field", table: "t1", column: "expr_id" }), "t1")
    })

    it("gets for scalar", function () {
      return assert.equal(
        this.exprUtils.getExprIdTable({
          type: "scalar",
          table: "t2",
          joins: ["2-1"],
          expr: { type: "id", table: "t1" }
        }),
        "t1"
      )
    })

    return it("gets for variable", function () {
      return assert.equal(
        this.exprUtils.getExprIdTable({ type: "variable", table: "t1", variableId: "varidexpr" }),
        "t2"
      )
    })
  })

  describe("getExprAggrStatus", function () {
    it("gets for literal", function () {
      return assert.equal(
        this.exprUtils.getExprAggrStatus({ type: "literal", valueType: "id", idTable: "xyz", value: "123" }),
        "literal"
      )
    })

    it("gets for id", function () {
      return assert.equal(this.exprUtils.getExprAggrStatus({ table: "xyz", type: "id" }), "individual")
    })

    it("gets for field", function () {
      return assert.equal(
        this.exprUtils.getExprAggrStatus({ type: "field", table: "t1", column: "number" }),
        "individual"
      )
    })

    it("gets for expr field", function () {
      return assert.equal(
        this.exprUtils.getExprAggrStatus({ type: "field", table: "t1", column: "expr_sum" }),
        "aggregate"
      )
    })

    it("gets for aggregate", function () {
      return assert.equal(
        this.exprUtils.getExprAggrStatus({
          type: "op",
          op: "sum",
          exprs: [{ type: "field", table: "xyz", column: "abc" }]
        }),
        "aggregate"
      )
    })

    it("gets for aggregate + literal", function () {
      return assert.equal(
        this.exprUtils.getExprAggrStatus({
          type: "op",
          op: "+",
          exprs: [
            { type: "op", op: "sum", exprs: [{ type: "field", table: "xyz", column: "abc" }] },
            { type: "literal", valueType: "number", value: 123 }
          ]
        }),
        "aggregate"
      )
    })

    it("gets for scalar", function () {
      return assert.equal(
        this.exprUtils.getExprAggrStatus({
          type: "scalar",
          table: "t2",
          joins: ["2-1"],
          expr: { type: "id", table: "t1" }
        }),
        "individual"
      )
    })

    it("gets for scalar aggregation", function () {
      return assert.equal(
        this.exprUtils.getExprAggrStatus({
          type: "scalar",
          table: "t1",
          joins: ["1-2"],
          expr: {
            type: "op",
            op: "sum",
            exprs: [{ type: "field", table: "t2", column: "number" }]
          }
        }),
        "individual"
      )
    })

    it("gets for literal variable", function () {
      return assert.equal(this.exprUtils.getExprAggrStatus({ type: "variable", variableId: "varnumber" }), "literal")
    })

    return it("gets for individual variable", function () {
      return assert.equal(
        this.exprUtils.getExprAggrStatus({ type: "variable", table: "t1", variableId: "varidexpr" }),
        "individual"
      )
    })
  })

  describe("findMatchingOpItems", function () {
    it("finds = for number", function () {
      return assert.equal(
        this.exprUtils.findMatchingOpItems({ lhsExpr: { type: "field", table: "t1", column: "number" } })[0].op,
        "="
      )
    })

    it("finds = for expr number", function () {
      return assert.equal(
        this.exprUtils.findMatchingOpItems({ lhsExpr: { type: "field", table: "t1", column: "expr_number" } })[0].op,
        "="
      )
    })

    it("first = for id type non-hierarchical", function () {
      return assert.equal(this.exprUtils.findMatchingOpItems({ lhsExpr: { type: "id", table: "t1" } })[0].op, "=")
    })

    return it("first within for id type hierarchical", function () {
      return assert.equal(
        this.exprUtils.findMatchingOpItems({ lhsExpr: { type: "id", table: "thier" } })[0].op,
        "within"
      )
    })
  })

  describe("getAggrTypes", function () {
    beforeEach(function () {
      this.schema = new Schema().addTable({
        id: "a",
        name: "A",
        ordering: "z",
        contents: [
          { id: "y", name: "Y", type: "text" },
          { id: "z", name: "Z", type: "number" }
        ]
      })
      return (this.exprUtils = new ExprUtils(this.schema))
    })

    it("includes text (last)", function () {
      const field = { type: "field", table: "a", column: "y" }
      const types = this.exprUtils.getAggrTypes(field)
      return assert.isTrue(types.includes("text"), JSON.stringify(types))
    })

    return it("doesn't include last normally", function () {
      this.schema = this.schema.addTable({ id: "b", name: "B", contents: [{ id: "x", name: "X", type: "text" }] })
      this.exprUtils = new ExprUtils(this.schema)

      const field = { type: "field", table: "b", column: "x" }
      const types = this.exprUtils.getAggrTypes(field)
      return assert.deepEqual(types, ["text[]", "number"])
    })
  })

  describe("getExprType", function () {
    it("gets field type", function () {
      return assert.equal(this.exprUtils.getExprType({ type: "field", table: "t1", column: "text" }), "text")
    })

    it("gets expr field type", function () {
      return assert.equal(this.exprUtils.getExprType({ type: "field", table: "t1", column: "expr_number" }), "number")
    })

    it("gets join field type", function () {
      return assert.equal(this.exprUtils.getExprType({ type: "field", table: "t1", column: "1-2" }), "id[]")
    })

    it("gets scalar type", function () {
      const expr = {
        type: "scalar",
        table: "t1",
        expr: { type: "field", table: "t1", column: "text" },
        joins: []
      }
      return assert.equal(this.exprUtils.getExprType(expr), "text")
    })

    it("gets scalar type with aggr", function () {
      const expr = {
        type: "scalar",
        table: "t1",
        expr: { type: "field", table: "t2", column: "number" },
        aggr: "avg",
        joins: ["1-2"]
      }
      return assert.equal(this.exprUtils.getExprType(expr), "number")
    })

    it("gets scalar type with count", function () {
      const expr = {
        type: "scalar",
        table: "t1",
        expr: { type: "count", table: "t2" },
        aggr: "count",
        joins: ["1-2"]
      }
      return assert.equal(this.exprUtils.getExprType(expr), "number")
    })

    it("gets literal types", function () {
      return assert.equal(this.exprUtils.getExprType({ type: "literal", valueType: "boolean", value: true }), "boolean")
    })

    it("gets boolean type for and/or", function () {
      assert.equal(this.exprUtils.getExprType({ type: "op", op: "and", exprs: [] }), "boolean")
      return assert.equal(this.exprUtils.getExprType({ type: "op", op: "or", exprs: [] }), "boolean")
    })

    it("gets boolean type for =", function () {
      return assert.equal(this.exprUtils.getExprType({ type: "op", op: "=", exprs: [] }), "boolean")
    })

    it("no type for {}", function () {
      return assert.isNull(this.exprUtils.getExprType({}))
    })

    it("number type if number + number", function () {
      return assert.equal(
        this.exprUtils.getExprType({
          type: "op",
          op: "+",
          exprs: [
            { type: "field", table: "t1", column: "number" },
            { type: "field", table: "t1", column: "number" }
          ]
        }),
        "number"
      )
    })

    return it("variable type", function () {
      return assert.equal(this.exprUtils.getExprType({ type: "variable", variableId: "varnumber" }), "number")
    })
  })

  describe("summarizeExpr", function () {
    it("summarizes null", function () {
      return assert.equal(this.exprUtils.summarizeExpr(null), "None")
    })

    it("summarizes field expr", function () {
      const expr = { type: "field", table: "t1", column: "number" }
      return assert.equal(this.exprUtils.summarizeExpr(expr), "Number")
    })

    it("summarizes simple scalar expr", function () {
      const fieldExpr = { type: "field", table: "t1", column: "number" }
      const scalarExpr = { type: "scalar", table: "t1", joins: [], expr: fieldExpr }
      return assert.equal(this.exprUtils.summarizeExpr(scalarExpr), "Number")
    })

    it("summarizes joined scalar expr", function () {
      const fieldExpr = { type: "field", table: "t2", column: "number" }
      const scalarExpr = { type: "scalar", table: "t1", joins: ["1-2"], expr: fieldExpr }
      return assert.equal(this.exprUtils.summarizeExpr(scalarExpr), "T1->T2 > Number")
    })

    it("summarizes joined aggr scalar expr", function () {
      const fieldExpr = { type: "field", table: "t2", column: "number" }
      const scalarExpr = { type: "scalar", table: "t1", joins: ["1-2"], expr: fieldExpr, aggr: "sum" }
      return assert.equal(this.exprUtils.summarizeExpr(scalarExpr), "T1->T2 > Total Number")
    })

    it("simplifies when count id", function () {
      const scalarExpr = {
        type: "scalar",
        table: "t1",
        joins: ["1-2"],
        expr: { type: "id", table: "t2" },
        aggr: "count"
      }
      return assert.equal(this.exprUtils.summarizeExpr(scalarExpr), "T1->T2 > Number of T2")
    })

    it("simplifies when id", function () {
      const scalarExpr = { type: "scalar", table: "t2", joins: ["2-1"], expr: { type: "id", table: "t1" } }
      return assert.equal(this.exprUtils.summarizeExpr(scalarExpr), "T2->T1")
    })

    it("shows join types", function () {
      const scalarExpr = {
        type: "scalar",
        table: "t3",
        joins: ["3-2"],
        expr: { type: "field", table: "t2", column: "2-1" }
      }
      return assert.equal(this.exprUtils.summarizeExpr(scalarExpr), "T3->T2 > T2->T1")
    })

    it("summarizes +/-/*//", function () {
      const fieldExpr = { type: "field", table: "t2", column: "number" }
      const literalExpr = { type: "literal", valueType: "number", value: 5 }
      const opExpr = { type: "op", op: "+", exprs: [fieldExpr, literalExpr] }
      return assert.equal(this.exprUtils.summarizeExpr(opExpr), "Number + 5")
    })

    it("summarizes case with else", function () {
      const expr = {
        type: "case",
        cases: [
          {
            when: { type: "field", table: "t1", column: "boolean" },
            then: { type: "field", table: "t1", column: "text" }
          }
        ],
        else: { type: "field", table: "t1", column: "text" }
      }
      return assert.equal(this.exprUtils.summarizeExpr(expr), "If Boolean Then Text Else Text")
    })

    it("summarizes = with enum literal", function () {
      const fieldExpr = { type: "field", table: "t1", column: "enum" }
      const literalExpr = { type: "literal", valueType: "enum", value: "a" }
      const opExpr = { type: "op", op: "=", exprs: [fieldExpr, literalExpr] }
      return assert.equal(this.exprUtils.summarizeExpr(opExpr), "Enum is A")
    })

    it("summarizes = any with enumset literal", function () {
      const fieldExpr = { type: "field", table: "t1", column: "enum" }
      const literalExpr = { type: "literal", valueType: "enumset", value: ["a", "b"] }
      const opExpr = { type: "op", op: "= any", exprs: [fieldExpr, literalExpr] }
      return assert.equal(this.exprUtils.summarizeExpr(opExpr), "Enum is any of A, B")
    })

    it("summarizes contains with enumset literal", function () {
      const fieldExpr = { type: "field", table: "t1", column: "enumset" }
      const literalExpr = { type: "literal", valueType: "enumset", value: ["a"] }
      const opExpr = { type: "op", op: "contains", exprs: [fieldExpr, literalExpr] }
      return assert.equal(this.exprUtils.summarizeExpr(opExpr), "EnumSet includes all of A")
    })

    it("summarizes intersects with enumset literal", function () {
      const fieldExpr = { type: "field", table: "t1", column: "enumset" }
      const literalExpr = { type: "literal", valueType: "enumset", value: ["a"] }
      const opExpr = { type: "op", op: "intersects", exprs: [fieldExpr, literalExpr] }
      return assert.equal(this.exprUtils.summarizeExpr(opExpr), "EnumSet includes any of A")
    })

    it("summarizes contains with text[] literal", function () {
      const fieldExpr = { type: "field", table: "t1", column: "text[]" }
      const literalExpr = { type: "literal", valueType: "text[]", value: ["a"] }
      const opExpr = { type: "op", op: "contains", exprs: [fieldExpr, literalExpr] }
      return assert.equal(this.exprUtils.summarizeExpr(opExpr), "Text[] includes all of a")
    })

    it("summarizes intersects with enumset literal", function () {
      const fieldExpr = { type: "field", table: "t1", column: "text[]" }
      const literalExpr = { type: "literal", valueType: "text[]", value: ["a"] }
      const opExpr = { type: "op", op: "intersects", exprs: [fieldExpr, literalExpr] }
      return assert.equal(this.exprUtils.summarizeExpr(opExpr), "Text[] includes any of a")
    })

    it("summarizes sum(field) expr", function () {
      const expr = { type: "op", op: "sum", table: "t2", exprs: [{ type: "field", table: "t2", column: "number" }] }
      return assert.equal(this.exprUtils.summarizeExpr(expr), "Total Number")
    })

    it("summarizes count", function () {
      const expr = { type: "op", op: "count", table: "t1", exprs: [] }
      return assert.equal(this.exprUtils.summarizeExpr(expr), "Number of T1")
    })

    it("summarizes max where expr", function () {
      const expr = {
        type: "op",
        op: "max where",
        table: "t1",
        exprs: [
          { type: "field", table: "t1", column: "number" },
          { type: "field", table: "t1", column: "boolean" }
        ]
      }
      return assert.equal(this.exprUtils.summarizeExpr(expr), "Maximum Number of Boolean")
    })

    it("summarizes max where expr without rhs", function () {
      const expr = {
        type: "op",
        op: "max where",
        table: "t1",
        exprs: [{ type: "field", table: "t1", column: "number" }, null]
      }
      return assert.equal(this.exprUtils.summarizeExpr(expr), "Maximum Number of All")
    })

    it("summarizes max where without expr or rhs", function () {
      const expr = { type: "op", op: "max where", table: "t1", exprs: [] }
      return assert.equal(this.exprUtils.summarizeExpr(expr), "Maximum ")
    })

    it("summarizes date ops", function () {
      const expr = { type: "op", op: "thisyear", table: "t1", exprs: [{ type: "field", table: "t1", column: "date" }] }
      return assert.equal(this.exprUtils.summarizeExpr(expr), "Date is this year")
    })

    return it("summarizes variable", function () {
      const expr = { type: "variable", variableId: "varnumber" }
      return assert.equal(this.exprUtils.summarizeExpr(expr), "Varnumber")
    })
  })

  // TODO readd
  // it "uses named expression when matching one present", ->
  //   # Add named expression
  //   @schema.addNamedExpr("t1", { id: "number", name: "NE Column 1", expr: { type: "field", table: "t1", column: "number" }})

  //   # Test with scalar that can simplify
  //   expr = {
  //     type: "scalar"
  //     table: "t1"
  //     expr: { type: "field", table: "t1", column: "number" }
  //     joins: []
  //   }
  //   assert.equal @exprUtils.summarizeExpr(expr), "NE Column 1"

  describe("stringifyExprLiteral", function () {
    it("stringifies number", function () {
      const str = this.exprUtils.stringifyExprLiteral({ type: "field", table: "t1", column: "number" }, 2.34)
      return assert.equal(str, "2.34")
    })

    it("stringifies expr number", function () {
      const str = this.exprUtils.stringifyExprLiteral({ type: "field", table: "t1", column: "expr_number" }, 2.34)
      return assert.equal(str, "2.34")
    })

    it("stringifies null", function () {
      const str = this.exprUtils.stringifyExprLiteral({ type: "field", table: "t1", column: "number" }, null)
      return assert.equal(str, "None")
    })

    it("looks up enum", function () {
      const str = this.exprUtils.stringifyExprLiteral({ type: "field", table: "t1", column: "enum" }, "a")
      return assert.equal(str, "A")
    })

    it("handles null enum", function () {
      const str = this.exprUtils.stringifyExprLiteral({ type: "field", table: "t1", column: "enum" }, null)
      return assert.equal(str, "None")
    })

    it("handles invalid enum", function () {
      const str = this.exprUtils.stringifyExprLiteral({ type: "field", table: "t1", column: "enum" }, "xyz")
      return assert.equal(str, "???")
    })

    it("looks up enumset", function () {
      const str = this.exprUtils.stringifyExprLiteral({ type: "field", table: "t1", column: "enumset" }, ["a", "b"])
      return assert.equal(str, "A, B")
    })

    it("handles null enumset", function () {
      const str = this.exprUtils.stringifyExprLiteral({ type: "field", table: "t1", column: "enumset" }, null)
      return assert.equal(str, "None")
    })

    it("handles invalid enumset", function () {
      const str = this.exprUtils.stringifyExprLiteral({ type: "field", table: "t1", column: "enumset" }, ["xyz", "b"])
      return assert.equal(str, "???, B")
    })

    return it("handles text[]", function () {
      const str = this.exprUtils.stringifyExprLiteral({ type: "field", table: "t1", column: "text[]" }, ["xyz", "b"])
      return assert.equal(str, "xyz, b")
    })
  })

  describe("findMatchingOpItems", () =>
    it("finds = any for text lhs with boolean result", function () {
      const opItem = this.exprUtils.findMatchingOpItems({ resultType: "boolean", exprTypes: ["text"] })[0]
      assert.equal(opItem.op, "= any")
      return assert.equal(opItem.exprTypes[0], "text")
    }))

  describe("getExprEnumValues", function () {
    it("finds in field", function () {
      return assert.deepEqual(this.exprUtils.getExprEnumValues({ type: "field", table: "t1", column: "enum" }), [
        { id: "a", name: { en: "A" } },
        { id: "b", name: { en: "B" } }
      ])
    })

    it("finds in case statement thens", function () {
      const expr = {
        type: "case",
        cases: [{ when: null, then: { type: "field", table: "t1", column: "enum" } }]
      }
      return assert.deepEqual(this.exprUtils.getExprEnumValues(expr), [
        { id: "a", name: { en: "A" } },
        { id: "b", name: { en: "B" } }
      ])
    })

    it("finds in case statement else", function () {
      const expr = {
        type: "case",
        cases: [{ when: null, then: null }],
        else: { type: "field", table: "t1", column: "enum" }
      }
      return assert.deepEqual(this.exprUtils.getExprEnumValues(expr), [
        { id: "a", name: { en: "A" } },
        { id: "b", name: { en: "B" } }
      ])
    })

    it("handles special month case", function () {
      const expr = {
        type: "op",
        op: "month",
        exprs: [{ type: "field", table: "t1", column: "date" }]
      }
      return assert.deepEqual(this.exprUtils.getExprEnumValues(expr), [
        { id: "01", name: { _base: "en", en: "January" } },
        { id: "02", name: { _base: "en", en: "February" } },
        { id: "03", name: { _base: "en", en: "March" } },
        { id: "04", name: { _base: "en", en: "April" } },
        { id: "05", name: { _base: "en", en: "May" } },
        { id: "06", name: { _base: "en", en: "June" } },
        { id: "07", name: { _base: "en", en: "July" } },
        { id: "08", name: { _base: "en", en: "August" } },
        { id: "09", name: { _base: "en", en: "September" } },
        { id: "10", name: { _base: "en", en: "October" } },
        { id: "11", name: { _base: "en", en: "November" } },
        { id: "12", name: { _base: "en", en: "December" } }
      ])
    })

    return it("finds in field", function () {
      return assert.deepEqual(this.exprUtils.getExprEnumValues({ type: "variable", variableId: "varenum" }), [
        { id: "a", name: { en: "A" } },
        { id: "b", name: { en: "B" } }
      ])
    })
  })

  describe("getReferencedFields", function () {
    it("gets field", function () {
      const cols = this.exprUtils.getReferencedFields({ type: "field", table: "t1", column: "number" })
      return compare(cols, [{ type: "field", table: "t1", column: "number" }])
    })

    it("gets expr field", function () {
      const cols = this.exprUtils.getReferencedFields({ type: "field", table: "t1", column: "expr_number" })
      return compare(cols, [
        { type: "field", table: "t1", column: "expr_number" },
        { type: "field", table: "t1", column: "number" }
      ])
    })

    it("gets join", function () {
      const cols = this.exprUtils.getReferencedFields({
        type: "scalar",
        table: "t1",
        joins: ["1-2"],
        expr: { type: "op", table: "t2", exprs: [{ type: "field", table: "t2", column: "number" }] }
      })
      return compare(cols, [
        { type: "field", table: "t1", column: "1-2" },
        { type: "field", table: "t2", column: "number" }
      ])
    })

    it("recurses into ops", function () {
      const cols = this.exprUtils.getReferencedFields({
        type: "op",
        op: "+",
        exprs: [{ type: "field", table: "t1", column: "number" }]
      })
      return compare(cols, [{ type: "field", table: "t1", column: "number" }])
    })

    it("includes cases", function () {
      const cols = this.exprUtils.getReferencedFields({
        type: "case",
        cases: [
          {
            when: { type: "field", table: "t1", column: "boolean" },
            then: { type: "field", table: "t1", column: "enum" }
          }
        ],
        else: { type: "field", table: "t1", column: "text" }
      })

      return compare(cols, [
        { type: "field", table: "t1", column: "boolean" },
        { type: "field", table: "t1", column: "enum" },
        { type: "field", table: "t1", column: "text" }
      ])
    })

    it("includes build enumset", function () {
      const cols = this.exprUtils.getReferencedFields({
        type: "build enumset",
        table: "t1",
        values: {
          a: { type: "field", table: "t1", column: "boolean" }
        }
      })

      return compare(cols, [{ type: "field", table: "t1", column: "boolean" }])
    })

    return it("de-duplicates", function () {
      const cols = this.exprUtils.getReferencedFields({
        type: "op",
        op: "+",
        exprs: [
          { type: "field", table: "t1", column: "number" },
          { type: "field", table: "t1", column: "number" }
        ]
      })
      return compare(cols, [{ type: "field", table: "t1", column: "number" }])
    })
  })

  describe("inlineVariableValues", function () {
    it("inlines literals", function () {
      const expr = {
        type: "op",
        op: ">",
        exprs: [
          { type: "variable", variableId: "varnumber" },
          { type: "literal", valueType: "number", value: 3 }
        ]
      }
      const result = {
        type: "op",
        op: ">",
        exprs: [
          { type: "literal", valueType: "number", value: 4 },
          { type: "literal", valueType: "number", value: 3 }
        ]
      }
      return compare(
        this.exprUtils.inlineVariableValues(expr, { varnumber: { type: "literal", valueType: "number", value: 4 } }),
        result
      )
    })

    it("inlines literal ids", function () {
      const expr = {
        type: "op",
        op: "=",
        exprs: [
          { type: "variable", variableId: "varid" },
          { type: "literal", valueType: "id", idTable: "t2", value: "123" }
        ]
      }
      const result = {
        type: "op",
        op: "=",
        exprs: [
          { type: "literal", valueType: "id", idTable: "t2", value: "123" },
          { type: "literal", valueType: "id", idTable: "t2", value: "123" }
        ]
      }
      return compare(
        this.exprUtils.inlineVariableValues(expr, {
          varid: { type: "literal", valueType: "id", idTable: "t2", value: "123" }
        }),
        result
      )
    })

    return it("nulls entire value if literal null", function () {
      const expr = {
        type: "op",
        op: ">",
        exprs: [
          { type: "variable", variableId: "varnumber" },
          { type: "literal", valueType: "number", value: 3 }
        ]
      }
      const result = { type: "op", op: ">", exprs: [null, { type: "literal", valueType: "number", value: 3 }] }
      return compare(this.exprUtils.inlineVariableValues(expr, { varnumber: null }), result)
    })
  })

  return describe("andExprs", function () {
    it("handles trivial case", function () {
      assert.isNull(ExprUtils.andExprs("xyz"))
      assert.isNull(ExprUtils.andExprs("xyz", null))
      return assert.isNull(ExprUtils.andExprs("xyz", null, null))
    })

    return it("denests", () =>
      compare(
        ExprUtils.andExprs(
          "t1",
          { type: "field", table: "t1", column: "b1" },
          {
            type: "op",
            op: "and",
            exprs: [
              { type: "field", table: "t1", column: "b2" },
              { type: "field", table: "t1", column: "b3" }
            ]
          }
        ),
        {
          type: "op",
          op: "and",
          table: "t1",
          exprs: [
            { type: "field", table: "t1", column: "b1" },
            { type: "field", table: "t1", column: "b2" },
            { type: "field", table: "t1", column: "b3" }
          ]
        }
      ))
  })
})
