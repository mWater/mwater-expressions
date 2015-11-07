assert = require('chai').assert
_ = require 'lodash'
Schema = require '../src/Schema'
ExprCleaner = require '../src/ExprCleaner'
fixtures = require './fixtures'

canonical = require 'canonical-json'

compare = (actual, expected) ->
  assert.equal canonical(actual), canonical(expected), "\n" + canonical(actual) + "\n" + canonical(expected)

describe "ExprCleaner", ->
  beforeEach ->
    @schema = fixtures.simpleSchema()
    @exprCleaner = new ExprCleaner(@schema)

  describe "cleanExpr", ->
    it "nulls if wrong table", ->
      assert.isNull @exprCleaner.cleanExpr({ type: "field", table: "t1", column: "text" }, table: "t2")

    it "preserves 'and' by cleaning child expressions with boolean type", ->
      expr = { type: "op", op: "and", table: "t1", exprs: [{ type: "field", table: "t1", column: "text" }, { type: "field", table: "t1", column: "boolean" }]}

      compare(@exprCleaner.cleanExpr(expr), {
        type: "op"
        op: "and"
        table: "t1"
        exprs: [
          # Wrapped in =any to make boolean
          { type: "op", table: "t1", op: "= any", exprs: [{ type: "field", table: "t1", column: "text" }] }
          # Untouched
          { type: "field", table: "t1", column: "boolean" }
        ]})

    it "simplifies and", ->
      expr = { type: "op", op: "and", table: "t1", exprs: [{ type: "field", table: "t1", column: "boolean" }]}
      compare(@exprCleaner.cleanExpr(expr), { type: "field", table: "t1", column: "boolean" })

      expr = { type: "op", op: "and", table: "t1", exprs: []}
      compare(@exprCleaner.cleanExpr(expr), null)

    describe "boolean required", ->
      before ->
        @clean = (before, afterExpected) ->
          after = @exprCleaner.cleanExpr(before, type: "boolean")
          compare(after, afterExpected)

      it "wraps enum with '= any'", ->
        field = { type: "field", table: "t1", column: "enum" }
        @clean(
          field
          { type: "op", table: "t1", op: "= any", exprs: [field]}
        )

      it "wraps text with '= any'", ->
        field = { type: "field", table: "t1", column: "text" }
        @clean(
          field
          { type: "op", table: "t1", op: "= any", exprs: [field]}
        )

      it "wraps number with '='", ->
        field = { type: "field", table: "t1", column: "number" }
        @clean(
          field
          { type: "op", table: "t1", op: "=", exprs: [field]}
        )

  describe "scalar", ->
    it "leaves valid one alone", ->
      fieldExpr = { type: "field", table: "t2", column: "number" }
      scalarExpr = { type: "scalar", table: "t1", joins: ['1-2'], expr: fieldExpr, aggr: "sum" }

      assert.equal scalarExpr, @exprCleaner.cleanExpr(scalarExpr)

    it "strips aggr if not needed", ->
      fieldExpr = { type: "field", table: "t2", column: "number" }
      scalarExpr = { type: "scalar", table: "t1", joins: [], expr: fieldExpr, aggr: "sum" }
      scalarExpr = @exprCleaner.cleanExpr(scalarExpr)
      assert not scalarExpr.aggr

    it "defaults aggr if needed and wrong", ->
      fieldExpr = { type: "field", table: "t2", column: "text" }
      scalarExpr = { type: "scalar", table: "t1", joins: ['1-2'], expr: fieldExpr, aggr: "sum" }
      scalarExpr = @exprCleaner.cleanExpr(scalarExpr)
      assert.equal scalarExpr.aggr, "last"

    it "strips where if wrong table", ->
      fieldExpr = { type: "field", table: "t2", column: "number" }
      whereExpr = { type: "logical", table: "t1" }
      scalarExpr = { type: "scalar", table: "t1", joins: ['1-2'], expr: fieldExpr, aggr: "sum" }
      scalarExpr = @exprCleaner.cleanExpr(scalarExpr)
      assert.equal scalarExpr.aggr, "sum"
      assert not scalarExpr.where

    it "strips if invalid join", ->
      fieldExpr = { type: "field", table: "t2", column: "number" }
      scalarExpr = { type: "scalar", table: "t1", joins: ['xyz'], expr: fieldExpr, aggr: "sum" }
      scalarExpr = @exprCleaner.cleanExpr(scalarExpr)
      assert not scalarExpr

    it "simplifies if no joins"

  # describe "cleanComparisonExpr", ->
  #   it "removes op if no lhs", ->
  #     expr = { type: "comparison", op: "=" }
  #     expr = @exprCleaner.cleanComparisonExpr(expr)
  #     assert not expr.op

  #   it "removes rhs if wrong type", ->
  #     expr = { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "text" }, op: "~*", rhs: { type: "literal", valueType: "text", value: "x" } }
  #     expr = @exprCleaner.cleanComparisonExpr(expr)
  #     assert expr.rhs, "should keep"

  #     expr = { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "text" }, op: "~*", rhs: { type: "literal", valueType: "number", value: 3 } }
  #     expr = @exprCleaner.cleanComparisonExpr(expr)
  #     assert not expr.rhs, "should remove"

  #   it "removes rhs if invalid enum", ->
  #     expr = { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "enum" }, op: "=", rhs: { type: "literal", valueType: "enum", value: "a" } }
  #     expr = @exprCleaner.cleanComparisonExpr(expr)
  #     assert expr.rhs, "should keep"

  #     expr = { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "enum" }, op: "=", rhs: { type: "literal", valueType: "enum", value: "x" } }
  #     expr = @exprCleaner.cleanComparisonExpr(expr)
  #     assert not expr.rhs

  #   it "removes rhs if empty enum[]", ->
  #     expr = { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "enum" }, op: "= any", rhs: { type: "literal", valueType: "enum[]", value: ['a'] } }
  #     expr = @exprCleaner.cleanComparisonExpr(expr)
  #     assert expr.rhs, "should keep"

  #     expr = { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "enum" }, op: "= any", rhs: { type: "literal", valueType: "enum[]", value: [] } }
  #     expr = @exprCleaner.cleanComparisonExpr(expr)
  #     assert not expr.rhs

  #   it "defaults op", ->
  #     expr = { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "text" } }
  #     expr = @exprCleaner.cleanComparisonExpr(expr)
  #     assert.equal expr.op, "= any"

  #   it "removes invalid op", ->
  #     expr = { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "text" }, op: ">" }
  #     expr = @exprCleaner.cleanComparisonExpr(expr)
  #     assert.equal expr.op, "= any"
