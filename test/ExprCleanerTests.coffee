assert = require('chai').assert
_ = require 'lodash'
Schema = require '../src/Schema'
ExprCleaner = require '../src/ExprCleaner'
fixtures = require './fixtures'

describe "ExprCleaner", ->
  beforeEach ->
    @schema = fixtures.simpleSchema()
    @exprCleaner = new ExprCleaner(@schema)

  describe "cleanExpr", ->
    describe "boolean required", ->
      it "wraps enum with '= any' with empty list", ->
        expr = { type: "field", }

  describe "cleanScalarExpr", ->
    it "leaves valid one alone", ->
      fieldExpr = { type: "field", table: "t2", column: "integer" }
      scalarExpr = { type: "scalar", table: "t1", joins: ['1-2'], expr: fieldExpr, aggr: "sum" }

      assert.equal scalarExpr, @exprCleaner.cleanScalarExpr(scalarExpr)

    it "strips aggr if not needed", ->
      fieldExpr = { type: "field", table: "t2", column: "integer" }
      scalarExpr = { type: "scalar", table: "t1", joins: [], expr: fieldExpr, aggr: "sum" }
      scalarExpr = @exprCleaner.cleanScalarExpr(scalarExpr)
      assert not scalarExpr.aggr

    it "defaults aggr if needed and wrong", ->
      fieldExpr = { type: "field", table: "t2", column: "text" }
      scalarExpr = { type: "scalar", table: "t1", joins: ['1-2'], expr: fieldExpr, aggr: "sum" }
      scalarExpr = @exprCleaner.cleanScalarExpr(scalarExpr)
      assert.equal scalarExpr.aggr, "last"

    it "strips where if wrong table", ->
      fieldExpr = { type: "field", table: "t2", column: "integer" }
      whereExpr = { type: "logical", table: "t1" }
      scalarExpr = { type: "scalar", table: "t1", joins: ['1-2'], expr: fieldExpr, aggr: "sum" }
      scalarExpr = @exprCleaner.cleanScalarExpr(scalarExpr)
      assert.equal scalarExpr.aggr, "sum"
      assert not scalarExpr.where

    it "strips if invalid join", ->
      fieldExpr = { type: "field", table: "t2", column: "integer" }
      scalarExpr = { type: "scalar", table: "t1", joins: ['xyz'], expr: fieldExpr, aggr: "sum" }
      scalarExpr = @exprCleaner.cleanScalarExpr(scalarExpr)
      assert not scalarExpr

  describe "cleanComparisonExpr", ->
    it "removes op if no lhs", ->
      expr = { type: "comparison", op: "=" }
      expr = @exprCleaner.cleanComparisonExpr(expr)
      assert not expr.op

    it "removes rhs if wrong type", ->
      expr = { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "text" }, op: "~*", rhs: { type: "literal", valueType: "text", value: "x" } }
      expr = @exprCleaner.cleanComparisonExpr(expr)
      assert expr.rhs, "should keep"

      expr = { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "text" }, op: "~*", rhs: { type: "literal", valueType: "integer", value: 3 } }
      expr = @exprCleaner.cleanComparisonExpr(expr)
      assert not expr.rhs, "should remove"

    it "removes rhs if invalid enum", ->
      expr = { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "enum" }, op: "=", rhs: { type: "literal", valueType: "enum", value: "a" } }
      expr = @exprCleaner.cleanComparisonExpr(expr)
      assert expr.rhs, "should keep"

      expr = { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "enum" }, op: "=", rhs: { type: "literal", valueType: "enum", value: "x" } }
      expr = @exprCleaner.cleanComparisonExpr(expr)
      assert not expr.rhs

    it "removes rhs if empty enum[]", ->
      expr = { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "enum" }, op: "= any", rhs: { type: "literal", valueType: "enum[]", value: ['a'] } }
      expr = @exprCleaner.cleanComparisonExpr(expr)
      assert expr.rhs, "should keep"

      expr = { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "enum" }, op: "= any", rhs: { type: "literal", valueType: "enum[]", value: [] } }
      expr = @exprCleaner.cleanComparisonExpr(expr)
      assert not expr.rhs

    it "defaults op", ->
      expr = { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "text" } }
      expr = @exprCleaner.cleanComparisonExpr(expr)
      assert.equal expr.op, "= any"

    it "removes invalid op", ->
      expr = { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "text" }, op: ">" }
      expr = @exprCleaner.cleanComparisonExpr(expr)
      assert.equal expr.op, "= any"
