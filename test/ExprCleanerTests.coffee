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
    @clean = (expr, expected, options) =>
      compare(@exprCleaner.cleanExpr(expr, options), expected)

  it "nulls if wrong table", ->
    assert.isNull @exprCleaner.cleanExpr({ type: "field", table: "t1", column: "text" }, table: "t2")

  it "preserves 'and' by cleaning child expressions with boolean type", ->
    expr = { type: "op", op: "and", table: "t1", exprs: [{ type: "field", table: "t1", column: "text" }, { type: "field", table: "t1", column: "boolean" }]}

    compare(@exprCleaner.cleanExpr(expr), {
      type: "op"
      op: "and"
      table: "t1"
      exprs: [
        # Removed
        null
        # Untouched
        { type: "field", table: "t1", column: "boolean" }
      ]})

  it "simplifies and", ->
    expr = { type: "op", op: "and", table: "t1", exprs: [{ type: "field", table: "t1", column: "boolean" }]}
    compare(@exprCleaner.cleanExpr(expr), { type: "field", table: "t1", column: "boolean" })

    expr = { type: "op", op: "and", table: "t1", exprs: []}
    compare(@exprCleaner.cleanExpr(expr), null)

  it "cleans invalid literal enum valueIds", ->
    expr = { type: "literal", valueType: "enum", value: "a" }
    compare(@exprCleaner.cleanExpr(expr, valueIds: ["a", "b"]), expr)
    compare(@exprCleaner.cleanExpr(expr, valueIds: ["b"]), null)
    compare(@exprCleaner.cleanExpr(expr, valueIds: ["a", "b", "c"]), expr)

  it "cleans invalid field enum valueIds", ->
    expr = { type: "field", table: "t1", column: "enum" }
    compare(@exprCleaner.cleanExpr(expr, valueIds: ["a", "b"]), expr)
    compare(@exprCleaner.cleanExpr(expr, valueIds: ["b"]), null)

  it "allows empty 'and' children", ->
    expr = { type: "op", op: "and", table: "t1", exprs: [{}, {}]}
    compare(@exprCleaner.cleanExpr(expr), expr)

  describe "boolean required", ->
    before ->
      @clean = (before, afterExpected) ->
        after = @exprCleaner.cleanExpr(before, type: "boolean")
        compare(after, afterExpected)

    it "strips enum", ->
      field = { type: "field", table: "t1", column: "enum" }
      @clean(
        field
        null
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

    it "simplifies if no joins", ->
      fieldExpr = { type: "field", table: "t1", column: "number" }
      scalarExpr = { type: "scalar", table: "t1", joins: [], expr: fieldExpr }
      scalarExpr = @exprCleaner.cleanExpr(scalarExpr)
      compare(fieldExpr, scalarExpr)

  # Version 1 expression should be upgraded to version 2
  describe "upgrade", ->
    it "scalar is simplified", ->
      @clean(
        { type: "scalar", table: "t1", joins: [], expr: { type: "field", table: "t1", column: "number" } }
        { type: "field", table: "t1", column: "number" }
      )

    it "logical becomes op", ->
      @clean(
        { type: "logical", op: "and", table: "t1", exprs: [{ type: "field", table: "t1", column: "boolean" }, { type: "field", table: "t1", column: "boolean" }] }
        { type: "op", op: "and", table: "t1", exprs: [{ type: "field", table: "t1", column: "boolean" }, { type: "field", table: "t1", column: "boolean" }] }        
      )

    it "comparison becomes op", ->
      @clean(
        { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "text" }, op: "~*", rhs: { type: "literal", valueType: "text", value: "x" } }
        { type: "op", op: "~*", table: "t1", exprs: [{ type: "field", table: "t1", column: "text" }, { type: "literal", valueType: "text", value: "x" }] }        
      )

    it "= true is simplified", ->
      @clean(
        { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "boolean" }, op: "= true" }
        { type: "field", table: "t1", column: "boolean" }
      )

    it "= false becomes 'not'", ->
      @clean(
        { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "boolean" }, op: "= false" }
        { type: "op", op: "not", table: "t1", exprs: [{ type: "field", table: "t1", column: "boolean" }] }
      )

    it "between becomes 3 parameters date", ->
      @clean(
        { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "date" }, op: "between", rhs: { type: "literal", valueType: "daterange", value: ["2014-01-01", "2014-12-31"] } }
        { 
          type: "op"
          op: "between"
          table: "t1"
          exprs: [
            { type: "field", table: "t1", column: "date" }            
            { type: "literal", valueType: "date", value: "2014-01-01" }
            { type: "literal", valueType: "date", value: "2014-12-31" }
          ]
        }
      )

    it "between becomes 3 parameters datetime", ->
      @clean(
        { type: "comparison", table: "t1", lhs: { type: "field", table: "t1", column: "datetime" }, op: "between", rhs: { type: "literal", valueType: "datetimerange", value: ["2014-01-01", "2014-12-31"] } }
        { 
          type: "op"
          op: "between"
          table: "t1"
          exprs: [
            { type: "field", table: "t1", column: "datetime" }
            { type: "literal", valueType: "datetime", value: "2014-01-01" }
            { type: "literal", valueType: "datetime", value: "2014-12-31" }
          ]
        }
      )

    it "between becomes 3 parameters date if were datetime on date", ->
      @clean(
        { 
          type: "comparison"
          table: "t1"
          lhs: { type: "field", table: "t1", column: "date" }
          op: "between"
          rhs: { type: "literal", valueType: "datetimerange", value: ["2014-01-01T01:02:04", "2014-12-31T01:02:04"] } }
        { 
          type: "op"
          op: "between"
          table: "t1"
          exprs: [
            { type: "field", table: "t1", column: "date" }
            { type: "literal", valueType: "date", value: "2014-01-01" }
            { type: "literal", valueType: "date", value: "2014-12-31" }
          ]
        }
      )
