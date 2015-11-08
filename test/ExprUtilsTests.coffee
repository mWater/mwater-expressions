assert = require('chai').assert
fixtures = require './fixtures'
_ = require 'lodash'

ExprUtils = require '../src/ExprUtils'
Schema = require '../src/Schema'

describe "ExprUtils", ->
  beforeEach ->
    @exprUtils = new ExprUtils(fixtures.simpleSchema())

  it "determines if multiple joins", ->
    assert.isTrue @exprUtils.isMultipleJoins("t1", ["1-2"])
    assert.isFalse @exprUtils.isMultipleJoins("t2", ["2-1"])

  it "follows joins", ->
    assert.equal @exprUtils.followJoins("t1", []), "t1"
    assert.equal @exprUtils.followJoins("t1", ["1-2"]), "t2"

  describe "getAggrs", ->
    beforeEach ->
      @schema = new Schema()
        .addTable({ id: "a", name: "A", ordering: "z", contents: [
          { id: "y", name: "Y", type: "text" }
          { id: "z", name: "Z", type: "number" }
          ]})
      @exprUtils = new ExprUtils(@schema)

    it "includes last if has natural ordering", ->
      field = { type: "field", table: "a", column: "y" }
      assert.equal _.findWhere(@exprUtils.getAggrs(field), id: "last").type, "text"

    it "doesn't include most recent normally", ->
      @schema = @schema.addTable({ id: "b", name: "B", contents:[{ id: "x", name: "X", type: "text" }]})
      @exprUtils = new ExprUtils(@schema)
      field = { type: "field", table: "b", column: "x" }
      assert.isUndefined _.findWhere(@exprUtils.getAggrs(field), id: "last")

    it "includes count for text", ->
      field = { type: "field", table: "a", column: "y" }
      aggrs = @exprUtils.getAggrs(field)

      assert.equal _.findWhere(aggrs, id: "count").type, "number"
      assert.isUndefined _.findWhere(aggrs, id: "sum")
      assert.isUndefined _.findWhere(aggrs, id: "avg")

    it "includes sum, avg, etc for number", ->
      field = { type: "field", table: "a", column: "z" }
      aggrs = @exprUtils.getAggrs(field)

      assert.equal _.findWhere(aggrs, id: "sum").type, "number"
      assert.equal _.findWhere(aggrs, id: "avg").type, "number"
      # TODO etc

    it "includes nothing for null", ->
      aggrs = @exprUtils.getAggrs(null)
      assert.equal aggrs.length, 0

    it "includes only count for count type", ->
      count = { type: "count", table: "a" }
      aggrs = @exprUtils.getAggrs(count)

      assert.equal _.findWhere(aggrs, id: "count").type, "number"
      assert.equal aggrs.length, 1
  
  describe "getExprType", ->
    it 'gets field type', ->
      assert.equal @exprUtils.getExprType({ type: "field", table: "t1", column: "text" }), "text"

    it 'gets scalar type', ->
      expr = {
        type: "scalar"
        table: "t1"
        expr: { type: "field", table: "t1", column: "text" }
        joins: []
      }
      assert.equal @exprUtils.getExprType(expr), "text"

    it 'gets scalar type with aggr', ->
      expr = {
        type: "scalar"
        table: "t1"
        expr: { type: "field", table: "t2", column: "number" }
        aggr: "avg"
        joins: ["1-2"]
      }
      assert.equal @exprUtils.getExprType(expr), "number"

    it 'gets scalar type with count', ->
      expr = {
        type: "scalar"
        table: "t1"
        expr: { type: "count", table: "t2" }
        aggr: "count"
        joins: ["1-2"]
      }
      assert.equal @exprUtils.getExprType(expr), "number"

    it "gets literal types", ->
      assert.equal @exprUtils.getExprType({ type: "literal", valueType: "boolean", value: true }), "boolean"

    it "gets boolean type for and/or", ->
      assert.equal @exprUtils.getExprType({ type: "op", op: "and", exprs: [] }), "boolean"
      assert.equal @exprUtils.getExprType({ type: "op", op: "or", exprs: [] }), "boolean"

    it "gets boolean type for =", ->
      assert.equal @exprUtils.getExprType({ type: "op", op: "=", exprs: [] }), "boolean"

    it "no type for {}", ->
      assert.isNull @exprUtils.getExprType({})

    it "number type if number + number", ->
      assert.equal @exprUtils.getExprType({ type: "op", op: "+", exprs: [{ type: "field", table: "t1", column: "number" }, { type: "field", table: "t1", column: "number" }]}), "number"

  describe "summarizeExpr", ->
    it "summarizes null", ->
      assert.equal @exprUtils.summarizeExpr(null), "None"

    it "summarizes field expr", ->
      expr = { type: "field", table: "t1", column: "number" }
      assert.equal @exprUtils.summarizeExpr(expr), "Number"

    it "summarizes simple scalar expr", ->
      fieldExpr = { type: "field", table: "t1", column: "number" }
      scalarExpr = { type: "scalar", table: "t1", joins: [], expr: fieldExpr }
      assert.equal @exprUtils.summarizeExpr(scalarExpr), "Number"

    it "summarizes joined scalar expr", ->
      fieldExpr = { type: "field", table: "t2", column: "number" }
      scalarExpr = { type: "scalar", table: "t1", joins: ['1-2'], expr: fieldExpr }
      assert.equal @exprUtils.summarizeExpr(scalarExpr), "T1->T2 > Number"

    it "summarizes joined aggr scalar expr", ->
      fieldExpr = { type: "field", table: "t2", column: "number" }
      scalarExpr = { type: "scalar", table: "t1", joins: ['1-2'], expr: fieldExpr, aggr: "sum" }
      assert.equal @exprUtils.summarizeExpr(scalarExpr), "Total T1->T2 > Number"

    it "simplifies when count", ->
      scalarExpr = { type: "scalar", table: "t1", joins: ['1-2'], expr: { type: "count", table: "t2" }, aggr: "count" }
      assert.equal @exprUtils.summarizeExpr(scalarExpr), "Number of T1->T2"

    # TODO readd
    # it "uses named expression when matching one present", ->
    #   # Add named expression
    #   @schema.addNamedExpr("t1", { id: "number", name: "NE Column 1", expr: { type: "field", table: "t1", column: "number" }})

    #   # Test with scalar that can simplify
    #   expr = {
    #     type: "scalar"
    #     table: "t1"
    #     expr: { type: "field", table: "t1", column: "number" }
    #     joins: []
    #   }
    #   assert.equal @exprUtils.summarizeExpr(expr), "NE Column 1"

  describe "summarizeAggrExpr", ->
    it "summarizes null", ->
      assert.equal @exprUtils.summarizeAggrExpr(null), "None"

    it "summarizes field expr", ->
      expr = { type: "field", table: "t1", column: "number" }
      assert.equal @exprUtils.summarizeAggrExpr(expr), "Number"

    it "summarizes field expr", ->
      expr = { type: "field", table: "t2", column: "number" }
      assert.equal @exprUtils.summarizeAggrExpr(expr, "sum"), "Total Number"

    it "simplifies when count", ->
      scalarExpr = { type: "scalar", table: "t1", joins: [], expr: { type: "count", table: "t1" } }
      assert.equal @exprUtils.summarizeAggrExpr(scalarExpr, "count"), "Number of T1"

  describe "stringifyExprLiteral", ->
    it "stringifies number", ->
      str = @exprUtils.stringifyExprLiteral({ type: "field", table: "t1", column: "number" }, 2.34)
      assert.equal str, "2.34"

    it "stringifies null", ->
      str = @exprUtils.stringifyExprLiteral({ type: "field", table: "t1", column: "number" }, null)
      assert.equal str, "None"

    it "looks up enum", ->
      str = @exprUtils.stringifyExprLiteral({ type: "field", table: "t1", column: "enum" }, "a")
      assert.equal str, "A"

    it "handles null enum", ->
      str = @exprUtils.stringifyExprLiteral({ type: "field", table: "t1", column: "enum" }, null)
      assert.equal str, "None"

    it "handles invalid enum", ->
      str = @exprUtils.stringifyExprLiteral({ type: "field", table: "t1", column: "enum" }, "xyz")
      assert.equal str, "???"
