assert = require('chai').assert
_ = require 'lodash'
canonical = require 'canonical-json'
moment = require 'moment'
sinon = require 'sinon'

ExprEvaluator = require '../src/ExprEvaluator'

compare = (actual, expected) ->
  assert.equal canonical(actual), canonical(expected), "\ngot:" + canonical(actual) + "\nexp:" + canonical(expected) + "\n"
 
describe "ExprEvaluator", ->
  beforeEach ->
    @ev = new ExprEvaluator()
    @check = (expr, rowObj, expected) =>
      # Fake row
      row = {
        getPrimaryKey: -> "id"
        getField: (columnId) -> rowObj[columnId]
      }

      result = @ev.evaluate(expr, row)
      if _.isFunction(expected)
        expected(result)
      else
        compare(result, expected)

  it "compiles field", ->
    @check(
      { type: "field", table: "t1", column: "number" }
      { number: 4 }
    4)

  it "literal", ->
    @check(
      { type: "literal", valueType: "number", value: 4 }
      {}
      4)

  describe "ops", ->
    before ->
      @testOp = (op, exprs, result) =>
        @check(
          { type: "op", op: op, exprs: [{ type: "field", table: "t1", column: "x" }, { type: "field", table: "t1", column: "y" }, { type: "field", table: "t1", column: "z" }] }
          { x: exprs[0], y: exprs[1], z: exprs[2] }
          result)

    it "and", ->
      @testOp("and", [true, false], false)
      @testOp("and", [true, true], true)

    it "or", ->
      @testOp("or", [true, false], true)

    it "=", ->
      @testOp("=", [1, 2], false)
      @testOp("=", [2, 2], true)

    it ">", ->
      @testOp(">", [2, 2], false)
      @testOp(">", [2, 1], true)

    it ">=", ->
      @testOp(">=", [2, 2], true)
      @testOp(">=", [2, 1], true)

    it "<", ->
      @testOp("<", [2, 2], false)
      @testOp("<", [1, 2], true)

    it "<=", ->
      @testOp("<=", [2, 2], true)
      @testOp("<=", [1, 2], true)

    it "<>", ->
      @testOp("<>", [2, 2], false)
      @testOp("<>", [1, 2], true)

    it "~*", ->
      @testOp("~*", ["abc", "ab"], true)
      @testOp("~*", ["ABC", "ab"], true)
      @testOp("~*", ["C", "ab"], false)

    it "= false", ->
      @testOp("= false", [false], true)
      @testOp("= false", [true], false)

    it "is null", ->
      @testOp("is null", [false], false)
      @testOp("is null", [null], true)

    it "is not null", ->
      @testOp("is not null", [false], true)
      @testOp("is not null", [null], false)

    it "= any", ->
      @testOp("= any", ["a", ["a", "b"]], true)
      @testOp("= any", ["a", ["c", "b"]], false)

    it "between", ->
      @testOp("between", [3, 2, 4], true)

    # it "contains", ->
    # Not supported

    describe "relative dates", ->
      before ->
        @clock = sinon.useFakeTimers(new Date().getTime())

      after ->
        @clock.restore()

      it "thisyear", ->
        @testOp("thisyear", [moment().format("YYYY-MM-DD")], true)
        @testOp("thisyear", [moment().add(1, "years").format("YYYY-MM-DD")], false)

      it "lastyear", ->
        @testOp("lastyear", [moment().format("YYYY-MM-DD")], false)
        @testOp("lastyear", [moment().subtract(1, "years").format("YYYY-MM-DD")], true)

      it "thismonth", ->
        @testOp("thismonth", [moment().format("YYYY-MM-DD")], true)
        @testOp("thismonth", [moment().add(1, "years").format("YYYY-MM-DD")], false)

      it "lastmonth", ->
        @testOp("lastmonth", [moment().format("YYYY-MM-DD")], false)
        @testOp("lastmonth", [moment().subtract(1, "months").format("YYYY-MM-DD")], true)

      it "today", ->
        @testOp("today", [moment().format("YYYY-MM-DD")], true)
        @testOp("today", [moment().add(1, "years").format("YYYY-MM-DD")], false)

      it "yesterday", ->
        @testOp("yesterday", [moment().format("YYYY-MM-DD")], false)
        @testOp("yesterday", [moment().subtract(1, "days").format("YYYY-MM-DD")], true)

      it "last7days", ->
        @testOp("last7days", [moment().add(1, "days")], false)
        @testOp("last7days", [moment().subtract(1, "days").format("YYYY-MM-DD")], true)

      it "last30days", ->
        @testOp("last30days", [moment().add(1, "days")], false)
        @testOp("last30days", [moment().subtract(1, "days").format("YYYY-MM-DD")], true)

      it "last365days", ->
        @testOp("last365days", [moment().add(1, "days")], false)
        @testOp("last365days", [moment().subtract(1, "days").format("YYYY-MM-DD")], true)

    describe "relative datetimes", ->
      before ->
        @clock = sinon.useFakeTimers(new Date().getTime())

      after ->
        @clock.restore()

      it "thisyear", ->
        @testOp("thisyear", [new Date().toISOString()], true)
        @testOp("thisyear", [moment().add(1, "years").toISOString()], false)

      it "lastyear", ->
        @testOp("lastyear", [new Date().toISOString()], false)
        @testOp("lastyear", [moment().subtract(1, "years").toISOString()], true)

      it "thismonth", ->
        @testOp("thismonth", [new Date().toISOString()], true)
        @testOp("thismonth", [moment().add(1, "years").toISOString()], false)

      it "lastmonth", ->
        @testOp("lastmonth", [new Date().toISOString()], false)
        @testOp("lastmonth", [moment().subtract(1, "months").toISOString()], true)

      it "today", ->
        @testOp("today", [new Date().toISOString()], true)
        @testOp("today", [moment().add(1, "years").toISOString()], false)

      it "yesterday", ->
        @testOp("yesterday", [new Date().toISOString()], false)
        @testOp("yesterday", [moment().subtract(1, "days").toISOString()], true)

      it "last7days", ->
        @testOp("last7days", [moment().add(1, "days")], false)
        @testOp("last7days", [moment().subtract(1, "days").toISOString()], true)

      it "last30days", ->
        @testOp("last30days", [moment().add(1, "days")], false)
        @testOp("last30days", [moment().subtract(1, "days").toISOString()], true)

      it "last365days", ->
        @testOp("last365days", [moment().add(1, "days")], false)
        @testOp("last365days", [moment().subtract(1, "days").toISOString()], true)

    it 'latitude', ->
      @testOp("latitude", [{ type: "Point", coordinates: [1, 2]}], 2) 

    it 'longitude', ->
      @testOp("longitude", [{ type: "Point", coordinates: [1, 2]}], 1) 

    it 'distance', ->
      @testOp("distance", [{ type: "Point", coordinates: [1, 2]}, { type: "Point", coordinates: [1, 2]}], 0) 
      @testOp("distance", [{ type: "Point", coordinates: [1, 2]}, { type: "Point", coordinates: [3, 4]}], (val) -> assert(val > 310000 and val < 320000) )

  describe "case", ->
    it "simple", ->
      expr = { 
        type: "case"
        cases: [
          { 
            when: { type: "field", table: "t1", column: "x" }
            then: { type: "literal", valueType: "number", value: 1 }
          }
          { 
            when: { type: "field", table: "t1", column: "y" }
            then: { type: "literal", valueType: "number", value: 2 }
          }
        ]
        else: { type: "literal", valueType: "number", value: 3 }
      }

      @check(expr, { x: true }, 1)
      @check(expr, { y: true }, 2)
      @check(expr, { }, 3)

  describe "score", ->
    it "does enum", ->
      expr = {
        type: "score"
        input: { type: "field", table: "t1", column: "x" }
        scores: {
          a: { type: "literal", valueType: "number", value: 3 }
          b: { type: "literal", valueType: "number", value: 4 }
        }
      }
      @check(expr, { x: "a" }, 3)
      @check(expr, { x: "c" }, 0)

    it "does enumset", ->
      expr = {
        type: "score"
        input: { type: "field", table: "t1", column: "x" }
        scores: {
          a: { type: "literal", valueType: "number", value: 3 }
          b: { type: "literal", valueType: "number", value: 4 }
        }
      }
      @check(expr, { x: ["a", "b"] }, 7)
      @check(expr, { x: ["a", "c"] }, 3)
      @check(expr, { x: null }, 0)

  describe "scalar", ->
    it "n-1 scalar", ->
      @check({ type: "scalar", joins: ['x'], expr: { type: "field", table: "t2", column: "y" }}, { x: { getField: (col) -> (if col == "y" then 4) }}, 4)

    it "n-1 null scalar", ->
      @check({ type: "scalar", joins: ['x'], expr: { type: "field", table: "t2", column: "y" }}, { x: null }, null)
