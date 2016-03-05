assert = require('chai').assert
_ = require 'lodash'
Schema = require '../src/Schema'
ExprCleaner = require '../src/ExprCleaner'
fixtures = require './fixtures'

canonical = require 'canonical-json'

compare = (actual, expected) ->
  assert.equal canonical(actual), canonical(expected), "\n" + canonical(actual) + "\n" + canonical(expected) + "\n"

describe "ExprCleaner", ->
  beforeEach ->
    @schema = fixtures.simpleSchema()
    @exprCleaner = new ExprCleaner(@schema)
    @clean = (expr, expected, options) =>
      compare(@exprCleaner.cleanExpr(expr, options), expected)

  it "nulls if wrong table", ->
    assert.isNull @exprCleaner.cleanExpr({ type: "field", table: "t1", column: "text" }, table: "t2")

  it "nulls if wrong type", ->
    field = { type: "field", table: "t1", column: "enum" }
    assert.isNull @exprCleaner.cleanExpr(field, types: ["boolean"])

  it "nulls if wrong idTable", ->
    field = { type: "id", table: "t1" }
    assert @exprCleaner.cleanExpr(field, types: ["id"], idTable: "t1")
    assert.isNull @exprCleaner.cleanExpr(field, types: ["id"], idTable: "t2")

  describe "op", ->
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

    it "allows empty 'and' children", ->
      expr = { type: "op", op: "and", table: "t1", exprs: [{}, {}]}
      compare(@exprCleaner.cleanExpr(expr), expr)

    it "allows empty '+' children", ->
      expr = { type: "op", op: "+", table: "t1", exprs: [{}, {}]}
      compare(@exprCleaner.cleanExpr(expr), expr)

    it "nulls if wrong type", ->
      expr = { type: "op", op: "and", table: "t1", exprs: [{}, {}]}
      compare(@exprCleaner.cleanExpr(expr, types: ["number"]), null)

    it "nulls if missing lhs of non-+/*/and/or expr", ->
      expr = { type: "op", op: "= any", table: "t1", exprs: [null, {}]}
      compare(@exprCleaner.cleanExpr(expr), null)

      expr = { type: "op", op: "=", table: "t1", exprs: [null, {}]}
      compare(@exprCleaner.cleanExpr(expr), null)

      expr = { type: "op", op: "=", table: "t1", exprs: [null, null]}
      compare(@exprCleaner.cleanExpr(expr), null)

    it "does not allow enum = enumset", ->
      expr = { type: "op", op: "=", table: "t1", exprs: [{ type: "field", table: "t1", column: "enum" }, { type: "literal", valueType: "enumset", value: ["a"] }]}
      compare(@exprCleaner.cleanExpr(expr).exprs[1], null)

    it "defaults op if lhs changes", ->
      expr = { type: "op", op: "= any", table: "t1", exprs: [{ type: "field", table: "t1", column: "number" }, { type: "literal", valueType: "enumset", value: ["a"] }]}
      compare(@exprCleaner.cleanExpr(expr), { type: "op", op: "=", table: "t1", exprs: [{ type: "field", table: "t1", column: "number" }, null]})

    it "removes extra exprs", ->
      expr = { type: "op", op: "=", table: "t1", exprs: [{ type: "field", table: "t1", column: "number" }, null, null]}
      compare(@exprCleaner.cleanExpr(expr), { type: "op", op: "=", table: "t1", exprs: [{ type: "field", table: "t1", column: "number" }, null]})

    it "adds missing exprs", ->
      expr = { type: "op", op: "=", table: "t1", exprs: [{ type: "field", table: "t1", column: "number" }]}
      compare(@exprCleaner.cleanExpr(expr), { type: "op", op: "=", table: "t1", exprs: [{ type: "field", table: "t1", column: "number" }, null]})

    it "allows null=wildcard unary expressions", ->
      expr = { type: "op", op: "is null", table: "t1", exprs: [{ type: "field", table: "t1", column: "number" }, null]}
      compare(@exprCleaner.cleanExpr(expr), { type: "op", op: "is null", table: "t1", exprs: [{ type: "field", table: "t1", column: "number" }]})

    it "removes invalid enums on rhs", ->
      expr = { type: "op", op: "= any", table: "t1", exprs: [{ type: "field", table: "t1", column: "enum" }, { type: "literal", valueType: "enumset", value: ["a", "x"] }]}
      compare(@exprCleaner.cleanExpr(expr), { type: "op", op: "= any", table: "t1", exprs: [{ type: "field", table: "t1", column: "enum" }, { type: "literal", valueType: "enumset", value: ["a"] }]}) # x is gone

    it "removes invalid id table on rhs", ->
      expr = { type: "op", op: "=", table: "t1", exprs: [{ type: "id", table: "t1" }, { type: "literal", valueType: "id", idTable: "t1", value: "123" }]}
      compare(@exprCleaner.cleanExpr(expr), expr)

      debugger
      expr = { type: "op", op: "=", table: "t1", exprs: [{ type: "id", table: "t1" }, { type: "literal", valueType: "id", idTable: "t2", value: "123" }]}
      compare(@exprCleaner.cleanExpr(expr), { type: "op", op: "=", table: "t1", exprs: [{ type: "id", table: "t1" }, null]})

  describe "case", ->
    it "cleans else", ->
      expr = { 
        type: "case"
        table: "t1"
        cases: [{ when: { type: "literal", valueType: "boolean", value: true }, then: { type: "literal", valueType: "number", value: 123 }}]
        else: { type: "literal", valueType: "text", value: "abc" }
      }
      compare(@exprCleaner.cleanExpr(expr, types: ["number"]), 
        {
          type: "case"
          table: "t1"
          cases: [{ when: { type: "literal", valueType: "boolean", value: true }, then: { type: "literal", valueType: "number", value: 123 }}]
          else: null
        })

    it "cleans whens as booleans", ->
      expr = { 
        type: "case"
        table: "t1"
        cases: [{ when: { type: "literal", valueType: "number", value: 123 }, then: { type: "literal", valueType: "number", value: 123 }}]
      }
      compare(@exprCleaner.cleanExpr(expr, types: ["number"]), 
        {
          type: "case"
          table: "t1"
          cases: [{ when: null, then: { type: "literal", valueType: "number", value: 123 }}]
          else: null
        })

    it "simplifies if no cases", ->
      expr = { 
        type: "case"
        table: "t1"
        cases: []
        else: { type: "literal", valueType: "text", value: "abc" }
      }
      compare(@exprCleaner.cleanExpr(expr, types: ["text"]), 
        { type: "literal", valueType: "text", value: "abc" })

    it "cleans thens as specified type", ->
      expr = { 
        type: "case"
        table: "t1"
        cases: [{ when: { type: "literal", valueType: "boolean", value: true }, then: { type: "literal", valueType: "number", value: 123 }}]
        else: null
      }
      compare(@exprCleaner.cleanExpr(expr, types: ["text"]), 
        {
          type: "case"
          table: "t1"
          cases: [{ when: { type: "literal", valueType: "boolean", value: true }, then: null }]
          else: null
        })

    it "cleans thens as specified enumValueIds", ->
      expr = { 
        type: "case"
        table: "t1"
        cases: [{ when: { type: "literal", valueType: "boolean", value: true }, then: { type: "literal", valueType: "enum", value: "x" }}]
        else: null
      }
      compare(@exprCleaner.cleanExpr(expr, types: ["enum"], enumValueIds: ['a']), 
        {
          type: "case"
          table: "t1"
          cases: [{ when: { type: "literal", valueType: "boolean", value: true }, then: null }]
          else: null
        })

  describe "literal", ->
    it "cleans invalid literal enum valueIds", ->
      expr = { type: "literal", valueType: "enum", value: "a" }
      compare(@exprCleaner.cleanExpr(expr, enumValueIds: ["a", "b"]), expr)
      compare(@exprCleaner.cleanExpr(expr, enumValueIds: ["b"]), null)
      compare(@exprCleaner.cleanExpr(expr, enumValueIds: ["a", "b", "c"]), expr)

    it "cleans invalid field enum valueIds", ->
      expr = { type: "field", table: "t1", column: "enum" }
      compare(@exprCleaner.cleanExpr(expr, enumValueIds: ["a", "b"]), expr)
      compare(@exprCleaner.cleanExpr(expr, enumValueIds: ["b"]), null)

  describe "scalar", ->
    it "leaves valid one alone", ->
      fieldExpr = { type: "field", table: "t2", column: "number" }
      scalarExpr = { type: "scalar", table: "t1", joins: ['1-2'], expr: fieldExpr, aggr: "sum" }

      compare(scalarExpr, @exprCleaner.cleanExpr(scalarExpr))

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

    it "removes aggr from null expr", ->
      scalarExpr = { type: "scalar", table: "t1", joins: ['1-2'], expr: null, aggr: "sum" }
      scalarExpr = @exprCleaner.cleanExpr(scalarExpr)
      assert not scalarExpr.aggr

  # Version 1 expression should be upgraded to version 2
  describe "upgrade", ->
    it "count becomes id", ->
      @clean(
        { type: "scalar", table: "t1", aggr: "count", joins: ["1-2"], expr: { type: "count", table: "t2" } }
        { type: "scalar", table: "t1", aggr: "count", joins: ["1-2"], expr: { type: "id", table: "t2" } }
      )

    it "scalar count becomes id", ->
      @clean(
        { type: "scalar", table: "t1", expr: { type: "count", table: "t1" }, joins: [] }
        { type: "id", table: "t1" }
      )

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

    it "enum[] becomes enumset", ->
      @clean(
        { type: "literal", valueType: "enum[]", value: ["a", "b"] }
        { type: "literal", valueType: "enumset", value: ["a", "b"] }
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

    it "upgrades legacy entity join references", ->
      schema = @schema.addTable({
        id: "entities.wwmc_visit"
        contents: [
          { id: "site", type: "join", join: { type: "n-1", toTable: "entities.surface_water" } }
        ]
        })

      schema = schema.addTable({
        id: "entities.surface_water"
        contents: [
          { id: "location", type: "geometry" }
        ]
        })

      exprCleaner = new ExprCleaner(schema)

      clean = (expr, expected, options) =>
        compare(exprCleaner.cleanExpr(expr, options), expected)

      clean(
        { 
          type: "scalar"
          expr: { type: "field", table: "entities.surface_water", column: "location" }
          joins: ["entities.wwmc_visit.site"]
          table: "entities.wwmc_visit"
        }
        {
          type: "scalar"
          expr: { type: "field", table: "entities.surface_water", column: "location" }
          joins: ["site"]
          table: "entities.wwmc_visit"
        }
        )

    it "upgrades complex expression with legacy literals", ->
      expr1 = { type: "comparison", table: "t1", op: "=", lhs: { type: "field", table: "t1", column: "number" }, rhs: { type: "literal", valueType: "integer", value: 4 } }
      expr2 = { type: "comparison", table: "t1", op: "=", lhs: { type: "field", table: "t1", column: "number" }, rhs: { type: "literal", valueType: "integer", value: 5 } }
      value = { type: "logical", table: "t1", op: "and", exprs: [expr1, expr2] }      
      debugger
      @clean(
        value,
        { 
          type: "op"
          op: "and"
          table: "t1"
          exprs: [
            { 
              type: "op"
              table: "t1"
              op: "="
              exprs: [
                { type: "field", table: "t1", column: "number" }
                { type: "literal", valueType: "number", value: 4 }
              ]
            }
            { 
              type: "op"
              table: "t1"
              op: "="
              exprs: [
                { type: "field", table: "t1", column: "number" }
                { type: "literal", valueType: "number", value: 5 }
              ]
            }
          ]
        }
      )

