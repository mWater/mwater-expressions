assert = require('chai').assert
_ = require 'lodash'
Schema = require '../src/Schema'
ExprCleaner = require '../src/ExprCleaner'
fixtures = require './fixtures'

canonical = require 'canonical-json'

compare = (actual, expected) ->
  assert.equal canonical(actual), canonical(expected), "\ngot: " + canonical(actual) + "\nexp: " + canonical(expected) + "\n"

variables = [
  { id: "varenum", name: { _base: "en", en: "Varenum" }, type: "enum", enumValues: [{ id: "a", name: { en: "A" }}, { id: "b", name: { en: "B" }}] }
  { id: "varnumber", name: { _base: "en", en: "Varnumber" }, type: "number" }
  { id: "varnumberexpr", name: { _base: "en", en: "Varnumberexpr" }, type: "number", table: "t1" }
  { id: "varid", name: { _base: "en", en: "Varid" }, type: "id", idTable: "t1" }
]

describe "ExprCleaner", ->
  beforeEach ->
    @schema = fixtures.simpleSchema()
    @exprCleaner = new ExprCleaner(@schema, variables)
    @clean = (expr, expected, options) =>
      compare(@exprCleaner.cleanExpr(expr, options), expected)

  describe "cleanExpr", ->
    it "nulls if wrong table", ->
      assert.isNull @exprCleaner.cleanExpr({ type: "field", table: "t1", column: "text" }, table: "t2")

    it "nulls if wrong type", ->
      field = { type: "field", table: "t1", column: "enum" }
      assert.isNull @exprCleaner.cleanExpr(field, types: ["text"])

    it "nulls if wrong idTable", ->
      field = { type: "id", table: "t1" }
      assert @exprCleaner.cleanExpr(field, types: ["id"], idTable: "t1")
      assert.isNull @exprCleaner.cleanExpr(field, types: ["id"], idTable: "t2")

    it "nulls if wrong enums", ->
      field = { type: "field", table: "t1", column: "enum" }
      assert.isNotNull @exprCleaner.cleanExpr(field, enumValueIds: ["a", "b", "c"])
      assert.isNull @exprCleaner.cleanExpr(field, enumValueIds: ["a"])

    it "nulls if wrong enums expression", ->
      field = { type: "field", table: "t1", column: "expr_enum" }
      assert.isNotNull @exprCleaner.cleanExpr(field, enumValueIds: ["a", "b", "c"])
      assert.isNull @exprCleaner.cleanExpr(field, enumValueIds: ["a"])

    it "nulls if missing variable", ->
      assert.isNotNull @exprCleaner.cleanExpr({ type: "variable", variableId: "varnumber" }, table: "t2")
      assert.isNull @exprCleaner.cleanExpr({ type: "variable", variableId: "varxyz" }, table: "t2")

    it "allows variable if right id table", ->
      assert.isNotNull @exprCleaner.cleanExpr({ type: "variable", variableId: "varid" }, { table: "t2", idTable: "t1" })
      assert.isNull @exprCleaner.cleanExpr({ type: "variable", variableId: "varid" }, { table: "t2", idTable: "t2" })

    it "nulls recursive field expr", ->
      table = @schema.getTable("t1")
      table.contents.push(
        { id: "expr_recursive", name: { en: "Expr Recursive"}, type: "expr", expr: { type: "field", table: "t1", column: "expr_recursive" }}
      )
      schema = @schema.addTable(table)
      exprCleaner = new ExprCleaner(schema)

      assert.isNull exprCleaner.cleanExpr({ type: "field", table: "t1", column: "expr_recursive" })

    it "nulls if expr is invalid", ->
      table = @schema.getTable("t1")
      table.contents.push(
        { id: "expr_invalid", name: { en: "Expr Invalid"}, type: "expr", expr: { type: "field", table: "t1", column: "nonsuch" }}
        { id: "expr_valid", name: { en: "Expr Invalid"}, type: "expr", expr: { type: "field", table: "t1", column: "enum" }}
      )
      schema = @schema.addTable(table)
      exprCleaner = new ExprCleaner(schema)

      assert.isNull exprCleaner.cleanExpr({ type: "field", table: "t1", column: "expr_invalid" })
      assert exprCleaner.cleanExpr({ type: "field", table: "t1", column: "expr_valid" })


    describe "aggregation", ->
      it "aggregates if required", ->
        field = { type: "field", table: "t1", column: "number" }
        compare(@exprCleaner.cleanExpr(field, aggrStatuses: ['aggregate']), {
          type: "op"
          op: "last"
          table: "t1"
          exprs: [field]
          })

      it "nulls if aggregate and should not be", ->
        field = { type: "field", table: "t1", column: "number" }
        assert @exprCleaner.cleanExpr(field, aggrStatuses: ['individual']), "is individual"
        assert.isNull @exprCleaner.cleanExpr(field, aggrStatuses: ['literal'])
        
        aggr = { type: "op", table: "t1", op: "sum", exprs: [field] }
        assert.isNull @exprCleaner.cleanExpr(aggr), 'is aggregate'
        assert.isNull @exprCleaner.cleanExpr(aggr, aggrStatuses: ['literal'])
        assert @exprCleaner.cleanExpr(aggr, aggrStatuses: ['aggregate']), 'should allow aggregate'

      it "nulls inner expr if wrong aggregation status", ->
        field = { type: "field", table: "t1", column: "number" }
        expr = { type: "op", table: "t1", op: "sum", exprs: [{ type: "op", op: "sum", exprs: [field] }] }

        compare(@exprCleaner.cleanExpr(expr, aggrStatuses: ["aggregate"]), { type: "op", op: "sum", table: "t1", exprs: [null] })

      it "passes types through aggregation", ->
        field = { type: "field", table: "t1", column: "number" }
        expr = { type: "op", table: "t1", op: "sum", exprs: [field] }

        compare(@exprCleaner.cleanExpr(expr, types: ["number"], aggrStatuses: ["aggregate"]), expr)
        compare(@exprCleaner.cleanExpr(expr, types: ["text"], aggrStatuses: ["aggregate"]), null)

      it "allows no args for count", ->
        field = { type: "field", table: "t1", column: "number" }
        expr = { type: "op", table: "t1", op: "count", exprs: [field] }

        compare(@exprCleaner.cleanExpr(expr, types: ["number"], aggrStatuses: ["aggregate"]), { type: "op", table: "t1", op: "count", exprs: [] })

    describe "fixing expression types", ->
      it "creates boolean from enum", ->
        expr = { type: "field", table: "t1", column: "enum" }
        compare(@exprCleaner.cleanExpr(expr, types: ["boolean"]), { type: "op", table: "t1", op: "= any", exprs: [expr, null] })

      it "creates percent where from enum", ->
        expr = { type: "field", table: "t1", column: "enum" }
        compare(
          @exprCleaner.cleanExpr(expr, types: ["number"], aggrStatuses: ["aggregate", "literal"]) 
          {
            type: "op"
            op: "percent where"
            table: "t1"
            exprs: [
              { type: "op", table: "t1", op: "= any", exprs: [expr, null] }
              null
            ]
          }
        )

      it "creates boolean inside percent where from enum", ->
        expr = { type: "op", table: "t1", op: "percent where", exprs: [{ type: "field", table: "t1", column: "enum" }] }
        compare(
          @exprCleaner.cleanExpr(expr, types: ["number"], aggrStatuses: ["aggregate", "literal"]) 
          {
            type: "op"
            op: "percent where"
            table: "t1"
            exprs: [
              { type: "op", table: "t1", op: "= any", exprs: [{ type: "field", table: "t1", column: "enum" }, null] }
              null
            ]
          }
        )

    describe "op", ->
      it "preserves 'and' by cleaning child expressions with boolean type", ->
        expr = { type: "op", op: "and", table: "t1", exprs: [{ type: "field", table: "t1", column: "enum" }, { type: "field", table: "t1", column: "boolean" }]}

        compare(@exprCleaner.cleanExpr(expr), {
          type: "op"
          op: "and"
          table: "t1"
          exprs: [
            # Booleanized
            { type: "op", table: "t1", op: "= any", exprs: [{ type: "field", table: "t1", column: "enum" }, null]}
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

      it "allows math on aggregates", ->
        field = { type: "field", table: "t1", column: "number" }
        expr = { type: "op", table: "t1", op: "sum", exprs: [field] }

        expr = { type: "op", op: "+", table: "t1", exprs: [expr, expr]}

        compare(@exprCleaner.cleanExpr(expr, types: ["number"], aggrStatuses: ["aggregate"]), expr)

      it "allows building math on aggregates", ->
        field = { type: "field", table: "t1", column: "number" }
        expr = { type: "op", table: "t1", op: "sum", exprs: [field] }

        expr = { type: "op", op: "+", table: "t1", exprs: [expr, null]}

        compare(@exprCleaner.cleanExpr(expr, types: ["number"], aggrStatuses: ["aggregate"]), expr)

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

        expr = { type: "op", op: "=", table: "t1", exprs: [{ type: "id", table: "t1" }, { type: "literal", valueType: "id", idTable: "t2", value: "123" }]}
        compare(@exprCleaner.cleanExpr(expr), { type: "op", op: "=", table: "t1", exprs: [{ type: "id", table: "t1" }, null]})

      it "allows empty lhs for prefix-type expressions", ->
        expr = { type: "op", op: "distance", table: "t1", exprs: []}
        compare(@exprCleaner.cleanExpr(expr), { type: "op", op: "distance", table: "t1", exprs: [null, null]})

      it "allows null-type lhs for prefix-type expressions", ->
        expr = { type: "op", op: "distance", table: "t1", exprs: [{ type: "scalar", table: "t1", joins: ["1-2"], expr: null }, null] }
        compare(@exprCleaner.cleanExpr(expr), expr)

      it "removes invalid lhs", ->
        expr = { type: "op", op: "=", table: "t1", exprs: [{ type: "field", table: "t1", column: "NONSUCH" }, null]}
        compare(@exprCleaner.cleanExpr(expr), null)

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
            cases: [{ when: { type: "op", op: "=", exprs: [{ type: "literal", valueType: "number", value: 123 }, null] }, then: { type: "literal", valueType: "number", value: 123 }}]
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

    describe "score", ->
      it "cleans input", ->
        expr = { type: "score", table: "t1", input: { type: "field", table: "t1", column: "text" }, scores: {} }
        @clean(expr, { type: "score", table: "t1", input: null, scores: {} })

        expr = { type: "score", table: "t1", input: { type: "field", table: "t1", column: "enum" }, scores: {} }
        @clean(expr, expr)

      it "removes invalid scores keys", ->
        expr = { type: "score", table: "t1", input: { type: "field", table: "t1", column: "enum" }, scores: { a: 3, nonsuch: 4 } }
        @clean(expr, { type: "score", table: "t1", input: { type: "field", table: "t1", column: "enum" }, scores: { a: 3 } })

      it "cleans score values", ->
        expr = { type: "score", table: "t1", input: { type: "field", table: "t1", column: "enum" }, scores: {
            a: { type: "field", table: "t1", column: "number" }
          } 
        }
        # Untouched since was number
        @clean(expr, expr)

        expr = { type: "score", table: "t1", input: { type: "field", table: "t1", column: "enum" }, scores: {
            a: { type: "field", table: "t1", column: "text" }
          } 
        }
        @clean(expr, { type: "score", table: "t1", input: { type: "field", table: "t1", column: "enum" }, scores: { } })

      it "removes all scores if no input", ->
        expr = { type: "score", table: "t1", input: null, scores: { a: 3, nonsuch: 4 } }
        @clean(expr, { type: "score", table: "t1", input: null, scores: { } })

    describe "build enumset", ->
      it "cleans values", ->
        expr = { type: "build enumset", table: "t1", values: { a: { type: "field", table: "t1", column: "boolean" }} }
        @clean(expr, expr)

        expr = { type: "build enumset", table: "t1", values: { a: { type: "field", table: "t1", column: "xyz" }} }
        @clean(expr, { type: "build enumset", table: "t1", values: {} })

      it "removes invalid value keys", ->
        expr = { 
          type: "build enumset"
          table: "t1"
          values: { 
            a: { type: "literal", valueType: "boolean", value: true }
            b: { type: "literal", valueType: "boolean", value: false }
          }
        }
        compare(@exprCleaner.cleanExpr(expr, types: ["enumset"], enumValueIds: ['a']), 
          {           
            type: "build enumset"
            table: "t1"
            values: { 
              a: { type: "literal", valueType: "boolean", value: true }
            }
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
        fieldExpr = { type: "op", table: "t2", op: "sum", exprs: [{ type: "field", table: "t2", column: "number" }] }
        scalarExpr = { type: "scalar", table: "t1", joins: ['1-2'], expr: fieldExpr }

        compare(scalarExpr, @exprCleaner.cleanExpr(scalarExpr))

      it "moves aggr to expr", ->
        fieldExpr = { type: "field", table: "t2", column: "number" }
        scalarExpr = { type: "scalar", table: "t1", joins: ['1-2'], expr: fieldExpr, aggr: "sum" }
        scalarExpr = @exprCleaner.cleanExpr(scalarExpr)
        compare(scalarExpr, { type: "scalar", table: "t1", joins: ['1-2'], expr: { type: "op", op: "sum", table: "t2", exprs: [fieldExpr] }})

      it "defaults aggr if needed", ->
        fieldExpr = { type: "field", table: "t2", column: "text" }
        scalarExpr = { type: "scalar", table: "t1", joins: ['1-2'], expr: fieldExpr }
        scalarExpr = @exprCleaner.cleanExpr(scalarExpr)
        compare(scalarExpr, { type: "scalar", table: "t1", joins: ['1-2'], expr: { type: "op", op: "last", table: "t2", exprs: [fieldExpr] }})

      it "strips where if wrong table", ->
        fieldExpr = { type: "op", op: "sum", table: "t2", exprs: [{ type: "field", table: "t2", column: "number" }] }
        whereExpr = { type: "logical", table: "t1" }
        scalarExpr = { type: "scalar", table: "t1", joins: ['1-2'], expr: fieldExpr }
        scalarExpr = @exprCleaner.cleanExpr(scalarExpr)
        assert scalarExpr.expr, "Should keep expr"
        assert not scalarExpr.where, "Should remove where"

      it "strips if invalid join", ->
        fieldExpr = { type: "op", op: "sum", exprs: [{ type: "field", table: "t2", column: "number" }] }
        scalarExpr = { type: "scalar", table: "t1", joins: ['xyz'], expr: fieldExpr }
        scalarExpr = @exprCleaner.cleanExpr(scalarExpr)
        assert not scalarExpr

      it "simplifies if no joins", ->
        fieldExpr = { type: "field", table: "t1", column: "number" }
        scalarExpr = { type: "scalar", table: "t1", joins: [], expr: fieldExpr }
        scalarExpr = @exprCleaner.cleanExpr(scalarExpr)
        compare(fieldExpr, scalarExpr)

      it "simplifies if id and single join", ->
        fieldExpr = { type: "id", table: "t2" }
        scalarExpr = { type: "scalar", table: "t1", joins: ["1-2"], expr: fieldExpr }
        expr = @exprCleaner.cleanExpr(scalarExpr)
        compare(expr, { type: "field", table: "t1", column: "1-2" })


  # Version 1 expression should be upgraded to version 2
  describe "upgrade", ->
    it "count becomes id", ->
      @clean(
        { type: "scalar", table: "t1", aggr: "count", joins: ["1-2"], expr: { type: "count", table: "t2" } }
        { type: "scalar", table: "t1", joins: ["1-2"], expr: { type: "op", op: "count", table: "t2", exprs: [] } }
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

