assert = require('chai').assert
fixtures = require './fixtures'
_ = require 'lodash'
canonical = require 'canonical-json'
moment = require 'moment'
sinon = require 'sinon'

ExprCompiler = require '../src/ExprCompiler'
ColumnNotFoundException = require '../src/ColumnNotFoundException'

compare = (actual, expected) ->
  assert.equal canonical(actual), canonical(expected), "\ngot:" + canonical(actual) + "\nexp:" + canonical(expected) + "\n"
 
describe "ExprCompiler", ->
  beforeEach ->
    @ec = new ExprCompiler(fixtures.simpleSchema())
    @compile = (expr, expected) =>
      @ec.testResetAlias() 
      jsonql = @ec.compileExpr(expr: expr, tableAlias: "T1")
      compare(jsonql, expected)

  it "compiles field", ->
    @compile(
      { type: "field", table: "t1", column: "number" }
      {
        type: "field"
        tableAlias: "T1"
        column: "number"
      })

  it "throws ColumnNotFoundException", ->
    assert.throws () =>
      @ec.compileExpr(expr: { type: "field", table: "t1", column: "XYZ" }, tableAlias: "T1")
    , ColumnNotFoundException

  describe "case", ->
    before ->
      @bool1 = { type: "literal", valueType: "boolean", value: true }
      @bool1JsonQL = { type: "literal", value: true }
      @bool2 = { type: "literal", valueType: "boolean", value: false }
      @bool2JsonQL = { type: "literal", value: false }

      @number1 = { type: "literal", valueType: "number", value: 2 }
      @number1JsonQL = { type: "literal", value: 2 }
      @number2 = { type: "literal", valueType: "number", value: 3 }
      @number2JsonQL = { type: "literal", value: 3 }
      @number3 = { type: "literal", valueType: "number", value: 4 }
      @number3JsonQL = { type: "literal", value: 4 }

    it "compiles case", ->
      @compile(
        { 
          type: "case"
          table: "t1"
          cases: [
            { when: @bool1, then: @number1 }
            { when: @bool2, then: @number2 }
          ]
          else: @number3
        }
        {
          type: "case"
          cases: [
            { when: @bool1JsonQL, then: @number1JsonQL }
            { when: @bool2JsonQL, then: @number2JsonQL }
          ]
          else: @number3JsonQL
        })

    it "skips null whens", ->
      @compile(
        { 
          type: "case"
          table: "t1"
          cases: [
            { when: null, then: @number1 }
            { when: @bool2, then: @number2 }
          ]
          else: @number3
        }
        {
          type: "case"
          cases: [
            { when: @bool2JsonQL, then: @number2JsonQL }
          ]
          else: @number3JsonQL
        })

    it "skips if empty", ->
      @compile(
        { 
          type: "case"
          table: "t1"
          cases: [
            { when: null, then: @number1 }
          ]
          else: @number3
        }
        null
      )

  describe "scalar", ->
    it "compiles scalar with no joins, simplifying", ->
      @compile(
        { type: "scalar", table: "t1", expr: { type: "field", table: "t1", column: "number" }, joins: [] }
        { type: "field", tableAlias: "T1", column: "number" }
      )

    it "compiles scalar with one join", ->
      @compile(
        { type: "scalar", table: "t1", expr: { type: "field", table: "t2", column: "number" }, joins: ["1-2"] }
        {
          type: "scalar"
          expr: { type: "field", tableAlias: "j1", column: "number" }
          from: { type: "table", table: "t2", alias: "j1" }
          where: { type: "op", op: "=", exprs: [
            { type: "field", tableAlias: "j1", column: "t1" }
            { type: "field", tableAlias: "T1", column: "primary" }
            ]}
        })

    it "compiles scalar with one join and sql aggr", ->
      @compile(
        { type: "scalar", table: "t1", expr: { type: "field", table: "t2", column: "number" }, joins: ["1-2"], aggr: "count" }
        {
          type: "scalar"
          expr: { type: "op", op: "count", exprs: [{ type: "field", tableAlias: "j1", column: "number" }] }
          from: { type: "table", table: "t2", alias: "j1" }
          where: { type: "op", op: "=", exprs: [
            { type: "field", tableAlias: "j1", column: "t1" }
            { type: "field", tableAlias: "T1", column: "primary" }
            ]}
        })

    it "compiles scalar with one join and count(<primary key>) aggr", ->
      @compile(
        { type: "scalar", table: "t1", expr: { type: "id", table: "t2" }, joins: ["1-2"], aggr: "count" }
        {
          type: "scalar"
          expr: { type: "op", op: "count", exprs: [{ type: "field", tableAlias: "j1", column: "primary" }] }
          from: { type: "table", table: "t2", alias: "j1" }
          where: { type: "op", op: "=", exprs: [
            { type: "field", tableAlias: "j1", column: "t1" }
            { type: "field", tableAlias: "T1", column: "primary" }
            ]}
        })

    it "compiles scalar with one join and last aggr", ->
      @compile(
        { type: "scalar", table: "t1", expr: { type: "field", table: "t2", column: "number" }, joins: ["1-2"], aggr: "last" }
        {
          type: "scalar"
          expr: { type: "field", tableAlias: "j1", column: "number" }
          from: { type: "table", table: "t2", alias: "j1" }
          where: { type: "op", op: "=", exprs: [
            { type: "field", tableAlias: "j1", column: "t1" }
            { type: "field", tableAlias: "T1", column: "primary" }
            ]}
          orderBy: [{ expr: { type: "field", tableAlias: "j1", column: "number" }, direction: "desc" }]
          limit: 1
        }
      )

    it "compiles scalar with two joins", -> 
      @compile(
        { type: "scalar", table: "t1", expr: { type: "field", table: "t1", column: "number" }, joins: ["1-2", "2-1"], aggr: "count" }
        {
          type: "scalar"
          expr: { type: "op", op: "count", exprs: [{ type: "field", tableAlias: "j2", column: "number" }] }
          from: { 
            type: "join" 
            left: { type: "table", table: "t2", alias: "j1" }
            right: { type: "table", table: "t1", alias: "j2" }
            kind: "left"
            on: { type: "op", op: "=", exprs: [
              { type: "field", tableAlias: "j1", column: "t1" }
              { type: "field", tableAlias: "j2", column: "primary" }
              ]}
            } 
          where: { type: "op", op: "=", exprs: [
            { type: "field", tableAlias: "j1", column: "t1" }
            { type: "field", tableAlias: "T1", column: "primary" }
            ]}
        })

    it "compiles scalar with one join and where", ->
      where = {
        "type": "logical",
        "op": "and",
        "exprs": [
          {
            "type": "comparison",
            "lhs": {
              "type": "scalar",
              "baseTableId": "t2",
              "expr": {
                "type": "field",
                "table": "t2",
                "column": "number"
              },
              "joins": []
            },
            "op": "=",
            "rhs": {
              "type": "literal",
              "valueType": "number",
              "value": 3
            }
          }
        ]
      }

      @compile(
        { 
          type: "scalar", 
          table: "t1",      
          expr: { type: "field", table: "t2", column: "number" }, 
          joins: ["1-2"], 
          where: where
        }
        {
          type: "scalar"
          expr: { type: "field", tableAlias: "j1", column: "number" }
          from: { type: "table", table: "t2", alias: "j1" }
          where: {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: "=", exprs: [
                { type: "field", tableAlias: "j1", column: "t1" }
                { type: "field", tableAlias: "T1", column: "primary" }
                ]
              }
              {
                type: "op", op: "=", exprs: [
                  { type: "field", tableAlias: "j1", column: "number" }
                  { type: "literal", value: 3 }
                ]
              }
            ]
          }
        })

  describe "score", ->
    it "scores enum", ->
      @compile(
        { 
          type: "score"
          input: { type: "field", table: "t1", column: "enum" }
          scores: { 
            a: { type: "literal", valueType: "number", value: 4 }
          }
        }
        # case T1.enum when 'a' then 4 else 0 end
        {
          type: "case"
          input: { type: "field", tableAlias: "T1", column: "enum" }
          cases: [
            { when: { type: "literal", value: "a" }, then: { type: "literal", value: 4 } }
          ]
          else: { type: "literal", value: 0 }
        }
      )

    it "scores empty enum", ->
      @compile(
        { 
          type: "score"
          input: { type: "field", table: "t1", column: "enum" }
          scores: { }
        }
        { type: "literal", value: 0 }
      )

    it "scores enumset", ->
      @compile(
        { 
          type: "score"
          input: { type: "field", table: "t1", column: "enumset" }
          scores: { 
            a: { type: "literal", valueType: "number", value: 3 }
            b: { type: "literal", valueType: "number", value: 4 } 
          }
        }
        # case when T1.enum  then 4 else 0 end
        {
          type: "op"
          op: "+"
          exprs: [
            {
              type: "case"
              cases: [
                { 
                  when: {
                    type: "op"
                    op: "@>"
                    exprs: [
                      { type: "op", op: "::jsonb", exprs: [{ type: "op", op: "to_json", exprs: [{ type: "field", tableAlias: "T1", column: "enumset" }] }]}
                      { type: "op", op: "::jsonb", exprs: [{ type: "op", op: "to_json", exprs: [{ type: "literal", value: ["a"] }] }]}
                    ]
                  }
                  then: { type: "literal", value: 3 } 
                }
              ]
              else: { type: "literal", value: 0 }
            }
            {
              type: "case"
              cases: [
                { 
                  when: {
                    type: "op"
                    op: "@>"
                    exprs: [
                      { type: "op", op: "::jsonb", exprs: [{ type: "op", op: "to_json", exprs: [{ type: "field", tableAlias: "T1", column: "enumset" }] }]}
                      { type: "op", op: "::jsonb", exprs: [{ type: "op", op: "to_json", exprs: [{ type: "literal", value: ["b"] }] }]}
                    ]
                  }
                  then: { type: "literal", value: 4 } 
                }
              ]
              else: { type: "literal", value: 0 }
            }
          ]
        }
      )

    it "scores empty enumset", ->
      @compile(
        { 
          type: "score"
          input: { type: "field", table: "t1", column: "enumset" }
          scores: { }
        }
        { type: "literal", value: 0 }
      )

  it "simplifies scalar join to id where toColumn is primary key", ->
    @compile({ 
      type: "scalar", 
      table: "t2",
      expr: { type: "id", table: "t1" }, 
      joins: ["2-1"],
    }, { type: "field", tableAlias: "T1", column: "t1" })

  it "compiles literals", ->
    @compile({ type: "literal", valueType: "text", value: "abc" }, { type: "literal", value: "abc" })
    @compile({ type: "literal", valueType: "number", value: 123 }, { type: "literal", value: 123 })
    @compile({ type: "literal", valueType: "enum", value: "id1" }, { type: "literal", value: "id1" })
    @compile({ type: "literal", valueType: "boolean", value: true }, { type: "literal", value: true })
    @compile({ type: "literal", valueType: "boolean", value: true }, { type: "literal", value: true })

  describe "ops", ->
    before ->
      @bool1 = { type: "literal", valueType: "boolean", value: true }
      @bool1JsonQL = { type: "literal", value: true }
      @bool2 = { type: "literal", valueType: "boolean", value: false }
      @bool2JsonQL = { type: "literal", value: false }

      @number1 = { type: "literal", valueType: "number", value: 2 }
      @number1JsonQL = { type: "literal", value: 2 }
      @number2 = { type: "literal", valueType: "number", value: 3 }
      @number2JsonQL = { type: "literal", value: 3 }

      @text1 = { type: "literal", valueType: "text", value: "a" }
      @text1JsonQL = { type: "literal", value: "a" }
      @text2 = { type: "literal", valueType: "text", value: "b" }
      @text2JsonQL = { type: "literal", value: "b" }

      @date1 = { type: "literal", valueType: "date", value: "2014-01-01" }
      @date1JsonQL = { type: "literal", value: "2014-01-01" }
      @date2 = { type: "literal", valueType: "date", value: "2014-12-31" }
      @date2JsonQL = { type: "literal", value: "2014-12-31" }
      @date3 = { type: "literal", valueType: "date", value: "2015-01-01" }
      @date3JsonQL = { type: "literal", value: "2015-01-01" }

      @datetime1 = { type: "literal", valueType: "datetime", value: "2014-01-01T01:02:03Z" }
      @datetime1JsonQL = { type: "literal", value: "2014-01-01T01:02:03Z" }

      @geometry = { type: "field", table: "t1", column: "geometry" }
      @geometryJsonQL = { type: "field", tableAlias: "T1", column: "geometry" }

    it "compiles and", ->
      @compile(
        { 
          type: "op"
          op: "and"
          exprs: [@bool1, @bool2]
        }
        { 
          type: "op"
          op: "and"
          exprs: [@bool1JsonQL, @bool2JsonQL]
        }
      )

    it "compiles or", ->
      @compile(
        { 
          type: "op"
          op: "or"
          exprs: [@bool1, @bool2]
        }
        { 
          type: "op"
          op: "or"
          exprs: [@bool1JsonQL, @bool2JsonQL]
        }
      )

    it "compiles or with nulls", ->
      @compile(
        { 
          type: "op"
          op: "or"
          exprs: [@bool1, null]
        }
        { 
          type: "op"
          op: "or"
          exprs: [@bool1JsonQL]
        }
      )

    it "compiles or with all nulls", ->
      @compile(
        { 
          type: "op"
          op: "or"
          exprs: [null, null]
        }
        null
      )

    it "compiles +, *", ->
      for op in ["+", "*"]
        @compile(
          {
            type: "op"
            op: op
            exprs: [@number1, @number2, @number1, null]
          }
          {
            type: "op"
            op: op
            exprs: [@number1JsonQL, @number2JsonQL, @number1JsonQL]
          }
        )

    it "compiles -, /", ->
      for op in ["-", "/"]
        @compile(
          {
            type: "op"
            op: op
            exprs: [@number1, @number2]
          }
          {
            type: "op"
            op: op
            exprs: [@number1JsonQL, @number2JsonQL]
          }
        )

        @compile(
          {
            type: "op"
            op: op
            exprs: [null, @number2]
          }
          null
        )

    it "compiles between", ->
      @compile(
        {
          type: "op"
          op: "between"
          exprs: [@date1, @date2, @date3]
        }
        {
          type: "op"
          op: "between"
          exprs: [@date1JsonQL, @date2JsonQL, @date3JsonQL]
        }
      )

    it "compiles between with first null (null)", ->
      @compile(
        {
          type: "op"
          op: "between"
          exprs: [null, @date2, @date3]
        }
        null
      )

    it "compiles between with second null (<=)", ->
      @compile(
        {
          type: "op"
          op: "between"
          exprs: [@date1, null, @date3]
        }
        {
          type: "op"
          op: "<="
          exprs: [@date1JsonQL, @date3JsonQL]
        }
      )

    it "compiles between with third null (>=)", ->
      @compile(
        {
          type: "op"
          op: "between"
          exprs: [@date1, @date2, null]
        }
        {
          type: "op"
          op: ">="
          exprs: [@date1JsonQL, @date2JsonQL]
        }
      )

    it "compiles not", ->
      @compile(
        {
          type: "op"
          op: "not"
          exprs: [@bool1]
        }
        {
          type: "op"
          op: "not"
          exprs: [@bool1JsonQL]
        }
      )

      @compile(
        {
          type: "op"
          op: "not"
          exprs: [null]
        }
        null
      )

    it "compiles =, <>, >, >=, <, <=", ->
      for op in ["=", "<>", ">", ">=", "<", "<="]
        # Normal
        @compile(
          {
            type: "op"
            op: op
            exprs: [@number1, @number2]
          }
          {
            type: "op"
            op: op
            exprs: [@number1JsonQL, @number2JsonQL]
          }
        )

        # Missing value
        @compile(
          {
            type: "op"
            op: op
            exprs: [@number1, null]
          }
          null
        )

    it "compiles ~*", ->
      @compile(
        {
          type: "op"
          op: "~*"
          exprs: [@text1, @text2]
        }
        {
          type: "op"
          op: "~*"
          exprs: [@text1JsonQL, @text2JsonQL]
        }
      )

      # Missing value
      @compile(
        {
          type: "op"
          op: "~*"
          exprs: [@text1, null]
        }
        null
      )

    it "compiles = any", ->
      @compile(
        { 
          type: "op"
          op: "= any", 
          exprs: [
            { type: "field", table: "t1", column: "enum" } 
            { type: "literal", valueType: "enumset", value: ["a", "b"] }
          ]
        }
        {
          type: "op"
          op: "="
          modifier: "any"
          exprs: [
            { type: "field", tableAlias: "T1", column: "enum" }
            { type: "literal", value: ["a", "b"] }
          ]
        }
      )

    it "compiles empty = any", ->
      @compile(
        { 
          type: "op"
          op: "= any", 
          exprs: [
            { type: "field", table: "t1", column: "enum" } 
            { type: "literal", valueType: "enumset", value: [] }
          ]
        }
        null
      )

    it "compiles invalid = any", ->
      @compile(
        { 
          type: "op"
          op: "= any", 
          exprs: [
            null
            { type: "literal", valueType: "enumset", value: [] }
          ]
        }
        null
      )

    it "compiles is null", ->
      @compile(
        {
          type: "op"
          op: "is null"
          exprs: [@number1]
        }
        {
          type: "op"
          op: "is null"
          exprs: [@number1JsonQL]
        }
      )

      @compile(
        {
          type: "op"
          op: "is null"
          exprs: [null]
        }
        null
      )

    it "compiles is not null", ->
      @compile(
        {
          type: "op"
          op: "is not null"
          exprs: [@number1]
        }
        {
          type: "op"
          op: "is not null"
          exprs: [@number1JsonQL]
        }
      )

      @compile(
        {
          type: "op"
          op: "is not null"
          exprs: [null]
        }
        null
      )

    it "compiles contains", ->
      @compile(
        { 
          type: "op"
          op: "contains", 
          exprs: [
            { type: "field", table: "t1", column: "enumset" } 
            { type: "literal", valueType: "enumset", value: ["a", "b"] }
          ]
        }
        {
          type: "op"
          op: "@>"
          exprs: [
            { type: "op", op: "::jsonb", exprs: [{ type: "op", op: "to_json", exprs: [{ type: "field", tableAlias: "T1", column: "enumset" }] }]}
            { type: "op", op: "::jsonb", exprs: [{ type: "op", op: "to_json", exprs: [{ type: "literal", value: ["a", "b"] }] }]}
          ]
        }
      )

    it "compiles empty contains", ->
      @compile(
        { 
          type: "op"
          op: "contains", 
          exprs: [
            { type: "field", table: "t1", column: "enumset" } 
            { type: "literal", valueType: "enumset", value: [] }
          ]
        }
        null
      )

    it "compiles latitude", ->
      @compile(
        {
          type: "op"
          op: "latitude"
          exprs: [{ type: "field", table: "t1", column: "geometry" }]
        }
        {
          type: "op"
          op: "ST_Y"
          exprs: [
            { 
              type: "op"
              op: "ST_Transform"
              exprs: [
                { type: "field", tableAlias: "T1", column: "geometry" }
                4326
              ]
            }
          ]
        }
      )

    it "compiles longitude", ->
      @compile(
        {
          type: "op"
          op: "longitude"
          exprs: [{ type: "field", table: "t1", column: "geometry" }]
        }
        {
          type: "op"
          op: "ST_X"
          exprs: [
            { 
              type: "op"
              op: "ST_Transform"
              exprs: [
                { type: "field", tableAlias: "T1", column: "geometry" }
                4326
              ]
            }
          ]
        }
      )

    it "compiles within", ->
      @compile(
        {
          type: "op"
          op: "within"
          exprs: [{ type: "id", table: "thier" }, { type: "literal", valueType: "id", idTable: "thier", value: "123" }]
        }
        { 
          type: "op"
          op: "in"
          exprs: [
            { type: "field", tableAlias: "T1", column: "primary" }
            {
              type: "scalar"
              expr: { type: "field", tableAlias: "subwithin", column: "primary" }
              from: { type: "table", table: "thier", alias: "subwithin" }
              where: {
                type: "op"
                op: "@>"
                exprs: [
                  { type: "field", tableAlias: "subwithin", column: "path" }
                  { type: "op", op: "::jsonb", exprs: [{ type: "op", op: "json_build_array", exprs: [{ type: "literal", value: "123" }] }] }
                ]
              } 
            }
          ]
        }
      ) 

    describe "relative dates", ->
      it "thisyear", ->
        @compile(
          {
            type: "op"
            op: "thisyear"
            exprs: [@date1]
          }
          {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: ">=", exprs: [@date1JsonQL, moment().startOf('year').format("YYYY-MM-DD")]}
              { type: "op", op: "<", exprs: [@date1JsonQL, moment().startOf('year').add(1, "years").format("YYYY-MM-DD")]}
            ]
          }
        )

      it "lastyear", ->
        @compile(
          {
            type: "op"
            op: "lastyear"
            exprs: [@date1]
          }
          {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: ">=", exprs: [@date1JsonQL, moment().startOf('year').subtract(1, "years").format("YYYY-MM-DD")]}
              { type: "op", op: "<", exprs: [@date1JsonQL, moment().startOf('year').format("YYYY-MM-DD")]}
            ]
          }
        )

      it "thismonth", ->
        @compile(
          {
            type: "op"
            op: "thismonth"
            exprs: [@date1]
          }
          {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: ">=", exprs: [@date1JsonQL, moment().startOf('month').format("YYYY-MM-DD")]}
              { type: "op", op: "<", exprs: [@date1JsonQL, moment().startOf('month').add(1, "months").format("YYYY-MM-DD")]}
            ]
          }
        )

      it "lastmonth", ->
        @compile(
          {
            type: "op"
            op: "lastmonth"
            exprs: [@date1]
          }
          {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: ">=", exprs: [@date1JsonQL, moment().startOf('month').subtract(1, "months").format("YYYY-MM-DD")]}
              { type: "op", op: "<", exprs: [@date1JsonQL, moment().startOf('month').format("YYYY-MM-DD")]}
            ]
          }
        )

      it "today", ->
        @compile(
          {
            type: "op"
            op: "today"
            exprs: [@date1]
          }
          {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: ">=", exprs: [@date1JsonQL, moment().format("YYYY-MM-DD")]}
              { type: "op", op: "<", exprs: [@date1JsonQL, moment().add(1, "days").format("YYYY-MM-DD")]}
            ]
          }
        )

      it "yesterday", ->
        @compile(
          {
            type: "op"
            op: "yesterday"
            exprs: [@date1]
          }
          {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: ">=", exprs: [@date1JsonQL, moment().subtract(1, "days").format("YYYY-MM-DD")]}
              { type: "op", op: "<", exprs: [@date1JsonQL, moment().format("YYYY-MM-DD")]}
            ]
          }
        )

      it "last7days", ->
        @compile(
          {
            type: "op"
            op: "last7days"
            exprs: [@date1]
          }
          {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: ">=", exprs: [@date1JsonQL, moment().subtract(7, "days").format("YYYY-MM-DD")]}
              { type: "op", op: "<", exprs: [@date1JsonQL, moment().add(1, "days").format("YYYY-MM-DD")]}
            ]
          }
        )

      it "last30days", ->
        @compile(
          {
            type: "op"
            op: "last30days"
            exprs: [@date1]
          }
          {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: ">=", exprs: [@date1JsonQL, moment().subtract(30, "days").format("YYYY-MM-DD")]}
              { type: "op", op: "<", exprs: [@date1JsonQL, moment().add(1, "days").format("YYYY-MM-DD")]}
            ]
          }
        )

      it "last365days", ->
        @compile(
          {
            type: "op"
            op: "last365days"
            exprs: [@date1]
          }
          {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: ">=", exprs: [@date1JsonQL, moment().subtract(365, "days").format("YYYY-MM-DD")]}
              { type: "op", op: "<", exprs: [@date1JsonQL, moment().add(1, "days").format("YYYY-MM-DD")]}
            ]
          }
        )

    describe "relative datetimes", ->
      before ->
        @clock = sinon.useFakeTimers(new Date().getTime())

      after ->
        @clock.restore()

      it "thisyear", ->
        @compile(
          {
            type: "op"
            op: "thisyear"
            exprs: [@datetime1]
          }
          {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: ">=", exprs: [@datetime1JsonQL, moment().startOf('year').toISOString()]}
              { type: "op", op: "<", exprs: [@datetime1JsonQL, moment().startOf('year').add(1, "years").toISOString()]}
            ]
          }
        )

      it "lastyear", ->
        @compile(
          {
            type: "op"
            op: "lastyear"
            exprs: [@datetime1]
          }
          {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: ">=", exprs: [@datetime1JsonQL, moment().startOf('year').subtract(1, "years").toISOString()]}
              { type: "op", op: "<", exprs: [@datetime1JsonQL, moment().startOf('year').toISOString()]}
            ]
          }
        )

      it "thismonth", ->
        @compile(
          {
            type: "op"
            op: "thismonth"
            exprs: [@datetime1]
          }
          {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: ">=", exprs: [@datetime1JsonQL, moment().startOf('month').toISOString()]}
              { type: "op", op: "<", exprs: [@datetime1JsonQL, moment().startOf('month').add(1, "months").toISOString()]}
            ]
          }
        )

      it "lastmonth", ->
        @compile(
          {
            type: "op"
            op: "lastmonth"
            exprs: [@datetime1]
          }
          {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: ">=", exprs: [@datetime1JsonQL, moment().startOf('month').subtract(1, "months").toISOString()]}
              { type: "op", op: "<", exprs: [@datetime1JsonQL, moment().startOf('month').toISOString()]}
            ]
          }
        )

      it "today", ->
        @compile(
          {
            type: "op"
            op: "today"
            exprs: [@datetime1]
          }
          {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: ">=", exprs: [@datetime1JsonQL, moment().startOf("day").toISOString()]}
              { type: "op", op: "<", exprs: [@datetime1JsonQL, moment().startOf("day").add(1, "days").toISOString()]}
            ]
          }
        )

      it "yesterday", ->
        @compile(
          {
            type: "op"
            op: "yesterday"
            exprs: [@datetime1]
          }
          {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: ">=", exprs: [@datetime1JsonQL, moment().startOf("day").subtract(1, "days").toISOString()]}
              { type: "op", op: "<", exprs: [@datetime1JsonQL, moment().startOf("day").toISOString()]}
            ]
          }
        )

      it "last7days", ->
        @compile(
          {
            type: "op"
            op: "last7days"
            exprs: [@datetime1]
          }
          {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: ">=", exprs: [@datetime1JsonQL, moment().startOf("day").subtract(7, "days").toISOString()]}
              { type: "op", op: "<", exprs: [@datetime1JsonQL, moment().startOf("day").add(1, "days").toISOString()]}
            ]
          }
        )

      it "last30days", ->
        @compile(
          {
            type: "op"
            op: "last30days"
            exprs: [@datetime1]
          }
          {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: ">=", exprs: [@datetime1JsonQL, moment().startOf("day").subtract(30, "days").toISOString()]}
              { type: "op", op: "<", exprs: [@datetime1JsonQL, moment().startOf("day").add(1, "days").toISOString()]}
            ]
          }
        )

      it "last365days", ->
        @compile(
          {
            type: "op"
            op: "last365days"
            exprs: [@datetime1]
          }
          {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: ">=", exprs: [@datetime1JsonQL, moment().startOf("day").subtract(365, "days").toISOString()]}
              { type: "op", op: "<", exprs: [@datetime1JsonQL, moment().startOf("day").add(1, "days").toISOString()]}
            ]
          }
        )

    it "distance", ->
      @compile(
        {
          type: "op"
          op: "distance"
          exprs: [@geometry, @geometry]
        }
        # ST_Distance_Sphere(ST_Transform(x, 4326), ST_Transform(y, 4326))
        {
          type: "op"
          op: "ST_Distance_Sphere"
          exprs: [
            { type: "op", op: "ST_Transform", exprs: [@geometryJsonQL, 4326] }
            { type: "op", op: "ST_Transform", exprs: [@geometryJsonQL, 4326] }
          ]
        }
      )

  describe "custom jsonql", ->
    describe "table", ->
      it "substitutes table", ->
        schema = fixtures.simpleSchema()
        tableJsonql = {
          type: "query"
          selects: [
            {
              type: "field"
              tableAlias: "abc"
              column: "number"
            }
          ]
          from: { type: "table", table: "t2", alias: "abc" }
        }

        # Customize t2
        schema.getTable("t2").jsonql = tableJsonql
        
        ec = new ExprCompiler(schema)

        jql = ec.compileExpr(expr: { type: "scalar", table: "t1", joins: ["1-2"], expr: { type: "field", table: "t2", column: "number" } }, tableAlias: "T1")

        from = {
          type: "subquery",
          query: {
            type: "query",
            selects: [
              {
                type: "field",
                tableAlias: "abc",
                column: "number"
              }
            ],
            from: {
              type: "table",
              table: "t2",
              alias: "abc"
            }
          },
          alias: "j1"
        }

        assert _.isEqual(jql.from, from), JSON.stringify(jql, null, 2)

    # describe "join"
    describe "column", ->
      it "substitutes {alias}", ->
        schema = fixtures.simpleSchema()
        columnJsonql = {
          type: "op"
          op: "sum"
          exprs: [
            {
              type: "field"
              tableAlias: "{alias}"  # Should be replaced!
              column: "number"
            }
          ]
        }

        schema = schema.addTable({ id: "t1", contents:[{ id: "custom", name: "Custom", type: "text", jsonql: columnJsonql }]})
        
        ec = new ExprCompiler(schema)

        jql = ec.compileExpr(expr: { type: "field", table: "t1", column: "custom" }, tableAlias: "T1")

        assert _.isEqual jql, {
          type: "op"
          op: "sum"
          exprs: [
            {
              type: "field"
              tableAlias: "T1" # Replaced with table alias
              column: "number"
            }
          ]
        }

  describe "comparisons (deprecated)", ->
    it "compiles =", ->
      @compile(
        { 
          type: "comparison"
          op: "="
          lhs: { type: "field", table: "t1", column: "number" }
          rhs: { type: "literal", valueType: "number", value: 3 }
        }
        {
          type: "op"
          op: "="
          exprs: [
            { type: "field", tableAlias: "T1", column: "number" }
            { type: "literal", value: 3 }
          ]
        })

    it "compiles = any", ->
      @compile(
        { 
          type: "comparison", op: "= any", 
          lhs: { type: "field", table: "t1", column: "enum" } 
          rhs: { type: "literal", valueType: "enum[]", value: ["a", "b"] }
        }
        {
          type: "op"
          op: "="
          modifier: "any"
          exprs: [
            { type: "field", tableAlias: "T1", column: "enum" }
            { type: "literal", value: ["a", "b"] }
          ]
        })

    it "compiles no rhs as null", ->
      @compile(
        { 
          type: "comparison"
          op: "="
          lhs: { type: "field", table: "t1", column: "number" }
        }
        null
      )

    it "compiles daterange", ->
      @compile(
        { 
          type: "comparison"
          op: "between"
          lhs: { type: "field", table: "t1", column: "date" }
          rhs: { type: "literal", valueType: "daterange", value: ["2014-01-01", "2014-12-31"] }
        }
        {
          type: "op"
          op: "between"
          exprs: [
            { type: "field", tableAlias: "T1", column: "date" }
            { type: "literal", value: "2014-01-01" }
            { type: "literal", value: "2014-12-31" }
          ]
        })

  describe "logicals (deprecated)", ->
    it "simplifies logical", ->
      expr1 = { type: "comparison", op: "= false", lhs: { type: "field", table: "t1", column: "boolean" } }

      @compile(
        { type: "logical", op: "and", exprs: [expr1] }
        {
          type: "op"
          op: "="
          exprs: [
            { type: "field", tableAlias: "T1", column: "boolean" }
            { type: "literal", value: false }
          ]
        }
      )

    it "compiles logical", ->
      expr1 = { type: "comparison", op: "=", lhs: { type: "field", table: "t1", column: "number" }, rhs: { type: "literal", valueType: "number", value: 3 } }

      expr2 = { type: "comparison", op: "= false", lhs: { type: "field", table: "t1", column: "boolean" } }

      @compile(
        { type: "logical", op: "and", exprs: [expr1, expr2] }
        { type: "op", op: "and", exprs: [
          {
            type: "op"
            op: "="
            exprs: [
              { type: "field", tableAlias: "T1", column: "number" }
              { type: "literal", value: 3 }
            ]
          },
          {
            type: "op"
            op: "="
            exprs: [
              { type: "field", tableAlias: "T1", column: "boolean" }
              { type: "literal", value: false }
            ]
          }
        ]}
      )

    it "excluded blank condition", ->
      expr1 = { type: "comparison", op: "= true", lhs: { type: "field", table: "t1", column: "number" } }

      expr2 = { type: "comparison", op: "=", lhs: { type: "field", table: "t1", column: "number" } } # No RHS

      @compile(
        { type: "logical", op: "and", exprs: [expr1, expr2] }
        {
          type: "op"
          op: "="
          exprs: [
            { type: "field", tableAlias: "T1", column: "number" }
            { type: "literal", value: true }
          ]
        }
      )

        