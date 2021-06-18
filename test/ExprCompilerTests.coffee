assert = require('chai').assert
fixtures = require './fixtures'
_ = require 'lodash'
canonical = require 'canonical-json'
moment = require 'moment'
sinon = require 'sinon'
Schema = require('../src/Schema').default
ExprCompiler = require('../src/ExprCompiler').default
ColumnNotFoundException = require '../src/ColumnNotFoundException'
setupTestExtension = require('./extensionSetup').setupTestExtension

setupTestExtension()

compare = (actual, expected) ->
  assert.equal canonical(actual), canonical(expected), "\ngot:" + canonical(actual) + "\nexp:" + canonical(expected) + "\n"

# now expression (to_json(now() at time zone 'UTC')#>>'{}') as timestamp
nowExpr = {
  type: "op"
  op: "#>>"
  exprs: [
    { type: "op", op: "to_json", exprs: [
      { type: "op", op: "at time zone", exprs: [
        { type: "op", op: "now", exprs: [] }
        "UTC"
      ]}
    ]}
    "{}"
  ]
}

# to_json((now() - interval '24 hour') at time zone 'UTC')#>>'{}'
nowMinus24HoursExpr = {
  type: "op"
  op: "#>>"
  exprs: [
    { type: "op", op: "to_json", exprs: [
      { type: "op", op: "at time zone", exprs: [
        { type: "op", op: "-", exprs: [{ type: "op", op: "now", exprs: [] }, { type: "op", op: "interval", exprs: [{ type: "literal", value: "24 hour" }] }] }
        "UTC"
      ]}
    ]}
    "{}"
  ]
}

variables = [
  { id: "varenum", name: { _base: "en", en: "Varenum" }, type: "enum", enumValues: [{ id: "a", name: { en: "A" }}, { id: "b", name: { en: "B" }}] }
  { id: "varnumber", name: { _base: "en", en: "Varnumber" }, type: "number" }
  { id: "varnumberexpr", name: { _base: "en", en: "Varnumberexpr" }, type: "number", table: "t1" }
]

variableValues = {
  varenum: { type: "literal", valueType: "enum", value: "a" } 
  varnumber: { type: "literal", valueType: "number", value: 123 } 
  varnumberexpr: { type: "op", op: "+", table: "t1", exprs: [
    { type: "field", table: "t1", column: "number" }
    { type: "literal", valueType: "number", value: 2 }
  ]}
}

describe "ExprCompiler", ->
  beforeEach ->
    @ec = new ExprCompiler(fixtures.simpleSchema(), variables, variableValues)
    @compile = (expr, expected) =>
      jsonql = @ec.compileExpr(expr: expr, tableAlias: "T1")
      compare(jsonql, expected)

  it "compiles literal", ->
    @compile(
      { type: "literal", valueType: "number", value: 2 }
      {
        type: "literal"
        value: 2
      })

  it "compiles null literal", ->
    @compile(
      { type: "literal", value: null }
      null
    )

  it "compiles field", ->
    @compile(
      { type: "field", table: "t1", column: "number" }
      {
        type: "field"
        tableAlias: "T1"
        column: "number"
      })

  it "compiles expression field", ->
    @compile(
      { type: "field", table: "t1", column: "expr_enum" }
      {
        type: "field"
        tableAlias: "T1"
        column: "enum"
      })

  it "compiles join (id[]) field", ->
    @compile(
      { type: "field", table: "t1", column: "1-2" }
      {
        type: "scalar"
        # Wrap in jsonb array for easier handling
        expr: { type: "op", op: "to_jsonb", exprs:[{ type: "op", op: "array_agg", exprs: [{ type: "field", tableAlias: "inner", column: "primary" }] }] }
        from: { type: "table", table: "t2", alias: "inner" }
        where: { type: "op", op: "=", exprs: [
          { type: "field", tableAlias: "inner", column: "t1" }
          { type: "field", tableAlias: "T1", column: "primary" }
          ]}
        limit: 1
      })

  it "compiles join (id) field", ->
    @compile({ 
      type: "field", 
      table: "t2",
      column: ["2-1"]
    }, { type: "field", tableAlias: "T1", column: "t1" })

  it "compiles jsonql primaryKey", ->
    schema = new Schema().addTable({ id: "tpk", name: { en: "T1" }, primaryKey: { type: "field", tableAlias: "{alias}", column: "primary" }, contents: [
      { id: "text", name: { en: "Text" }, type: "text" }
    ]})

    ec = new ExprCompiler(schema)
    jsonql = ec.compileExpr(expr: { type: "id", table: "tpk" }, tableAlias: "TPK")
    compare(jsonql, { type: "field", tableAlias: "TPK", column: "primary" })

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
          expr: { type: "field", tableAlias: "1_2", column: "number" }
          from: { type: "table", table: "t2", alias: "1_2" }
          where: { type: "op", op: "=", exprs: [
            { type: "field", tableAlias: "1_2", column: "t1" }
            { type: "field", tableAlias: "T1", column: "primary" }
            ]}
          limit: 1
        })

    it "compiles scalar with one join that is through id", ->
      @compile(
        { type: "scalar", table: "t1", expr: { type: "field", table: "t2", column: "number" }, joins: ["id"] }
        {
          type: "scalar"
          expr: { type: "field", tableAlias: "id", column: "number" }
          from: { type: "table", table: "t2", alias: "id" }
          where: { type: "op", op: "=", exprs: [
            { type: "field", tableAlias: "T1", column: "id" }
            { type: "field", tableAlias: "id", column: "primary" }
          ]}
          limit: 1
        })

    it "compiles scalar with one join that is through id[]", ->
      @compile(
        { type: "scalar", table: "t1", expr: { type: "op", table: "t2", op: "count", exprs: [] }, joins: ["id[]"] }
        {
          type: "scalar"
          expr: { type: "op", op: "count", exprs: [] }
          from: { type: "table", table: "t2", alias: "id__" }
          where: { type: "op", op: "@>", exprs: [
            { type: "op", op: "to_jsonb", exprs: [{ type: "field", tableAlias: "T1", column: "id[]" }] }
            { type: "op", op: "to_jsonb", exprs: [{ type: "field", tableAlias: "id__", column: "primary" }] }
          ]}
          limit: 1
        })

    it "compiles scalar with one join and sql aggr", ->
      @compile(
        { type: "scalar", table: "t1", expr: { type: "field", table: "t2", column: "number" }, joins: ["1-2"], aggr: "count" }
        {
          type: "scalar"
          expr: { type: "op", op: "count", exprs: [{ type: "field", tableAlias: "1_2", column: "number" }] }
          from: { type: "table", table: "t2", alias: "1_2" }
          where: { type: "op", op: "=", exprs: [
            { type: "field", tableAlias: "1_2", column: "t1" }
            { type: "field", tableAlias: "T1", column: "primary" }
            ]}
          limit: 1
      })

    it "compiles scalar with one join and count(<primary key>) aggr", ->
      @compile(
        { type: "scalar", table: "t1", expr: { type: "id", table: "t2" }, joins: ["1-2"], aggr: "count" }
        {
          type: "scalar"
          expr: { type: "op", op: "count", exprs: [{ type: "field", tableAlias: "1_2", column: "primary" }] }
          from: { type: "table", table: "t2", alias: "1_2" }
          where: { type: "op", op: "=", exprs: [
            { type: "field", tableAlias: "1_2", column: "t1" }
            { type: "field", tableAlias: "T1", column: "primary" }
            ]}
          limit: 1
        })

    it "compiles scalar with one join and last aggr", ->
      @compile(
        { type: "scalar", table: "t1", expr: { type: "field", table: "t2", column: "number" }, joins: ["1-2"], aggr: "last" }
        {
          type: "scalar"
          expr: { type: "field", tableAlias: "1_2", column: "number" }
          from: { type: "table", table: "t2", alias: "1_2" }
          where: { type: "op", op: "=", exprs: [
            { type: "field", tableAlias: "1_2", column: "t1" }
            { type: "field", tableAlias: "T1", column: "primary" }
            ]}
          orderBy: [{ expr: { type: "field", tableAlias: "1_2", column: "number" }, direction: "desc" }]
          limit: 1
        }
      )

    it "compiles scalar with two joins", -> 
      @compile(
        { type: "scalar", table: "t1", expr: { type: "field", table: "t1", column: "number" }, joins: ["1-2", "2-1"], aggr: "count" }
        {
          type: "scalar"
          expr: { type: "op", op: "count", exprs: [{ type: "field", tableAlias: "2_1", column: "number" }] }
          from: { 
            type: "join" 
            left: { type: "table", table: "t2", alias: "1_2" }
            right: { type: "table", table: "t1", alias: "2_1" }
            kind: "inner"
            on: { type: "op", op: "=", exprs: [
              { type: "field", tableAlias: "2_1", column: "primary" }
              { type: "field", tableAlias: "1_2", column: "t1" }
              ]}
            } 
          where: { type: "op", op: "=", exprs: [
            { type: "field", tableAlias: "1_2", column: "t1" }
            { type: "field", tableAlias: "T1", column: "primary" }
            ]}
          limit: 1
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
          expr: { type: "field", tableAlias: "1_2", column: "number" }
          from: { type: "table", table: "t2", alias: "1_2" }
          where: {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: "=", exprs: [
                { type: "field", tableAlias: "1_2", column: "t1" }
                { type: "field", tableAlias: "T1", column: "primary" }
                ]
              }
              {
                type: "op", op: "=", exprs: [
                  { type: "field", tableAlias: "1_2", column: "number" }
                  { type: "literal", value: 3 }
                ]
              }
            ]
          }
          limit: 1
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
                      { type: "op", op: "to_jsonb", exprs: [{ type: "field", tableAlias: "T1", column: "enumset" }]}
                      { type: "op", op: "::jsonb", exprs: [{ type: "literal", value: '["a"]' }]}
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
                      { type: "op", op: "to_jsonb", exprs: [{ type: "field", tableAlias: "T1", column: "enumset" }] }
                      { type: "op", op: "::jsonb", exprs: [{ type: "literal", value: '["b"]' }]}
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

  describe "build enumset", ->
    it "builds", ->
      @compile(
        { 
          type: "build enumset"
          values: { 
            a: { type: "literal", valueType: "boolean", value: true }
            b: { type: "literal", valueType: "boolean", value: false }
          }
        }
        # (select to_jsonb(array_agg(bes.v)) from (select (case when true then 'x' end) as v union all select (case when true then 'y' end) as v union all select (case when false then 'z' end) as v) as bes where v is not null)
        {
          type: "scalar"
          # to_jsonb(array_agg(bes.v))
          expr: { type: "op", op: "to_jsonb", exprs: [{ type: "op", op: "array_agg", exprs: [{ type: "field", tableAlias: "bes", column: "v" }] }] }
          from: { 
            type: "subquery"
            query: {
              type: "union all"
              queries: [
                # Each is a "(select (case when true then 'x' end) as v)"
                { type: "query", selects: [{ type: "select", expr: { type: "case", cases: [{ when: { type: "literal", value: true }, then: "a" }] }, alias: "v" }] }
                { type: "query", selects: [{ type: "select", expr: { type: "case", cases: [{ when: { type: "literal", value: false }, then: "b" }] }, alias: "v" }] }
              ]
            }
            alias: "bes"
          }
          # Skip any null values
          where: {
            type: "op"
            op: "is not null"
            exprs: [
              { type: "field", tableAlias: "bes", column: "v" }
            ]
          }
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
                      { type: "op", op: "to_jsonb", exprs: [{ type: "field", tableAlias: "T1", column: "enumset" }] }
                      { type: "op", op: "::jsonb", exprs: [{ type: "literal", value: '["a"]' }]}
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
                      { type: "op", op: "to_jsonb", exprs: [{ type: "field", tableAlias: "T1", column: "enumset" }] }
                      { type: "op", op: "::jsonb", exprs: [{ type: "literal", value: '["b"]' }]}
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



  it "simplifies scalar join to id where toColumn is primary key", ->
    @compile({ 
      type: "scalar", 
      table: "t2",
      expr: { type: "id", table: "t1" }, 
      joins: ["2-1"],
    }, { type: "field", tableAlias: "T1", column: "t1" })

  it "simplifies scalar join to id", ->
    @compile({ 
      type: "scalar", 
      table: "t2",
      expr: { type: "id", table: "t1" }, 
      joins: ["id"],
    }, { type: "field", tableAlias: "T1", column: "id" })

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

      @enum1 = { type: "literal", valueType: "text", value: "a" }
      @enum1JsonQL = { type: "literal", value: "a" }

      @date1 = { type: "literal", valueType: "date", value: "2014-01-01" }
      @date1JsonQL = { type: "literal", value: "2014-01-01" }
      @date2 = { type: "literal", valueType: "date", value: "2014-12-31" }
      @date2JsonQL = { type: "literal", value: "2014-12-31" }
      @date3 = { type: "literal", valueType: "date", value: "2015-01-01" }
      @date3JsonQL = { type: "literal", value: "2015-01-01" }

      @datetime1 = { type: "literal", valueType: "datetime", value: "2014-01-01T01:02:03Z" }
      @datetime1JsonQL = { type: "literal", value: "2014-01-01T01:02:03Z" }

      @datetime2 = { type: "literal", valueType: "datetime", value: "2015-01-01T01:02:03Z" }
      @datetime2JsonQL = { type: "literal", value: "2015-01-01T01:02:03Z" }

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

    it "compiles +", ->
      @compile(
        {
          type: "op"
          op: "+"
          exprs: [@number1, @number2, @number1, null]
        }
        {
          type: "op"
          op: "+"
          exprs: [
            { type: "op", op: "coalesce", exprs: [{ type: "op", op: "::decimal", exprs: [@number1JsonQL] }, 0] }
            { type: "op", op: "coalesce", exprs: [{ type: "op", op: "::decimal", exprs: [@number2JsonQL] }, 0] }
            { type: "op", op: "coalesce", exprs: [{ type: "op", op: "::decimal", exprs: [@number1JsonQL] }, 0] }
          ]
        }
      )

    it "compiles *", ->
      @compile(
        {
          type: "op"
          op: "*"
          exprs: [@number1, @number2, @number1, null]
        }
        {
          type: "op"
          op: "*"
          exprs: [
            { type: "op", op: "::decimal", exprs: [@number1JsonQL] }
            { type: "op", op: "::decimal", exprs: [@number2JsonQL] }
            { type: "op", op: "::decimal", exprs: [@number1JsonQL] }
          ]
        }
      )

    it "compiles -", ->
      @compile(
        {
          type: "op"
          op: "-"
          exprs: [@number1, @number2]
        }
        {
          type: "op"
          op: "-"
          exprs: [{ type: "op", op: "::decimal", exprs: [@number1JsonQL] }, { type: "op", op: "::decimal", exprs: [@number2JsonQL] }]
        }
      )

      @compile(
        {
          type: "op"
          op: "-"
          exprs: [null, @number2]
        }
        null
      )

    it "compiles /, avoiding divide by zero which is fatal", ->
      @compile(
        {
          type: "op"
          op: "/"
          exprs: [@number1, @number2]
        }
        {
          type: "op"
          op: "/"
          exprs: [@number1JsonQL, { type: "op", op: "::decimal", exprs: [{ type: "op", op: "nullif", exprs: [@number2JsonQL, 0] }] }]
        }
      )

      @compile(
        {
          type: "op"
          op: "/"
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
          exprs: [{
            type: "op"
            op: "coalesce"
            exprs: [@bool1JsonQL, false]
          }]
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

    it "compiles least", ->
      @compile(
        {
          type: "op"
          op: "least"
          exprs: [@number1, @number2]
        }
        {
          type: "op"
          op: "least"
          exprs: [
            @number1JsonQL
            @number2JsonQL
          ]
        }
      )

    it "compiles greatest", ->
      @compile(
        {
          type: "op"
          op: "greatest"
          exprs: [@number1, @number2]
        }
        {
          type: "op"
          op: "greatest"
          exprs: [
            @number1JsonQL
            @number2JsonQL
          ]
        }
      )

    it "compiles sum()", ->
      @compile(
        {
          type: "op"
          op: "sum"
          exprs: [@number1]
        }
        {
          type: "op"
          op: "sum"
          exprs: [@number1JsonQL]
        }
      )

    it "compiles array_agg", ->
      @compile(
        {
          type: "op"
          op: "array_agg"
          exprs: [@text1]
        }
        {
          type: "op"
          op: "array_agg"
          exprs: [@text1JsonQL]
        }
      )

    it "compiles last", ->
      text = { type: "field", table: "t2", column: "text" }
      textJsonQL = { type: "field", tableAlias: "T1", column: "text" }

      @compile(
        {
          type: "op"
          op: "last"
          table: "t2"
          exprs: [text]
        }
        {
          type: "op"
          op: "[]"
          exprs: [
            { type: "op", op: "array_agg", exprs: [textJsonQL], orderBy: [{ expr: { type: "field", tableAlias: "T1", column: "number" }, direction: "desc", nulls: "last"}]}
            1
          ]
        }
      )

    it "compiles last where", ->
      text = { type: "field", table: "t2", column: "text" }
      textJsonQL = { type: "field", tableAlias: "T1", column: "text" }
      cond = { type: "op", op: ">", exprs: [{ type: "field", table: "t2", column: "number" }, { type: "literal", valueType: "number", value: 3 }] }
      condJsonQL = { type: "op", op: ">", exprs: [{ type: "field", tableAlias: "T1", column: "number" }, { type: "literal", value: 3 }] }

      # Compiles to 
      # (array_agg((case when <condition> then <value> else null end) order by (case when <condition> then 0 else 1 end), <ordering> desc nulls last))[1]
      # Which prevents non-matching from appearing

      @compile(
        {
          type: "op"
          op: "last where"
          table: "t2"
          exprs: [text, cond]
        }
        {
          type: "op"
          op: "[]"
          exprs: [
            { 
              type: "op"
              op: "array_agg"
              exprs: [
                { type: "case", cases: [{ when: condJsonQL, then: textJsonQL }], else: null }
              ]
              orderBy: [
                { expr: { type: "case", cases: [{ when: condJsonQL, then: 0 }], else: 1 } }
                { expr: { type: "field", tableAlias: "T1", column: "number" }, direction: "desc", nulls: "last" }
              ]
            }
            1
          ]
        }
      )

    it "compiles previous", ->
      text = { type: "field", table: "t2", column: "text" }
      textJsonQL = { type: "field", tableAlias: "T1", column: "text" }

      @compile(
        {
          type: "op"
          op: "previous"
          table: "t2"
          exprs: [text]
        }
        {
          type: "op"
          op: "[]"
          exprs: [
            { type: "op", op: "array_agg", exprs: [textJsonQL], orderBy: [{ expr: { type: "field", tableAlias: "T1", column: "number" }, direction: "desc", nulls: "last"}]}
            2
          ]
        }
      )

    it "compiles first", ->
      text = { type: "field", table: "t2", column: "text" }
      textJsonQL = { type: "field", tableAlias: "T1", column: "text" }

      @compile(
        {
          type: "op"
          op: "first"
          table: "t2"
          exprs: [text]
        }
        {
          type: "op"
          op: "[]"
          exprs: [
            { type: "op", op: "array_agg", exprs: [textJsonQL], orderBy: [{ expr: { type: "field", tableAlias: "T1", column: "number" }, direction: "asc", nulls: "last"}]}
            1
          ]
        }
      )

    it "compiles first where", ->
      text = { type: "field", table: "t2", column: "text" }
      textJsonQL = { type: "field", tableAlias: "T1", column: "text" }
      cond = { type: "op", op: ">", exprs: [{ type: "field", table: "t2", column: "number" }, { type: "literal", valueType: "number", value: 3 }] }
      condJsonQL = { type: "op", op: ">", exprs: [{ type: "field", tableAlias: "T1", column: "number" }, { type: "literal", value: 3 }] }

      # Compiles to 
      # (array_agg((case when <condition> then <value> else null end) order by (case when <condition> then 0 else 1 end), <ordering> asc nulls last))[1]
      # Which prevents non-matching from appearing

      @compile(
        {
          type: "op"
          op: "first where"
          table: "t2"
          exprs: [text, cond]
        }
        {
          type: "op"
          op: "[]"
          exprs: [
            { 
              type: "op"
              op: "array_agg"
              exprs: [
                { type: "case", cases: [{ when: condJsonQL, then: textJsonQL }], else: null }
              ]
              orderBy: [
                { expr: { type: "case", cases: [{ when: condJsonQL, then: 0 }], else: 1 } }
                { expr: { type: "field", tableAlias: "T1", column: "number" }, direction: "asc", nulls: "last" }
              ]
            }
            1
          ]
        }
      )

    it "compiles percent", ->
      # Compiles as count(*) * 100::decimal / sum(count(*)) over()
      @compile(
        {
          type: "op"
          op: "percent"
          table: "t2"
          exprs: []
        }
        {
          type: "op"
          op: "/"
          exprs: [
            {
              type: "op"
              op: "*"
              exprs: [
                { type: "op", op: "count", exprs: [] }
                { type: "op", op: "::decimal", exprs: [100] }
              ]
            }
            { 
              type: "op"
              op: "sum"
              exprs: [
                { type: "op", op: "count", exprs: [] }
              ]
              over: {}
            }
          ]
        }
      )

    it "compiles count where", ->
      cond = { type: "op", op: ">", exprs: [{ type: "field", table: "t2", column: "number" }, { type: "literal", valueType: "number", value: 3 }] }
      condJsonQL = { type: "op", op: ">", exprs: [{ type: "field", tableAlias: "T1", column: "number" }, { type: "literal", value: 3 }] }

      # Compiles as coalesce(sum(case when cond then 1 else 0 end), 0)
      @compile(
        {
          type: "op"
          op: "count where"
          table: "t2"
          exprs: [cond]
        }
        {
          type: "op"
          op: "coalesce"
          exprs: [
            {
              type: "op"
              op: "sum"
              exprs: [
                { 
                  type: "case"
                  cases: [
                    when: condJsonQL
                    then: 1
                  ]
                  else: 0
                }
              ]
            }
            0
          ]
        }
      )

    it "compiles percent where", ->
      cond = { type: "op", op: ">", exprs: [{ type: "field", table: "t2", column: "number" }, { type: "literal", valueType: "number", value: 3 }] }
      condJsonQL = { type: "op", op: ">", exprs: [{ type: "field", tableAlias: "T1", column: "number" }, { type: "literal", value: 3 }] }

      # Compiles as sum(case when cond then 100::decimal else 0 end) * 100/sum(1) (prevent div by zero)
      @compile(
        {
          type: "op"
          op: "percent where"
          table: "t2"
          exprs: [cond]
        }
        {
          type: "op"
          op: "/"
          exprs: [
            {
              type: "op"
              op: "sum"
              exprs: [
                { 
                  type: "case"
                  cases: [
                    when: condJsonQL
                    then: { type: "op", op: "::decimal", exprs: [100] }
                  ]
                  else: 0
                }
              ]
            }
            { type: "op", op: "sum", exprs: [1] }
          ]
        }
      )

    it "compiles percent where with of", ->
      cond = { type: "op", op: ">", exprs: [{ type: "field", table: "t2", column: "number" }, { type: "literal", valueType: "number", value: 3 }] }
      condJsonQL = { type: "op", op: ">", exprs: [{ type: "field", tableAlias: "T1", column: "number" }, { type: "literal", value: 3 }] }

      cond2 = { type: "op", op: ">", exprs: [{ type: "field", table: "t2", column: "number" }, { type: "literal", valueType: "number", value: 2 }] }
      cond2JsonQL = { type: "op", op: ">", exprs: [{ type: "field", tableAlias: "T1", column: "number" }, { type: "literal", value: 2 }] }

      # Compiles as sum(case when cond then 100 else 0 end)/nullif(sum(case when cond and cond2 then 1 else 0), 0) (prevent div by zero)
      @compile(
        {
          type: "op"
          op: "percent where"
          table: "t2"
          exprs: [cond, cond2]
        }
        {
          type: "op"
          op: "/"
          exprs: [
            {
              type: "op"
              op: "sum"
              exprs: [
                { 
                  type: "case"
                  cases: [
                    when: { type: "op", op: "and", exprs: [condJsonQL, cond2JsonQL] }
                    then: { type: "op", op: "::decimal", exprs: [100] }
                  ]
                  else: 0
                }
              ]
            }
            { 
              type: "op"
              op: "nullif"
              exprs: [
                {
                  type: "op"
                  op: "sum"
                  exprs: [
                    { 
                      type: "case"
                      cases: [
                        when: cond2JsonQL
                        then: 1
                      ]
                      else: 0
                    }
                  ]
                }
                0
              ]
            }
          ]
        }
      )

    it "compiles sum where", ->
      value = { type: "field", table: "t1", column: "number" }
      valueJsonQL = { type: "field", tableAlias: "T1", column: "number" }

      cond = { type: "op", op: ">", exprs: [{ type: "field", table: "t2", column: "number" }, { type: "literal", valueType: "number", value: 3 }] }
      condJsonQL = { type: "op", op: ">", exprs: [{ type: "field", tableAlias: "T1", column: "number" }, { type: "literal", value: 3 }] }

      # Compiles as sum(case when cond then 1 else 0 end)
      @compile(
        {
          type: "op"
          op: "sum where"
          table: "t2"
          exprs: [value, cond]
        }
        {
          type: "op"
          op: "sum"
          exprs: [
            { 
              type: "case"
              cases: [
                when: condJsonQL
                then: valueJsonQL
              ]
              else: 0
            }
          ]
        }
      )

    it "compiles min where", ->
      value = { type: "field", table: "t1", column: "number" }
      valueJsonQL = { type: "field", tableAlias: "T1", column: "number" }

      cond = { type: "op", op: ">", exprs: [{ type: "field", table: "t2", column: "number" }, { type: "literal", valueType: "number", value: 3 }] }
      condJsonQL = { type: "op", op: ">", exprs: [{ type: "field", tableAlias: "T1", column: "number" }, { type: "literal", value: 3 }] }

      # Compiles as min(case when cond then 1 else null end)
      @compile(
        {
          type: "op"
          op: "min where"
          table: "t2"
          exprs: [value, cond]
        }
        {
          type: "op"
          op: "min"
          exprs: [
            { 
              type: "case"
              cases: [
                when: condJsonQL
                then: valueJsonQL
              ]
              else: null
            }
          ]
        }
      )

    it "compiles max where", ->
      value = { type: "field", table: "t1", column: "number" }
      valueJsonQL = { type: "field", tableAlias: "T1", column: "number" }

      cond = { type: "op", op: ">", exprs: [{ type: "field", table: "t2", column: "number" }, { type: "literal", valueType: "number", value: 3 }] }
      condJsonQL = { type: "op", op: ">", exprs: [{ type: "field", tableAlias: "T1", column: "number" }, { type: "literal", value: 3 }] }

      # Compiles as max(case when cond then 1 else null end)
      @compile(
        {
          type: "op"
          op: "max where"
          table: "t2"
          exprs: [value, cond]
        }
        {
          type: "op"
          op: "max"
          exprs: [
            { 
              type: "case"
              cases: [
                when: condJsonQL
                then: valueJsonQL
              ]
              else: null
            }
          ]
        }
      )

    it "compiles count distinct", ->
      # Compiles as count(distinct value)
      value = { type: "field", table: "t1", column: "text" }
      valueJsonQL = { type: "field", tableAlias: "T1", column: "text" }

      @compile(
        {
          type: "op"
          op: "count distinct"
          table: "t1"
          exprs: [value]
        }
        {
          type: "op"
          op: "count"
          modifier: "distinct"
          exprs: [valueJsonQL]
        }
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
        false
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
            { type: "op", op: "to_jsonb", exprs: [{ type: "field", tableAlias: "T1", column: "enumset" }] }
            { type: "op", op: "::jsonb", exprs: [{ type: "literal", value: '["a","b"]' }]}
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

    it "compiles includes", ->
      @compile(
        { 
          type: "op"
          op: "includes", 
          exprs: [
            { type: "field", table: "t1", column: "enumset" } 
            { type: "literal", valueType: "enum", value: "a" }
          ]
        }
        {
          type: "op"
          op: "@>"
          exprs: [
            { type: "op", op: "to_jsonb", exprs: [{ type: "field", tableAlias: "T1", column: "enumset" }] }
            { type: "op", op: "::jsonb", exprs: [{ type: "literal", value: '"a"' }]}
          ]
        }
      )

    it "compiles intersects", ->
      @compile(
        { 
          type: "op"
          op: "intersects", 
          exprs: [
            { type: "field", table: "t1", column: "enumset" } 
            { type: "literal", valueType: "enumset", value: ["a", "b"] }
          ]
        }
        {
          type: "scalar",
          expr: { type: "op", op: "bool_or", exprs: [{ type: "field", tableAlias: "elements", column: "value" }] },
          from: { 
            type: "subquery",
            alias: "elements",
            query: {
              type: "query",
              selects: [
                { 
                  type: "select", 
                  expr: { type: "op", op: "@>", exprs: [
                    { type: "op", op: "to_jsonb", exprs: [{ type: "field", tableAlias: "T1", column: "enumset" }] }
                    { type: "op", op: "jsonb_array_elements", exprs: [{ type: "op", op: "::jsonb", exprs: [{ type: "literal", value: '["a","b"]' }]}] }
                  ]}, 
                  alias: "value" 
                }
              ]
            }
          }
        }        
      )

    it "compiles length", ->
      @compile(
        { 
          type: "op"
          op: "length", 
          exprs: [
            { type: "field", table: "t1", column: "enumset" } 
          ]
        }
        { 
          type: "op"
          op: "coalesce"
          exprs: [
            {
              type: "op"
              op: "jsonb_array_length"
              exprs: [
                { type: "op", op: "to_jsonb", exprs: [{ type: "field", tableAlias: "T1", column: "enumset" }] }
              ]
            }
            0
          ]
        }
      )

    it "compiles line length", ->
      # ST_Length_Spheroid(ST_Transform(location,4326), 'SPHEROID["GRS_1980",6378137,298.257222101]'::text)
      @compile(
        { 
          type: "op"
          op: "line length", 
          exprs: [
            { type: "field", table: "t1", column: "geometry" } 
          ]
        }
        { 
          type: "op"
          op: "ST_LengthSpheroid"
          exprs: [
            {
              type: "op"
              op: "ST_Transform"
              exprs: [
                { type: "field", tableAlias: "T1", column: "geometry" }
                { type: "op", op: "::integer", exprs: [4326] }
              ]
            }
            { type: "op", op: "::spheroid", exprs: ['SPHEROID["GRS_1980",6378137,298.257222101]'] }
          ]
        }
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
              op: "ST_Centroid"
              exprs: [
                { 
                  type: "op"
                  op: "ST_Transform"
                  exprs: [
                    { type: "field", tableAlias: "T1", column: "geometry" }
                    { type: "op", op: "::integer", exprs: [4326] }
                  ]
                }
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
              op: "ST_Centroid"
              exprs: [
                { 
                  type: "op"
                  op: "ST_Transform"
                  exprs: [
                    { type: "field", tableAlias: "T1", column: "geometry" }
                    { type: "op", op: "::integer", exprs: [4326] }
                  ]
                }
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
          op: "exists"
          exprs: [
            {
              type: "scalar"
              expr: null
              from: { type: "table", table: "thier_ancestry", alias: "subwithin" }
              where: {
                type: "op"
                op: "and"
                exprs: [
                  { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "subwithin", column: "ancestor" }, { type: "literal", value: "123" }]}
                  { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "subwithin", column: "descendant" }, { type: "field", tableAlias: "T1", column: "primary" }]}
                ]
              }
            }
          ]
        }
      ) 

    it "compiles within any", ->
      @compile(
        {
          type: "op"
          op: "within any"
          exprs: [{ type: "id", table: "thier" }, { type: "literal", valueType: "id[]", idTable: "thier", value: [123, 456] }]
        }
        { 
          type: "op"
          op: "exists"
          exprs: [
            {
              type: "scalar"
              expr: null
              from: { type: "table", table: "thier_ancestry", alias: "subwithin" }
              where: {
                type: "op"
                op: "and"
                exprs: [
                  { type: "op", op: "=", modifier: "any", exprs: [{ type: "field", tableAlias: "subwithin", column: "ancestor" }, { type: "literal", value: [123, 456] }]}
                  { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "subwithin", column: "descendant" }, { type: "field", tableAlias: "T1", column: "primary" }]}
                ]
              }
            }
          ]
        }
      ) 

    it "compiles current date", ->
      @compile(
        {
          type: "op"
          op: "current date"
          exprs: []
        }
        {
          type: "literal"
          value: moment().format("YYYY-MM-DD")
        }
      )

    it "compiles current datetime", ->
      jsonql = @ec.compileExpr(expr: { type: "op", op: "current datetime", exprs: [] }, tableAlias: "T1")
      assert.equal jsonql.type, "literal"

    # # DEPRECATED. Use ancestryTable
    # it "compiles within", ->
    #   @compile(
    #     {
    #       type: "op"
    #       op: "within"
    #       exprs: [{ type: "id", table: "thier" }, { type: "literal", valueType: "id", idTable: "thier", value: "123" }]
    #     }
    #     { 
    #       type: "op"
    #       op: "in"
    #       exprs: [
    #         { type: "field", tableAlias: "T1", column: "primary" }
    #         {
    #           type: "scalar"
    #           expr: { type: "field", tableAlias: "subwithin", column: "primary" }
    #           from: { type: "table", table: "thier", alias: "subwithin" }
    #           where: {
    #             type: "op"
    #             op: "@>"
    #             exprs: [
    #               { type: "field", tableAlias: "subwithin", column: "path" }
    #               { type: "op", op: "::jsonb", exprs: [{ type: "op", op: "json_build_array", exprs: [{ type: "literal", value: "123" }] }] }
    #             ]
    #           } 
    #         }
    #       ]
    #     }
    #   ) 

    # # DEPRECATED. Use ancestryTable
    # it "compiles within any", ->
    #   @compile(
    #     {
    #       type: "op"
    #       op: "within any"
    #       exprs: [{ type: "id", table: "thier" }, { type: "literal", valueType: "id[]", idTable: "thier", value: [123, 456] }]
    #     }
    #     { 
    #       type: "op"
    #       op: "in"
    #       exprs: [
    #         { type: "field", tableAlias: "T1", column: "primary" }
    #         {
    #           type: "scalar"
    #           expr: { type: "field", tableAlias: "subwithin", column: "primary" }
    #           from: { type: "table", table: "thier", alias: "subwithin" }
    #           where: {
    #             type: "op"
    #             op: "?|"
    #             exprs: [
    #               { type: "field", tableAlias: "subwithin", column: "path_text" }
    #               { type: "literal", value: ["123", "456"] }
    #             ]
    #           }
    #         }
    #       ]
    #     }
    #   ) 

    it "compiles days difference (date)", ->
      @compile(
        {
          type: "op"
          op: "days difference"
          exprs: [@date1, @date2]
        }
        {
          type: "op"
          op: "-"
          exprs: [
            { type: "op", op: "::date", exprs: [@date1JsonQL]}
            { type: "op", op: "::date", exprs: [@date2JsonQL]}
          ]
        }
      )

    it "compiles days difference (datetime)", ->
      @compile(
        {
          type: "op"
          op: "days difference"
          exprs: [@datetime1, @datetime2]
        }
        {
          type: "op"
          op: "/"
          exprs: [
            {
              type: "op"
              op: "-"
              exprs: [
                { type: "op", op: "date_part", exprs: ['epoch', { type: "op", op: "::timestamp", exprs: [@datetime1JsonQL] }]}
                { type: "op", op: "date_part", exprs: ['epoch', { type: "op", op: "::timestamp", exprs: [@datetime2JsonQL] }]}
              ]
            }
            86400
          ]
        }
      )

    it "compiles months difference (date)", ->
      @compile(
        {
          type: "op"
          op: "months difference"
          exprs: [@date1, @date2]
        }
        {
          type: "op"
          op: "/"
          exprs: [
            {
              type: "op"
              op: "-"
              exprs: [
                { type: "op", op: "::date", exprs: [@date1JsonQL]}
                { type: "op", op: "::date", exprs: [@date2JsonQL]}
              ]
            }
            30.5
          ]
        }
      )

    it "compiles months difference (datetime)", ->
      @compile(
        {
          type: "op"
          op: "months difference"
          exprs: [@datetime1, @datetime2]
        }
        {
          type: "op"
          op: "/"
          exprs: [
            {
              type: "op"
              op: "-"
              exprs: [
                { type: "op", op: "date_part", exprs: ['epoch', { type: "op", op: "::timestamp", exprs: [@datetime1JsonQL] }]}
                { type: "op", op: "date_part", exprs: ['epoch', { type: "op", op: "::timestamp", exprs: [@datetime2JsonQL] }]}
              ]
            }
            86400 * 30.5
          ]
        }
      )

    it "compiles months difference (date)", ->
      @compile(
        {
          type: "op"
          op: "years difference"
          exprs: [@date1, @date2]
        }
        {
          type: "op"
          op: "/"
          exprs: [
            {
              type: "op"
              op: "-"
              exprs: [
                { type: "op", op: "::date", exprs: [@date1JsonQL]}
                { type: "op", op: "::date", exprs: [@date2JsonQL]}
              ]
            }
            365
          ]
        }
      )

    it "compiles months difference (datetime)", ->
      @compile(
        {
          type: "op"
          op: "years difference"
          exprs: [@datetime1, @datetime2]
        }
        {
          type: "op"
          op: "/"
          exprs: [
            {
              type: "op"
              op: "-"
              exprs: [
                { type: "op", op: "date_part", exprs: ['epoch', { type: "op", op: "::timestamp", exprs: [@datetime1JsonQL] }]}
                { type: "op", op: "date_part", exprs: ['epoch', { type: "op", op: "::timestamp", exprs: [@datetime2JsonQL] }]}
              ]
            }
            86400 * 365
          ]
        }
      )

    it "compiles enum to text", ->
      @compile(
        {
          type: "op"
          op: "to text"
          exprs: [{ type: "field", table: "t1", column: "enum" }]
        }
        {
          type: "case"
          input: { type: "field", tableAlias: "T1", column: "enum" }
          cases: [
            { when: { type: "literal", value: "a" }, then: { type: "literal", value: "A" } }
            { when: { type: "literal", value: "b" }, then: { type: "literal", value: "B" } }
          ]
        }
      )

    it "compiles number to text", ->
      @compile(
        {
          type: "op"
          table: "t1"
          op: "to text"
          exprs: [{ type: "field", table: "t1", column: "number" }]
        }
        {
          type: "op"
          op: "::text"
          exprs: [{ type: "field", tableAlias: "T1", column: "number" }]
        }
      )

    it "compiles text[] to text", ->
      @compile(
        {
          type: "op"
          table: "t1"
          op: "to text"
          exprs: [{ type: "field", table: "t1", column: "text[]" }]
        }
        {
          type: "op"
          op: "array_to_string"
          exprs: [
            { 
              type: "scalar"
              expr: {
                type: "op"
                op: "array_agg"
                exprs: [{ type: "field", tableAlias: "values" }]
              }
              from: {
                type: "subexpr"
                expr: {
                  type: "op"
                  op: "jsonb_array_elements_text"
                  exprs: [{ type: "op", op: "to_jsonb", exprs: [
                    { type: "field", tableAlias: "T1", column: "text[]" }
                  ]}]
                }
                alias: "values"
              }
            }
            # Requires explicit text type
            { type: "op", op: "::text", exprs: [', '] }
          ]
        }
      )

    it "compiles to number", ->
      @compile(
        {
          type: "op"
          table: "t1"
          op: "to number"
          exprs: [{ type: "field", table: "t1", column: "text" }]
        }
        {
          type: "case"
          cases: [
            {
              when: {
                type: "op"
                op: "~"
                exprs: [
                  { type: "field", tableAlias: "T1", column: "text" }
                  "^([0-9]+[.]?[0-9]*|[.][0-9]+)$"
                ]
              }
              then: {
                type: "op"
                op: "::numeric"
                exprs: [
                  { type: "op", op: "::text", exprs: [{ type: "field", tableAlias: "T1", column: "text" }] }
                ]
              }
            }
          ]
          else: { type: "literal", value: null }
        }
      )

    it "compiles weekofmonth", ->
      @compile(
        {
          type: "op"
          table: "t1"
          op: "weekofmonth"
          exprs: [{ type: "field", table: "t1", column: "date" }]
        }
        { type: "op", op: "to_char", exprs: [
          { type: "op", op: "::timestamp", exprs: [
            { type: "field", tableAlias: "T1", column: "date" }
          ]}
          "W"
        ]}
      )

    it "compiles dayofmonth", ->
      @compile(
        {
          type: "op"
          table: "t1"
          op: "dayofmonth"
          exprs: [{ type: "field", table: "t1", column: "date" }]
        }
        { type: "op", op: "to_char", exprs: [
          { type: "op", op: "::timestamp", exprs: [
            { type: "field", tableAlias: "T1", column: "date" }
          ]}
          "DD"
        ]}
      )

    it "compiles month", ->
      @compile(
        {
          type: "op"
          table: "t1"
          op: "month"
          exprs: [{ type: "field", table: "t1", column: "date" }]
        }
        {
          type: "op"
          op: "substr"
          exprs: [
            { type: "field", tableAlias: "T1", column: "date" }
            6
            2
          ]
        }
      )

    it "compiles yearmonth", ->
      @compile(
        {
          type: "op"
          table: "t1"
          op: "yearmonth"
          exprs: [{ type: "field", table: "t1", column: "date" }]
        }
        {
          type: "op"
          op: "rpad"
          exprs: [
            { type: "op", op: "substr", exprs: [{ type: "field", tableAlias: "T1", column: "date" }, 1, 7] }
            10
            "-01"
          ]
        }
      )

    it "compiles yearquarter", ->
      @compile(
        {
          type: "op"
          table: "t1"
          op: "yearquarter"
          exprs: [{ type: "field", table: "t1", column: "date" }]
        }
        {
          type: "op"
          op: "to_char"
          exprs: [
            { type: "op", op: "::date", exprs: [{ type: "field", tableAlias: "T1", column: "date" }] }
            "YYYY-Q"
          ]
        }
      )

    it "compiles yearweek", ->
      @compile(
        {
          type: "op"
          table: "t1"
          op: "yearweek"
          exprs: [{ type: "field", table: "t1", column: "date" }]
        }
        {
          type: "op"
          op: "to_char"
          exprs: [
            { type: "op", op: "::date", exprs: [{ type: "field", tableAlias: "T1", column: "date" }] }
            "IYYY-IW"
          ]
        }
      )

    it "compiles weekofyear", ->
      @compile(
        {
          type: "op"
          table: "t1"
          op: "weekofyear"
          exprs: [{ type: "field", table: "t1", column: "date" }]
        }
        {
          type: "op"
          op: "to_char"
          exprs: [
            { type: "op", op: "::date", exprs: [{ type: "field", tableAlias: "T1", column: "date" }] }
            "IW"
          ]
        }
      )

    it "compiles year", ->
      @compile(
        {
          type: "op"
          table: "t1"
          op: "year"
          exprs: [{ type: "field", table: "t1", column: "date" }]
        }
        {
          type: "op"
          op: "rpad"
          exprs: [
            { type: "op", op: "substr", exprs: [{ type: "field", tableAlias: "T1", column: "date" }, 1, 4] }
            10
            "-01-01"
          ]
        }
      )

    it "compiles to date", ->
      @compile(
        {
          type: "op"
          table: "t1"
          op: "to date"
          exprs: [{ type: "field", table: "t1", column: "datetime" }]
        }
        { 
          type: "op", 
          op: "substr", 
          exprs: [{ type: "field", tableAlias: "T1", column: "datetime" }, 1, 10] 
        }
      )

    it "compiles weekofmonth", ->
      @compile(
        {
          type: "op"
          table: "t1"
          op: "weekofmonth"
          exprs: [{ type: "field", table: "t1", column: "date" }]
        }
        { type: "op", op: "to_char", exprs: [
          { type: "op", op: "::timestamp", exprs: [
            { type: "field", tableAlias: "T1", column: "date" }
          ]}
          "W"
        ]}
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

      it "compiles days since (date)", ->
        @compile(
          {
            type: "op"
            op: "days since"
            exprs: [@date1]
          }
          {
            type: "op"
            op: "-"
            exprs: [
              { type: "op", op: "::date", exprs: [moment().format("YYYY-MM-DD")]}
              { type: "op", op: "::date", exprs: [@date1JsonQL]}
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

      it "last24hours", ->
        @compile(
          {
            type: "op"
            op: "last24hours"
            exprs: [@datetime1]
          }
          {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: ">=", exprs: [@datetime1JsonQL, nowMinus24HoursExpr]}
              { type: "op", op: "<=", exprs: [@datetime1JsonQL, nowExpr]}
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

        it "last3months", ->
        @compile(
          {
            type: "op"
            op: "last3months"
            exprs: [@datetime1]
          }
          {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: ">=", exprs: [@datetime1JsonQL, moment().subtract(2, "months").startOf('month').toISOString()]}
              { type: "op", op: "<", exprs: [@datetime1JsonQL, moment().startOf("day").add(1, "days").toISOString()]}
            ]
          }
        )

      it "last6months", ->
        @compile(
          {
            type: "op"
            op: "last6months"
            exprs: [@datetime1]
          }
          {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: ">=", exprs: [@datetime1JsonQL, moment().subtract(5, "months").startOf('month').toISOString()]}
              { type: "op", op: "<", exprs: [@datetime1JsonQL, moment().startOf("day").add(1, "days").toISOString()]}
            ]
          }
        )

      it "last12months", ->
        @compile(
          {
            type: "op"
            op: "last12months"
            exprs: [@datetime1]
          }
          {
            type: "op"
            op: "and"
            exprs: [
              { type: "op", op: ">=", exprs: [@datetime1JsonQL, moment().subtract(11, "months").startOf('month').toISOString()]}
              { type: "op", op: "<", exprs: [@datetime1JsonQL, moment().startOf("day").add(1, "days").toISOString()]}
            ]
          }
        )

      it "compiles days since (datetime)", ->
        @compile(
          {
            type: "op"
            op: "days since"
            exprs: [@datetime1]
          }
          {
            type: "op"
            op: "/"
            exprs: [
              {
                type: "op"
                op: "-"
                exprs: [
                  { type: "op", op: "date_part", exprs: ['epoch', { type: "op", op: "::timestamp", exprs: [nowExpr] }]}
                  { type: "op", op: "date_part", exprs: ['epoch', { type: "op", op: "::timestamp", exprs: [@datetime1JsonQL] }]}
                ]
              }
              86400
            ]
          }
        )

      it "future", ->
        @compile(
          {
            type: "op"
            op: "future"
            exprs: [@datetime1]
          }
          { 
            type: "op", 
            op: ">", 
            exprs: [@datetime1JsonQL, nowExpr]
          }
        )

      it "notfuture", ->
        @compile(
          {
            type: "op"
            op: "notfuture"
            exprs: [@datetime1]
          }
          { 
            type: "op", 
            op: "<=", 
            exprs: [@datetime1JsonQL, nowExpr]
          }
        )

    it "distance", ->
      @compile(
        {
          type: "op"
          op: "distance"
          exprs: [@geometry, @geometry]
        }
        # ST_DistanceSphere(ST_Transform(x, 4326::integer), ST_Transform(y, 4326::integer))
        {
          type: "op"
          op: "ST_DistanceSphere"
          exprs: [
            { type: "op", op: "ST_Transform", exprs: [@geometryJsonQL, { type: "op", op: "::integer", exprs: [4326] }] }
            { type: "op", op: "ST_Transform", exprs: [@geometryJsonQL, { type: "op", op: "::integer", exprs: [4326] }] }
          ]
        }
      )

    it "is latest", ->
      @compile(
        {
          type: "op"
          op: "is latest"
          table: "t2"  # t2 is ordered by number. Note that alias is still T1 in the testing, but just ignore that
          exprs: [@enum1, @bool1]
        }
        # _id in (select id from (select id, row_number() over (partition by EXPR1 order by ORDERING desc) as rn from the_table as innerrn where filter) as outerrn where outerrn.rn = 1)
        {
          type: "op"
          op: "in"
          exprs: [
            { type: "field", tableAlias: "T1", column: "primary" }    
            {
              type: "scalar"
              expr: { type: "field", tableAlias: "outerrn", column: "id" }
              from: {
                type: "subquery"
                query: {
                  type: "query"
                  selects: [
                    { type: "select", expr: { type: "field", tableAlias: "innerrn", column: "primary" }, alias: "id" }
                    { 
                      type: "select"
                      expr: {
                        type: "op"
                        op: "row_number"
                        exprs: []
                        over: {
                          partitionBy: [@enum1JsonQL]
                          orderBy: [{ expr: { type: "field", tableAlias: "innerrn", column: "number" }, direction: "desc" }]
                        }
                      }
                      alias: "rn" 
                    }
                  ]
                  from: { type: "table", table: "t2", alias: "innerrn" }
                  where: @bool1JsonQL
                }
                alias: "outerrn"
              }
              where: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "outerrn", column: "rn" }, 1]}
            }
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
          alias: "1_2"
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
       
  describe "variable", ->
    it "compiles literal", ->
      expr = { type: "variable", variableId: "varnumber" }
      @compile(expr, { type: "literal", value: 123 })

    it "compiles expression", ->
      expr = { type: "variable", variableId: "varnumberexpr", table: "t1" }
      @compile(expr, {
        type: "op"
        op: "+"
        exprs: [
          { type: "op", op: "coalesce", exprs:[{ type: "op", op: "::decimal", exprs: [{ type: "field", tableAlias: "T1", column: "number" }]}, 0] }
          { type: "op", op: "coalesce", exprs:[{ type: "op", op: "::decimal", exprs: [{ type: "literal", value: 2 }]}, 0] }
        ]
      })

  # describe "spatial join", ->
  #   it "compiles", ->
  #     expr = { 
  #       type: "spatial join"
  #       valueExpr: { type: "op", op: "count", table: "t2", exprs: [] }
  #       table: "t1"
  #       toTable: "t2"
  #       fromGeometryExpr: { type: "field", table: "t1", column: "geometry" }
  #       toGeometryExpr: { type: "field", table: "t2", column: "geometry" }
  #       radiusExpr: { type: "literal", valueType: "number", value: 10 }
  #       filterExpr: { type: "field", table: "t2", column: "boolean" }
  #     }

  #     radiusExpr = {
  #       type: "op"
  #       op: "/"
  #       exprs: [
  #         { type: "literal", value: 10 }
  #         { type: "op", op: "cos", exprs: [
  #           { type: "op", op: "/", exprs: [
  #             { type: "op", op: "ST_YMin", exprs: [
  #               { type: "op", op: "ST_Transform", exprs: [
  #                 { type: "field", tableAlias: "T1", column: "geometry" }
  #                 4326
  #               ]}
  #             ]}
  #             57.3
  #           ]}
  #         ]}
  #       ]
  #     }

  #     @compile(expr, { 
  #       type: "scalar",
  #       expr: { type: "op", op: "count", exprs: [] },
  #       from: { type: "table", table: "t2", alias: "spatial" }
  #       where: { type: "op", op: "and", exprs: [
  #         {
  #           type: "op"
  #           op: "&&"
  #           exprs: [
  #             { type: "field", tableAlias: "spatial", column: "geometry" }
  #             { type: "op", op: "ST_Expand", exprs: [
  #               { type: "field", tableAlias: "T1", column: "geometry" }
  #               radiusExpr
  #             ]}
  #           ]
  #         },
  #         {
  #           type: "op"
  #           op: "<="
  #           exprs: [
  #             { type: "op", op: "ST_Distance", exprs: [
  #               { type: "field", tableAlias: "spatial", column: "geometry" }
  #               { type: "field", tableAlias: "T1", column: "geometry" }
  #             ]}
  #             radiusExpr
  #           ]
  #         },
  #         { type: "field", tableAlias: "spatial", column: "boolean" }
  #       ]}
  #     })

  describe "extension", ->
    it "compiles", ->
      @compile({ type: "extension", extension: "test" }, { type: "literal", value: 4 })
