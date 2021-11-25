import { assert } from "chai"
import * as fixtures from "./fixtures"
import _ from "lodash"
import canonical from "canonical-json"
import moment from "moment"
import sinon from "sinon"
import { default as Schema } from "../src/Schema"
import { default as ExprCompiler } from "../src/ExprCompiler"
import ColumnNotFoundException from "../src/ColumnNotFoundException"
import { setupTestExtension } from "./extensionSetup"
import { Expr, Variable } from "../src"
import { JsonQLExpr, JsonQLQuery } from "jsonql"

setupTestExtension()

function compare(actual: any, expected: any) {
  assert.equal(
    canonical(actual),
    canonical(expected),
    "\ngot:" + canonical(actual) + "\nexp:" + canonical(expected) + "\n"
  )
}

// now expression (to_json(now() at time zone 'UTC')#>>'{}') as timestamp
const nowExpr = {
  type: "op",
  op: "#>>",
  exprs: [
    {
      type: "op",
      op: "to_json",
      exprs: [{ type: "op", op: "at time zone", exprs: [{ type: "op", op: "now", exprs: [] }, "UTC"] }]
    },
    "{}"
  ]
}

// to_json((now() - interval '24 hour') at time zone 'UTC')#>>'{}'
const nowMinus24HoursExpr = {
  type: "op",
  op: "#>>",
  exprs: [
    {
      type: "op",
      op: "to_json",
      exprs: [
        {
          type: "op",
          op: "at time zone",
          exprs: [
            {
              type: "op",
              op: "-",
              exprs: [
                { type: "op", op: "now", exprs: [] },
                { type: "op", op: "interval", exprs: [{ type: "literal", value: "24 hour" }] }
              ]
            },
            "UTC"
          ]
        }
      ]
    },
    "{}"
  ]
}

const variables: Variable[] = [
  {
    id: "varenum",
    name: { _base: "en", en: "Varenum" },
    type: "enum",
    enumValues: [
      { id: "a", name: { _base: "en", en: "A" } },
      { id: "b", name: { _base: "en", en: "B" } }
    ]
  },
  { id: "varnumber", name: { _base: "en", en: "Varnumber" }, type: "number" },
  { id: "varnumberexpr", name: { _base: "en", en: "Varnumberexpr" }, type: "number", table: "t1" }
]

const variableValues: { [variableId: string]: Expr } = {
  varenum: { type: "literal", valueType: "enum", value: "a" } as Expr,
  varnumber: { type: "literal", valueType: "number", value: 123 } as Expr,
  varnumberexpr: {
    type: "op",
    op: "+",
    table: "t1",
    exprs: [
      { type: "field", table: "t1", column: "number" },
      { type: "literal", valueType: "number", value: 2 }
    ]
  } as Expr
}

describe("ExprCompiler", function () {
  beforeEach(function () {
    this.ec = new ExprCompiler(fixtures.simpleSchema(), variables, variableValues)
    return (this.compile = (expr: any, expected: any) => {
      const jsonql = this.ec.compileExpr({ expr, tableAlias: "T1" })
      return compare(jsonql, expected)
    })
  })

  it("compiles literal", function () {
    return this.compile(
      { type: "literal", valueType: "number", value: 2 },
      {
        type: "literal",
        value: 2
      }
    )
  })

  it("compiles null literal", function () {
    return this.compile({ type: "literal", value: null }, null)
  })

  it("compiles field", function () {
    return this.compile(
      { type: "field", table: "t1", column: "number" },
      {
        type: "field",
        tableAlias: "T1",
        column: "number"
      }
    )
  })

  it("compiles expression field", function () {
    return this.compile(
      { type: "field", table: "t1", column: "expr_enum" },
      {
        type: "field",
        tableAlias: "T1",
        column: "enum"
      }
    )
  })

  it("compiles join (id[]) field", function () {
    return this.compile(
      { type: "field", table: "t1", column: "1-2" },
      {
        type: "scalar",
        // Wrap in jsonb array for easier handling
        expr: {
          type: "op",
          op: "to_jsonb",
          exprs: [{ type: "op", op: "array_agg", exprs: [{ type: "field", tableAlias: "inner", column: "primary" }] }]
        },
        from: { type: "table", table: "t2", alias: "inner" },
        where: {
          type: "op",
          op: "=",
          exprs: [
            { type: "field", tableAlias: "inner", column: "t1" },
            { type: "field", tableAlias: "T1", column: "primary" }
          ]
        },
        limit: 1
      }
    )
  })

  it("compiles join (id) field", function () {
    return this.compile(
      {
        type: "field",
        table: "t2",
        column: ["2-1"]
      },
      { type: "field", tableAlias: "T1", column: "t1" }
    )
  })

  // it("compiles jsonql primaryKey", function () {
  //   const schema = new Schema().addTable({
  //     id: "tpk",
  //     name: { _base: "en", en: "T1" },
  //     primaryKey: { type: "field", tableAlias: "{alias}", column: "primary" },
  //     contents: [{ id: "text", name: { _base: "en", en: "Text" }, type: "text" }]
  //   })

  //   const ec = new ExprCompiler(schema)
  //   const jsonql = ec.compileExpr({ expr: { type: "id", table: "tpk" }, tableAlias: "TPK" })
  //   return compare(jsonql, { type: "field", tableAlias: "TPK", column: "primary" })
  // })

  it("throws ColumnNotFoundException", function () {
    assert.throws(() => {
      return this.ec.compileExpr({ expr: { type: "field", table: "t1", column: "XYZ" }, tableAlias: "T1" })
    }, ColumnNotFoundException)
  })

  describe("case", function () {
    before(function () {
      this.bool1 = { type: "literal", valueType: "boolean", value: true }
      this.bool1JsonQL = { type: "literal", value: true }
      this.bool2 = { type: "literal", valueType: "boolean", value: false }
      this.bool2JsonQL = { type: "literal", value: false }

      this.number1 = { type: "literal", valueType: "number", value: 2 }
      this.number1JsonQL = { type: "literal", value: 2 }
      this.number2 = { type: "literal", valueType: "number", value: 3 }
      this.number2JsonQL = { type: "literal", value: 3 }
      this.number3 = { type: "literal", valueType: "number", value: 4 }
      return (this.number3JsonQL = { type: "literal", value: 4 })
    })

    it("compiles case", function () {
      return this.compile(
        {
          type: "case",
          table: "t1",
          cases: [
            { when: this.bool1, then: this.number1 },
            { when: this.bool2, then: this.number2 }
          ],
          else: this.number3
        },
        {
          type: "case",
          cases: [
            { when: this.bool1JsonQL, then: this.number1JsonQL },
            { when: this.bool2JsonQL, then: this.number2JsonQL }
          ],
          else: this.number3JsonQL
        }
      )
    })

    it("skips null whens", function () {
      return this.compile(
        {
          type: "case",
          table: "t1",
          cases: [
            { when: null, then: this.number1 },
            { when: this.bool2, then: this.number2 }
          ],
          else: this.number3
        },
        {
          type: "case",
          cases: [{ when: this.bool2JsonQL, then: this.number2JsonQL }],
          else: this.number3JsonQL
        }
      )
    })

    return it("skips if empty", function () {
      return this.compile(
        {
          type: "case",
          table: "t1",
          cases: [{ when: null, then: this.number1 }],
          else: this.number3
        },
        null
      )
    })
  })

  describe("scalar", function () {
    it("compiles scalar with no joins, simplifying", function () {
      return this.compile(
        { type: "scalar", table: "t1", expr: { type: "field", table: "t1", column: "number" }, joins: [] },
        { type: "field", tableAlias: "T1", column: "number" }
      )
    })

    it("compiles scalar with one join", function () {
      return this.compile(
        { type: "scalar", table: "t1", expr: { type: "field", table: "t2", column: "number" }, joins: ["1-2"] },
        {
          type: "scalar",
          expr: { type: "field", tableAlias: "1_2", column: "number" },
          from: { type: "table", table: "t2", alias: "1_2" },
          where: {
            type: "op",
            op: "=",
            exprs: [
              { type: "field", tableAlias: "1_2", column: "t1" },
              { type: "field", tableAlias: "T1", column: "primary" }
            ]
          },
          limit: 1
        }
      )
    })

    it("compiles scalar with one join that is through id", function () {
      return this.compile(
        { type: "scalar", table: "t1", expr: { type: "field", table: "t2", column: "number" }, joins: ["id"] },
        {
          type: "scalar",
          expr: { type: "field", tableAlias: "id", column: "number" },
          from: { type: "table", table: "t2", alias: "id" },
          where: {
            type: "op",
            op: "=",
            exprs: [
              { type: "field", tableAlias: "T1", column: "id" },
              { type: "field", tableAlias: "id", column: "primary" }
            ]
          },
          limit: 1
        }
      )
    })

    it("compiles scalar with one join that is through id[]", function () {
      return this.compile(
        { type: "scalar", table: "t1", expr: { type: "op", table: "t2", op: "count", exprs: [] }, joins: ["id[]"] },
        {
          type: "scalar",
          expr: { type: "op", op: "count", exprs: [] },
          from: { type: "table", table: "t2", alias: "id__" },
          where: {
            type: "op",
            op: "@>",
            exprs: [
              { type: "op", op: "to_jsonb", exprs: [{ type: "field", tableAlias: "T1", column: "id[]" }] },
              { type: "op", op: "to_jsonb", exprs: [{ type: "field", tableAlias: "id__", column: "primary" }] }
            ]
          },
          limit: 1
        }
      )
    })

    it("compiles scalar with one join and sql aggr", function () {
      return this.compile(
        {
          type: "scalar",
          table: "t1",
          expr: { type: "field", table: "t2", column: "number" },
          joins: ["1-2"],
          aggr: "count"
        },
        {
          type: "scalar",
          expr: { type: "op", op: "count", exprs: [{ type: "field", tableAlias: "1_2", column: "number" }] },
          from: { type: "table", table: "t2", alias: "1_2" },
          where: {
            type: "op",
            op: "=",
            exprs: [
              { type: "field", tableAlias: "1_2", column: "t1" },
              { type: "field", tableAlias: "T1", column: "primary" }
            ]
          },
          limit: 1
        }
      )
    })

    it("compiles scalar with one join and count(<primary key>) aggr", function () {
      return this.compile(
        { type: "scalar", table: "t1", expr: { type: "id", table: "t2" }, joins: ["1-2"], aggr: "count" },
        {
          type: "scalar",
          expr: { type: "op", op: "count", exprs: [{ type: "field", tableAlias: "1_2", column: "primary" }] },
          from: { type: "table", table: "t2", alias: "1_2" },
          where: {
            type: "op",
            op: "=",
            exprs: [
              { type: "field", tableAlias: "1_2", column: "t1" },
              { type: "field", tableAlias: "T1", column: "primary" }
            ]
          },
          limit: 1
        }
      )
    })

    it("compiles scalar with one join and last aggr", function () {
      return this.compile(
        {
          type: "scalar",
          table: "t1",
          expr: { type: "field", table: "t2", column: "number" },
          joins: ["1-2"],
          aggr: "last"
        },
        {
          type: "scalar",
          expr: { type: "field", tableAlias: "1_2", column: "number" },
          from: { type: "table", table: "t2", alias: "1_2" },
          where: {
            type: "op",
            op: "=",
            exprs: [
              { type: "field", tableAlias: "1_2", column: "t1" },
              { type: "field", tableAlias: "T1", column: "primary" }
            ]
          },
          orderBy: [{ expr: { type: "field", tableAlias: "1_2", column: "number" }, direction: "desc" }],
          limit: 1
        }
      )
    })

    it("compiles scalar with two joins", function () {
      return this.compile(
        {
          type: "scalar",
          table: "t1",
          expr: { type: "field", table: "t1", column: "number" },
          joins: ["1-2", "2-1"],
          aggr: "count"
        },
        {
          type: "scalar",
          expr: { type: "op", op: "count", exprs: [{ type: "field", tableAlias: "2_1", column: "number" }] },
          from: {
            type: "join",
            left: { type: "table", table: "t2", alias: "1_2" },
            right: { type: "table", table: "t1", alias: "2_1" },
            kind: "inner",
            on: {
              type: "op",
              op: "=",
              exprs: [
                { type: "field", tableAlias: "2_1", column: "primary" },
                { type: "field", tableAlias: "1_2", column: "t1" }
              ]
            }
          },
          where: {
            type: "op",
            op: "=",
            exprs: [
              { type: "field", tableAlias: "1_2", column: "t1" },
              { type: "field", tableAlias: "T1", column: "primary" }
            ]
          },
          limit: 1
        }
      )
    })

    return it("compiles scalar with one join and where", function () {
      const where = {
        type: "logical",
        op: "and",
        exprs: [
          {
            type: "comparison",
            lhs: {
              type: "scalar",
              baseTableId: "t2",
              expr: {
                type: "field",
                table: "t2",
                column: "number"
              },
              joins: []
            },
            op: "=",
            rhs: {
              type: "literal",
              valueType: "number",
              value: 3
            }
          }
        ]
      }

      return this.compile(
        {
          type: "scalar",
          table: "t1",
          expr: { type: "field", table: "t2", column: "number" },
          joins: ["1-2"],
          where
        },
        {
          type: "scalar",
          expr: { type: "field", tableAlias: "1_2", column: "number" },
          from: { type: "table", table: "t2", alias: "1_2" },
          where: {
            type: "op",
            op: "and",
            exprs: [
              {
                type: "op",
                op: "=",
                exprs: [
                  { type: "field", tableAlias: "1_2", column: "t1" },
                  { type: "field", tableAlias: "T1", column: "primary" }
                ]
              },
              {
                type: "op",
                op: "=",
                exprs: [
                  { type: "field", tableAlias: "1_2", column: "number" },
                  { type: "literal", value: 3 }
                ]
              }
            ]
          },
          limit: 1
        }
      )
    })
  })

  describe("score", function () {
    it("scores enum", function () {
      return this.compile(
        {
          type: "score",
          input: { type: "field", table: "t1", column: "enum" },
          scores: {
            a: { type: "literal", valueType: "number", value: 4 }
          }
        },
        // case T1.enum when 'a' then 4 else 0 end
        {
          type: "case",
          input: { type: "field", tableAlias: "T1", column: "enum" },
          cases: [{ when: { type: "literal", value: "a" }, then: { type: "literal", value: 4 } }],
          else: { type: "literal", value: 0 }
        }
      )
    })

    it("scores empty enum", function () {
      return this.compile(
        {
          type: "score",
          input: { type: "field", table: "t1", column: "enum" },
          scores: {}
        },
        { type: "literal", value: 0 }
      )
    })

    it("scores enumset", function () {
      return this.compile(
        {
          type: "score",
          input: { type: "field", table: "t1", column: "enumset" },
          scores: {
            a: { type: "literal", valueType: "number", value: 3 },
            b: { type: "literal", valueType: "number", value: 4 }
          }
        },
        // case when T1.enum  then 4 else 0 end
        {
          type: "op",
          op: "+",
          exprs: [
            {
              type: "case",
              cases: [
                {
                  when: {
                    type: "op",
                    op: "@>",
                    exprs: [
                      { type: "op", op: "to_jsonb", exprs: [{ type: "field", tableAlias: "T1", column: "enumset" }] },
                      { type: "op", op: "::jsonb", exprs: [{ type: "literal", value: '["a"]' }] }
                    ]
                  },
                  then: { type: "literal", value: 3 }
                }
              ],
              else: { type: "literal", value: 0 }
            },
            {
              type: "case",
              cases: [
                {
                  when: {
                    type: "op",
                    op: "@>",
                    exprs: [
                      { type: "op", op: "to_jsonb", exprs: [{ type: "field", tableAlias: "T1", column: "enumset" }] },
                      { type: "op", op: "::jsonb", exprs: [{ type: "literal", value: '["b"]' }] }
                    ]
                  },
                  then: { type: "literal", value: 4 }
                }
              ],
              else: { type: "literal", value: 0 }
            }
          ]
        }
      )
    })

    return it("scores empty enumset", function () {
      return this.compile(
        {
          type: "score",
          input: { type: "field", table: "t1", column: "enumset" },
          scores: {}
        },
        { type: "literal", value: 0 }
      )
    })
  })

  describe("build enumset", function () {
    it("builds", function () {
      return this.compile(
        {
          type: "build enumset",
          values: {
            a: { type: "literal", valueType: "boolean", value: true },
            b: { type: "literal", valueType: "boolean", value: false }
          }
        },
        // (select to_jsonb(array_agg(bes.v)) from (select (case when true then 'x' end) as v union all select (case when true then 'y' end) as v union all select (case when false then 'z' end) as v) as bes where v is not null)
        {
          type: "scalar",
          // to_jsonb(array_agg(bes.v))
          expr: {
            type: "op",
            op: "to_jsonb",
            exprs: [{ type: "op", op: "array_agg", exprs: [{ type: "field", tableAlias: "bes", column: "v" }] }]
          },
          from: {
            type: "subquery",
            query: {
              type: "union all",
              queries: [
                // Each is a "(select (case when true then 'x' end) as v)"
                {
                  type: "query",
                  selects: [
                    {
                      type: "select",
                      expr: { type: "case", cases: [{ when: { type: "literal", value: true }, then: "a" }] },
                      alias: "v"
                    }
                  ]
                },
                {
                  type: "query",
                  selects: [
                    {
                      type: "select",
                      expr: { type: "case", cases: [{ when: { type: "literal", value: false }, then: "b" }] },
                      alias: "v"
                    }
                  ]
                }
              ]
            },
            alias: "bes"
          },
          // Skip any null values
          where: {
            type: "op",
            op: "is not null",
            exprs: [{ type: "field", tableAlias: "bes", column: "v" }]
          }
        }
      )
    })

    it("scores empty enum", function () {
      return this.compile(
        {
          type: "score",
          input: { type: "field", table: "t1", column: "enum" },
          scores: {}
        },
        { type: "literal", value: 0 }
      )
    })

    return it("scores enumset", function () {
      return this.compile(
        {
          type: "score",
          input: { type: "field", table: "t1", column: "enumset" },
          scores: {
            a: { type: "literal", valueType: "number", value: 3 },
            b: { type: "literal", valueType: "number", value: 4 }
          }
        },
        // case when T1.enum  then 4 else 0 end
        {
          type: "op",
          op: "+",
          exprs: [
            {
              type: "case",
              cases: [
                {
                  when: {
                    type: "op",
                    op: "@>",
                    exprs: [
                      { type: "op", op: "to_jsonb", exprs: [{ type: "field", tableAlias: "T1", column: "enumset" }] },
                      { type: "op", op: "::jsonb", exprs: [{ type: "literal", value: '["a"]' }] }
                    ]
                  },
                  then: { type: "literal", value: 3 }
                }
              ],
              else: { type: "literal", value: 0 }
            },
            {
              type: "case",
              cases: [
                {
                  when: {
                    type: "op",
                    op: "@>",
                    exprs: [
                      { type: "op", op: "to_jsonb", exprs: [{ type: "field", tableAlias: "T1", column: "enumset" }] },
                      { type: "op", op: "::jsonb", exprs: [{ type: "literal", value: '["b"]' }] }
                    ]
                  },
                  then: { type: "literal", value: 4 }
                }
              ],
              else: { type: "literal", value: 0 }
            }
          ]
        }
      )
    })
  })

  it("simplifies scalar join to id where toColumn is primary key", function () {
    return this.compile(
      {
        type: "scalar",
        table: "t2",
        expr: { type: "id", table: "t1" },
        joins: ["2-1"]
      },
      { type: "field", tableAlias: "T1", column: "t1" }
    )
  })

  it("simplifies scalar join to id", function () {
    return this.compile(
      {
        type: "scalar",
        table: "t2",
        expr: { type: "id", table: "t1" },
        joins: ["id"]
      },
      { type: "field", tableAlias: "T1", column: "id" }
    )
  })

  it("compiles literals", function () {
    this.compile({ type: "literal", valueType: "text", value: "abc" }, { type: "literal", value: "abc" })
    this.compile({ type: "literal", valueType: "number", value: 123 }, { type: "literal", value: 123 })
    this.compile({ type: "literal", valueType: "enum", value: "id1" }, { type: "literal", value: "id1" })
    this.compile({ type: "literal", valueType: "boolean", value: true }, { type: "literal", value: true })
    return this.compile({ type: "literal", valueType: "boolean", value: true }, { type: "literal", value: true })
  })

  describe("ops", function () {
    before(function () {
      this.bool1 = { type: "literal", valueType: "boolean", value: true }
      this.bool1JsonQL = { type: "literal", value: true }
      this.bool2 = { type: "literal", valueType: "boolean", value: false }
      this.bool2JsonQL = { type: "literal", value: false }

      this.number1 = { type: "literal", valueType: "number", value: 2 }
      this.number1JsonQL = { type: "literal", value: 2 }
      this.number2 = { type: "literal", valueType: "number", value: 3 }
      this.number2JsonQL = { type: "literal", value: 3 }

      this.text1 = { type: "literal", valueType: "text", value: "a" }
      this.text1JsonQL = { type: "literal", value: "a" }
      this.text2 = { type: "literal", valueType: "text", value: "b" }
      this.text2JsonQL = { type: "literal", value: "b" }

      this.enum1 = { type: "literal", valueType: "text", value: "a" }
      this.enum1JsonQL = { type: "literal", value: "a" }

      this.date1 = { type: "literal", valueType: "date", value: "2014-01-01" }
      this.date1JsonQL = { type: "literal", value: "2014-01-01" }
      this.date2 = { type: "literal", valueType: "date", value: "2014-12-31" }
      this.date2JsonQL = { type: "literal", value: "2014-12-31" }
      this.date3 = { type: "literal", valueType: "date", value: "2015-01-01" }
      this.date3JsonQL = { type: "literal", value: "2015-01-01" }

      this.datetime1 = { type: "literal", valueType: "datetime", value: "2014-01-01T01:02:03Z" }
      this.datetime1JsonQL = { type: "literal", value: "2014-01-01T01:02:03Z" }

      this.datetime2 = { type: "literal", valueType: "datetime", value: "2015-01-01T01:02:03Z" }
      this.datetime2JsonQL = { type: "literal", value: "2015-01-01T01:02:03Z" }

      this.geometry = { type: "field", table: "t1", column: "geometry" }
      return (this.geometryJsonQL = { type: "field", tableAlias: "T1", column: "geometry" })
    })

    it("compiles and", function () {
      return this.compile(
        {
          type: "op",
          op: "and",
          exprs: [this.bool1, this.bool2]
        },
        {
          type: "op",
          op: "and",
          exprs: [this.bool1JsonQL, this.bool2JsonQL]
        }
      )
    })

    it("compiles or", function () {
      return this.compile(
        {
          type: "op",
          op: "or",
          exprs: [this.bool1, this.bool2]
        },
        {
          type: "op",
          op: "or",
          exprs: [this.bool1JsonQL, this.bool2JsonQL]
        }
      )
    })

    it("compiles or with nulls", function () {
      return this.compile(
        {
          type: "op",
          op: "or",
          exprs: [this.bool1, null]
        },
        {
          type: "op",
          op: "or",
          exprs: [this.bool1JsonQL]
        }
      )
    })

    it("compiles or with all nulls", function () {
      return this.compile(
        {
          type: "op",
          op: "or",
          exprs: [null, null]
        },
        null
      )
    })

    it("compiles +", function () {
      return this.compile(
        {
          type: "op",
          op: "+",
          exprs: [this.number1, this.number2, this.number1, null]
        },
        {
          type: "op",
          op: "+",
          exprs: [
            { type: "op", op: "coalesce", exprs: [{ type: "op", op: "::decimal", exprs: [this.number1JsonQL] }, 0] },
            { type: "op", op: "coalesce", exprs: [{ type: "op", op: "::decimal", exprs: [this.number2JsonQL] }, 0] },
            { type: "op", op: "coalesce", exprs: [{ type: "op", op: "::decimal", exprs: [this.number1JsonQL] }, 0] }
          ]
        }
      )
    })

    it("compiles *", function () {
      return this.compile(
        {
          type: "op",
          op: "*",
          exprs: [this.number1, this.number2, this.number1, null]
        },
        {
          type: "op",
          op: "*",
          exprs: [
            { type: "op", op: "::decimal", exprs: [this.number1JsonQL] },
            { type: "op", op: "::decimal", exprs: [this.number2JsonQL] },
            { type: "op", op: "::decimal", exprs: [this.number1JsonQL] }
          ]
        }
      )
    })

    it("compiles -", function () {
      this.compile(
        {
          type: "op",
          op: "-",
          exprs: [this.number1, this.number2]
        },
        {
          type: "op",
          op: "-",
          exprs: [
            { type: "op", op: "::decimal", exprs: [this.number1JsonQL] },
            { type: "op", op: "::decimal", exprs: [this.number2JsonQL] }
          ]
        }
      )

      return this.compile(
        {
          type: "op",
          op: "-",
          exprs: [null, this.number2]
        },
        null
      )
    })

    it("compiles /, avoiding divide by zero which is fatal", function () {
      this.compile(
        {
          type: "op",
          op: "/",
          exprs: [this.number1, this.number2]
        },
        {
          type: "op",
          op: "/",
          exprs: [
            this.number1JsonQL,
            { type: "op", op: "::decimal", exprs: [{ type: "op", op: "nullif", exprs: [this.number2JsonQL, 0] }] }
          ]
        }
      )

      return this.compile(
        {
          type: "op",
          op: "/",
          exprs: [null, this.number2]
        },
        null
      )
    })

    it("compiles between", function () {
      return this.compile(
        {
          type: "op",
          op: "between",
          exprs: [this.date1, this.date2, this.date3]
        },
        {
          type: "op",
          op: "between",
          exprs: [this.date1JsonQL, this.date2JsonQL, this.date3JsonQL]
        }
      )
    })

    it("compiles between with first null (null)", function () {
      return this.compile(
        {
          type: "op",
          op: "between",
          exprs: [null, this.date2, this.date3]
        },
        null
      )
    })

    it("compiles between with second null (<=)", function () {
      return this.compile(
        {
          type: "op",
          op: "between",
          exprs: [this.date1, null, this.date3]
        },
        {
          type: "op",
          op: "<=",
          exprs: [this.date1JsonQL, this.date3JsonQL]
        }
      )
    })

    it("compiles between with third null (>=)", function () {
      return this.compile(
        {
          type: "op",
          op: "between",
          exprs: [this.date1, this.date2, null]
        },
        {
          type: "op",
          op: ">=",
          exprs: [this.date1JsonQL, this.date2JsonQL]
        }
      )
    })

    it("compiles not", function () {
      this.compile(
        {
          type: "op",
          op: "not",
          exprs: [this.bool1]
        },
        {
          type: "op",
          op: "not",
          exprs: [
            {
              type: "op",
              op: "coalesce",
              exprs: [this.bool1JsonQL, false]
            }
          ]
        }
      )

      return this.compile(
        {
          type: "op",
          op: "not",
          exprs: [null]
        },
        null
      )
    })

    it("compiles =, <>, >, >=, <, <=", function () {
      return (() => {
        const result = []
        for (let op of ["=", "<>", ">", ">=", "<", "<="]) {
          // Normal
          this.compile(
            {
              type: "op",
              op,
              exprs: [this.number1, this.number2]
            },
            {
              type: "op",
              op,
              exprs: [this.number1JsonQL, this.number2JsonQL]
            }
          )

          // Missing value
          result.push(
            this.compile(
              {
                type: "op",
                op,
                exprs: [this.number1, null]
              },
              null
            )
          )
        }
        return result
      })()
    })

    it("compiles ~*", function () {
      this.compile(
        {
          type: "op",
          op: "~*",
          exprs: [this.text1, this.text2]
        },
        {
          type: "op",
          op: "~*",
          exprs: [this.text1JsonQL, this.text2JsonQL]
        }
      )

      // Missing value
      return this.compile(
        {
          type: "op",
          op: "~*",
          exprs: [this.text1, null]
        },
        null
      )
    })

    it("compiles least", function () {
      return this.compile(
        {
          type: "op",
          op: "least",
          exprs: [this.number1, this.number2]
        },
        {
          type: "op",
          op: "least",
          exprs: [this.number1JsonQL, this.number2JsonQL]
        }
      )
    })

    it("compiles greatest", function () {
      return this.compile(
        {
          type: "op",
          op: "greatest",
          exprs: [this.number1, this.number2]
        },
        {
          type: "op",
          op: "greatest",
          exprs: [this.number1JsonQL, this.number2JsonQL]
        }
      )
    })

    it("compiles sum()", function () {
      return this.compile(
        {
          type: "op",
          op: "sum",
          exprs: [this.number1]
        },
        {
          type: "op",
          op: "sum",
          exprs: [this.number1JsonQL]
        }
      )
    })

    it("compiles array_agg", function () {
      return this.compile(
        {
          type: "op",
          op: "array_agg",
          exprs: [this.text1]
        },
        {
          type: "op",
          op: "array_agg",
          exprs: [this.text1JsonQL]
        }
      )
    })

    it("compiles last", function () {
      const text = { type: "field", table: "t2", column: "text" }
      const textJsonQL = { type: "field", tableAlias: "T1", column: "text" }

      return this.compile(
        {
          type: "op",
          op: "last",
          table: "t2",
          exprs: [text]
        },
        {
          type: "op",
          op: "[]",
          exprs: [
            {
              type: "op",
              op: "array_agg",
              exprs: [textJsonQL],
              orderBy: [
                { expr: { type: "field", tableAlias: "T1", column: "number" }, direction: "desc", nulls: "last" }
              ]
            },
            1
          ]
        }
      )
    })

    it("compiles last where", function () {
      const text = { type: "field", table: "t2", column: "text" }
      const textJsonQL = { type: "field", tableAlias: "T1", column: "text" }
      const cond = {
        type: "op",
        op: ">",
        exprs: [
          { type: "field", table: "t2", column: "number" },
          { type: "literal", valueType: "number", value: 3 }
        ]
      }
      const condJsonQL = {
        type: "op",
        op: ">",
        exprs: [
          { type: "field", tableAlias: "T1", column: "number" },
          { type: "literal", value: 3 }
        ]
      }

      // Compiles to
      // (array_agg((case when <condition> then <value> else null end) order by (case when <condition> then 0 else 1 end), <ordering> desc nulls last))[1]
      // Which prevents non-matching from appearing

      return this.compile(
        {
          type: "op",
          op: "last where",
          table: "t2",
          exprs: [text, cond]
        },
        {
          type: "op",
          op: "[]",
          exprs: [
            {
              type: "op",
              op: "array_agg",
              exprs: [{ type: "case", cases: [{ when: condJsonQL, then: textJsonQL }], else: null }],
              orderBy: [
                { expr: { type: "case", cases: [{ when: condJsonQL, then: 0 }], else: 1 } },
                { expr: { type: "field", tableAlias: "T1", column: "number" }, direction: "desc", nulls: "last" }
              ]
            },
            1
          ]
        }
      )
    })

    it("compiles previous", function () {
      const text = { type: "field", table: "t2", column: "text" }
      const textJsonQL = { type: "field", tableAlias: "T1", column: "text" }

      return this.compile(
        {
          type: "op",
          op: "previous",
          table: "t2",
          exprs: [text]
        },
        {
          type: "op",
          op: "[]",
          exprs: [
            {
              type: "op",
              op: "array_agg",
              exprs: [textJsonQL],
              orderBy: [
                { expr: { type: "field", tableAlias: "T1", column: "number" }, direction: "desc", nulls: "last" }
              ]
            },
            2
          ]
        }
      )
    })

    it("compiles first", function () {
      const text = { type: "field", table: "t2", column: "text" }
      const textJsonQL = { type: "field", tableAlias: "T1", column: "text" }

      return this.compile(
        {
          type: "op",
          op: "first",
          table: "t2",
          exprs: [text]
        },
        {
          type: "op",
          op: "[]",
          exprs: [
            {
              type: "op",
              op: "array_agg",
              exprs: [textJsonQL],
              orderBy: [
                { expr: { type: "field", tableAlias: "T1", column: "number" }, direction: "asc", nulls: "last" }
              ]
            },
            1
          ]
        }
      )
    })

    it("compiles first where", function () {
      const text = { type: "field", table: "t2", column: "text" }
      const textJsonQL = { type: "field", tableAlias: "T1", column: "text" }
      const cond = {
        type: "op",
        op: ">",
        exprs: [
          { type: "field", table: "t2", column: "number" },
          { type: "literal", valueType: "number", value: 3 }
        ]
      }
      const condJsonQL = {
        type: "op",
        op: ">",
        exprs: [
          { type: "field", tableAlias: "T1", column: "number" },
          { type: "literal", value: 3 }
        ]
      }

      // Compiles to
      // (array_agg((case when <condition> then <value> else null end) order by (case when <condition> then 0 else 1 end), <ordering> asc nulls last))[1]
      // Which prevents non-matching from appearing

      return this.compile(
        {
          type: "op",
          op: "first where",
          table: "t2",
          exprs: [text, cond]
        },
        {
          type: "op",
          op: "[]",
          exprs: [
            {
              type: "op",
              op: "array_agg",
              exprs: [{ type: "case", cases: [{ when: condJsonQL, then: textJsonQL }], else: null }],
              orderBy: [
                { expr: { type: "case", cases: [{ when: condJsonQL, then: 0 }], else: 1 } },
                { expr: { type: "field", tableAlias: "T1", column: "number" }, direction: "asc", nulls: "last" }
              ]
            },
            1
          ]
        }
      )
    })

    it("compiles percent", function () {
      // Compiles as count(*) * 100::decimal / sum(count(*)) over()
      return this.compile(
        {
          type: "op",
          op: "percent",
          table: "t2",
          exprs: []
        },
        {
          type: "op",
          op: "/",
          exprs: [
            {
              type: "op",
              op: "*",
              exprs: [
                { type: "op", op: "count", exprs: [] },
                { type: "op", op: "::decimal", exprs: [100] }
              ]
            },
            {
              type: "op",
              op: "sum",
              exprs: [{ type: "op", op: "count", exprs: [] }],
              over: {}
            }
          ]
        }
      )
    })

    it("compiles count where", function () {
      const cond = {
        type: "op",
        op: ">",
        exprs: [
          { type: "field", table: "t2", column: "number" },
          { type: "literal", valueType: "number", value: 3 }
        ]
      }
      const condJsonQL = {
        type: "op",
        op: ">",
        exprs: [
          { type: "field", tableAlias: "T1", column: "number" },
          { type: "literal", value: 3 }
        ]
      }

      // Compiles as coalesce(sum(case when cond then 1 else 0 end), 0)
      return this.compile(
        {
          type: "op",
          op: "count where",
          table: "t2",
          exprs: [cond]
        },
        {
          type: "op",
          op: "coalesce",
          exprs: [
            {
              type: "op",
              op: "sum",
              exprs: [
                {
                  type: "case",
                  cases: [
                    {
                      when: condJsonQL,
                      then: 1
                    }
                  ],
                  else: 0
                }
              ]
            },
            0
          ]
        }
      )
    })

    it("compiles percent where", function () {
      const cond = {
        type: "op",
        op: ">",
        exprs: [
          { type: "field", table: "t2", column: "number" },
          { type: "literal", valueType: "number", value: 3 }
        ]
      }
      const condJsonQL = {
        type: "op",
        op: ">",
        exprs: [
          { type: "field", tableAlias: "T1", column: "number" },
          { type: "literal", value: 3 }
        ]
      }

      // Compiles as sum(case when cond then 100::decimal else 0 end) * 100/sum(1) (prevent div by zero)
      return this.compile(
        {
          type: "op",
          op: "percent where",
          table: "t2",
          exprs: [cond]
        },
        {
          type: "op",
          op: "/",
          exprs: [
            {
              type: "op",
              op: "sum",
              exprs: [
                {
                  type: "case",
                  cases: [
                    {
                      when: condJsonQL,
                      then: { type: "op", op: "::decimal", exprs: [100] }
                    }
                  ],
                  else: 0
                }
              ]
            },
            { type: "op", op: "sum", exprs: [1] }
          ]
        }
      )
    })

    it("compiles percent where with of", function () {
      const cond = {
        type: "op",
        op: ">",
        exprs: [
          { type: "field", table: "t2", column: "number" },
          { type: "literal", valueType: "number", value: 3 }
        ]
      }
      const condJsonQL = {
        type: "op",
        op: ">",
        exprs: [
          { type: "field", tableAlias: "T1", column: "number" },
          { type: "literal", value: 3 }
        ]
      }

      const cond2 = {
        type: "op",
        op: ">",
        exprs: [
          { type: "field", table: "t2", column: "number" },
          { type: "literal", valueType: "number", value: 2 }
        ]
      }
      const cond2JsonQL = {
        type: "op",
        op: ">",
        exprs: [
          { type: "field", tableAlias: "T1", column: "number" },
          { type: "literal", value: 2 }
        ]
      }

      // Compiles as sum(case when cond then 100 else 0 end)/nullif(sum(case when cond and cond2 then 1 else 0), 0) (prevent div by zero)
      return this.compile(
        {
          type: "op",
          op: "percent where",
          table: "t2",
          exprs: [cond, cond2]
        },
        {
          type: "op",
          op: "/",
          exprs: [
            {
              type: "op",
              op: "sum",
              exprs: [
                {
                  type: "case",
                  cases: [
                    {
                      when: { type: "op", op: "and", exprs: [condJsonQL, cond2JsonQL] },
                      then: { type: "op", op: "::decimal", exprs: [100] }
                    }
                  ],
                  else: 0
                }
              ]
            },
            {
              type: "op",
              op: "nullif",
              exprs: [
                {
                  type: "op",
                  op: "sum",
                  exprs: [
                    {
                      type: "case",
                      cases: [
                        {
                          when: cond2JsonQL,
                          then: 1
                        }
                      ],
                      else: 0
                    }
                  ]
                },
                0
              ]
            }
          ]
        }
      )
    })

    it("compiles sum where", function () {
      const value = { type: "field", table: "t1", column: "number" }
      const valueJsonQL = { type: "field", tableAlias: "T1", column: "number" }

      const cond = {
        type: "op",
        op: ">",
        exprs: [
          { type: "field", table: "t2", column: "number" },
          { type: "literal", valueType: "number", value: 3 }
        ]
      }
      const condJsonQL = {
        type: "op",
        op: ">",
        exprs: [
          { type: "field", tableAlias: "T1", column: "number" },
          { type: "literal", value: 3 }
        ]
      }

      // Compiles as sum(case when cond then 1 else 0 end)
      return this.compile(
        {
          type: "op",
          op: "sum where",
          table: "t2",
          exprs: [value, cond]
        },
        {
          type: "op",
          op: "sum",
          exprs: [
            {
              type: "case",
              cases: [
                {
                  when: condJsonQL,
                  then: valueJsonQL
                }
              ],
              else: 0
            }
          ]
        }
      )
    })

    it("compiles min where", function () {
      const value = { type: "field", table: "t1", column: "number" }
      const valueJsonQL = { type: "field", tableAlias: "T1", column: "number" }

      const cond = {
        type: "op",
        op: ">",
        exprs: [
          { type: "field", table: "t2", column: "number" },
          { type: "literal", valueType: "number", value: 3 }
        ]
      }
      const condJsonQL = {
        type: "op",
        op: ">",
        exprs: [
          { type: "field", tableAlias: "T1", column: "number" },
          { type: "literal", value: 3 }
        ]
      }

      // Compiles as min(case when cond then 1 else null end)
      return this.compile(
        {
          type: "op",
          op: "min where",
          table: "t2",
          exprs: [value, cond]
        },
        {
          type: "op",
          op: "min",
          exprs: [
            {
              type: "case",
              cases: [
                {
                  when: condJsonQL,
                  then: valueJsonQL
                }
              ],
              else: null
            }
          ]
        }
      )
    })

    it("compiles max where", function () {
      const value = { type: "field", table: "t1", column: "number" }
      const valueJsonQL = { type: "field", tableAlias: "T1", column: "number" }

      const cond = {
        type: "op",
        op: ">",
        exprs: [
          { type: "field", table: "t2", column: "number" },
          { type: "literal", valueType: "number", value: 3 }
        ]
      }
      const condJsonQL = {
        type: "op",
        op: ">",
        exprs: [
          { type: "field", tableAlias: "T1", column: "number" },
          { type: "literal", value: 3 }
        ]
      }

      // Compiles as max(case when cond then 1 else null end)
      return this.compile(
        {
          type: "op",
          op: "max where",
          table: "t2",
          exprs: [value, cond]
        },
        {
          type: "op",
          op: "max",
          exprs: [
            {
              type: "case",
              cases: [
                {
                  when: condJsonQL,
                  then: valueJsonQL
                }
              ],
              else: null
            }
          ]
        }
      )
    })

    it("compiles count distinct", function () {
      // Compiles as count(distinct value)
      const value = { type: "field", table: "t1", column: "text" }
      const valueJsonQL = { type: "field", tableAlias: "T1", column: "text" }

      return this.compile(
        {
          type: "op",
          op: "count distinct",
          table: "t1",
          exprs: [value]
        },
        {
          type: "op",
          op: "count",
          modifier: "distinct",
          exprs: [valueJsonQL]
        }
      )
    })

    it("compiles = any", function () {
      return this.compile(
        {
          type: "op",
          op: "= any",
          exprs: [
            { type: "field", table: "t1", column: "enum" },
            { type: "literal", valueType: "enumset", value: ["a", "b"] }
          ]
        },
        {
          type: "op",
          op: "=",
          modifier: "any",
          exprs: [
            { type: "field", tableAlias: "T1", column: "enum" },
            { type: "literal", value: ["a", "b"] }
          ]
        }
      )
    })

    it("compiles empty = any", function () {
      return this.compile(
        {
          type: "op",
          op: "= any",
          exprs: [
            { type: "field", table: "t1", column: "enum" },
            { type: "literal", valueType: "enumset", value: [] }
          ]
        },
        false
      )
    })

    it("compiles invalid = any", function () {
      return this.compile(
        {
          type: "op",
          op: "= any",
          exprs: [null, { type: "literal", valueType: "enumset", value: [] }]
        },
        null
      )
    })

    it("compiles is null", function () {
      this.compile(
        {
          type: "op",
          op: "is null",
          exprs: [this.number1]
        },
        {
          type: "op",
          op: "is null",
          exprs: [this.number1JsonQL]
        }
      )

      return this.compile(
        {
          type: "op",
          op: "is null",
          exprs: [null]
        },
        null
      )
    })

    it("compiles is not null", function () {
      this.compile(
        {
          type: "op",
          op: "is not null",
          exprs: [this.number1]
        },
        {
          type: "op",
          op: "is not null",
          exprs: [this.number1JsonQL]
        }
      )

      return this.compile(
        {
          type: "op",
          op: "is not null",
          exprs: [null]
        },
        null
      )
    })

    it("compiles contains", function () {
      return this.compile(
        {
          type: "op",
          op: "contains",
          exprs: [
            { type: "field", table: "t1", column: "enumset" },
            { type: "literal", valueType: "enumset", value: ["a", "b"] }
          ]
        },
        {
          type: "op",
          op: "@>",
          exprs: [
            { type: "op", op: "to_jsonb", exprs: [{ type: "field", tableAlias: "T1", column: "enumset" }] },
            { type: "op", op: "::jsonb", exprs: [{ type: "literal", value: '["a","b"]' }] }
          ]
        }
      )
    })

    it("compiles empty contains", function () {
      return this.compile(
        {
          type: "op",
          op: "contains",
          exprs: [
            { type: "field", table: "t1", column: "enumset" },
            { type: "literal", valueType: "enumset", value: [] }
          ]
        },
        null
      )
    })

    it("compiles includes", function () {
      return this.compile(
        {
          type: "op",
          op: "includes",
          exprs: [
            { type: "field", table: "t1", column: "enumset" },
            { type: "literal", valueType: "enum", value: "a" }
          ]
        },
        {
          type: "op",
          op: "@>",
          exprs: [
            { type: "op", op: "to_jsonb", exprs: [{ type: "field", tableAlias: "T1", column: "enumset" }] },
            { type: "op", op: "::jsonb", exprs: [{ type: "literal", value: '"a"' }] }
          ]
        }
      )
    })

    it("compiles intersects", function () {
      return this.compile(
        {
          type: "op",
          op: "intersects",
          exprs: [
            { type: "field", table: "t1", column: "enumset" },
            { type: "literal", valueType: "enumset", value: ["a", "b"] }
          ]
        },
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
                  expr: {
                    type: "op",
                    op: "@>",
                    exprs: [
                      { type: "op", op: "to_jsonb", exprs: [{ type: "field", tableAlias: "T1", column: "enumset" }] },
                      {
                        type: "op",
                        op: "jsonb_array_elements",
                        exprs: [{ type: "op", op: "::jsonb", exprs: [{ type: "literal", value: '["a","b"]' }] }]
                      }
                    ]
                  },
                  alias: "value"
                }
              ]
            }
          }
        }
      )
    })

    it("compiles length", function () {
      return this.compile(
        {
          type: "op",
          op: "length",
          exprs: [{ type: "field", table: "t1", column: "enumset" }]
        },
        {
          type: "op",
          op: "coalesce",
          exprs: [
            {
              type: "op",
              op: "jsonb_array_length",
              exprs: [{ type: "op", op: "to_jsonb", exprs: [{ type: "field", tableAlias: "T1", column: "enumset" }] }]
            },
            0
          ]
        }
      )
    })

    it("compiles line length", function () {
      // ST_Length_Spheroid(ST_Transform(location,4326), 'SPHEROID["GRS_1980",6378137,298.257222101]'::text)
      return this.compile(
        {
          type: "op",
          op: "line length",
          exprs: [{ type: "field", table: "t1", column: "geometry" }]
        },
        {
          type: "op",
          op: "ST_LengthSpheroid",
          exprs: [
            {
              type: "op",
              op: "ST_Transform",
              exprs: [
                { type: "field", tableAlias: "T1", column: "geometry" },
                { type: "op", op: "::integer", exprs: [4326] }
              ]
            },
            { type: "op", op: "::spheroid", exprs: ['SPHEROID["GRS_1980",6378137,298.257222101]'] }
          ]
        }
      )
    })

    it("compiles latitude", function () {
      return this.compile(
        {
          type: "op",
          op: "latitude",
          exprs: [{ type: "field", table: "t1", column: "geometry" }]
        },
        {
          type: "op",
          op: "ST_Y",
          exprs: [
            {
              type: "op",
              op: "ST_Centroid",
              exprs: [
                {
                  type: "op",
                  op: "ST_Transform",
                  exprs: [
                    { type: "field", tableAlias: "T1", column: "geometry" },
                    { type: "op", op: "::integer", exprs: [4326] }
                  ]
                }
              ]
            }
          ]
        }
      )
    })

    it("compiles longitude", function () {
      return this.compile(
        {
          type: "op",
          op: "longitude",
          exprs: [{ type: "field", table: "t1", column: "geometry" }]
        },
        {
          type: "op",
          op: "ST_X",
          exprs: [
            {
              type: "op",
              op: "ST_Centroid",
              exprs: [
                {
                  type: "op",
                  op: "ST_Transform",
                  exprs: [
                    { type: "field", tableAlias: "T1", column: "geometry" },
                    { type: "op", op: "::integer", exprs: [4326] }
                  ]
                }
              ]
            }
          ]
        }
      )
    })

    it("compiles within", function () {
      return this.compile(
        {
          type: "op",
          op: "within",
          exprs: [
            { type: "id", table: "thier" },
            { type: "literal", valueType: "id", idTable: "thier", value: "123" }
          ]
        },
        {
          type: "op",
          op: "exists",
          exprs: [
            {
              type: "scalar",
              expr: null,
              from: { type: "table", table: "thier_ancestry", alias: "subwithin" },
              where: {
                type: "op",
                op: "and",
                exprs: [
                  {
                    type: "op",
                    op: "=",
                    exprs: [
                      { type: "field", tableAlias: "subwithin", column: "ancestor" },
                      { type: "literal", value: "123" }
                    ]
                  },
                  {
                    type: "op",
                    op: "=",
                    exprs: [
                      { type: "field", tableAlias: "subwithin", column: "descendant" },
                      { type: "field", tableAlias: "T1", column: "primary" }
                    ]
                  }
                ]
              }
            }
          ]
        }
      )
    })

    it("compiles within any", function () {
      return this.compile(
        {
          type: "op",
          op: "within any",
          exprs: [
            { type: "id", table: "thier" },
            { type: "literal", valueType: "id[]", idTable: "thier", value: [123, 456] }
          ]
        },
        {
          type: "op",
          op: "exists",
          exprs: [
            {
              type: "scalar",
              expr: null,
              from: { type: "table", table: "thier_ancestry", alias: "subwithin" },
              where: {
                type: "op",
                op: "and",
                exprs: [
                  {
                    type: "op",
                    op: "=",
                    modifier: "any",
                    exprs: [
                      { type: "field", tableAlias: "subwithin", column: "ancestor" },
                      { type: "literal", value: [123, 456] }
                    ]
                  },
                  {
                    type: "op",
                    op: "=",
                    exprs: [
                      { type: "field", tableAlias: "subwithin", column: "descendant" },
                      { type: "field", tableAlias: "T1", column: "primary" }
                    ]
                  }
                ]
              }
            }
          ]
        }
      )
    })

    it("compiles current date", function () {
      return this.compile(
        {
          type: "op",
          op: "current date",
          exprs: []
        },
        {
          type: "literal",
          value: moment().format("YYYY-MM-DD")
        }
      )
    })

    it("compiles current datetime", function () {
      const jsonql = this.ec.compileExpr({ expr: { type: "op", op: "current datetime", exprs: [] }, tableAlias: "T1" })
      assert.equal(jsonql.type, "literal")
    })

    // # DEPRECATED. Use ancestryTable
    // it "compiles within", ->
    //   @compile(
    //     {
    //       type: "op"
    //       op: "within"
    //       exprs: [{ type: "id", table: "thier" }, { type: "literal", valueType: "id", idTable: "thier", value: "123" }]
    //     }
    //     {
    //       type: "op"
    //       op: "in"
    //       exprs: [
    //         { type: "field", tableAlias: "T1", column: "primary" }
    //         {
    //           type: "scalar"
    //           expr: { type: "field", tableAlias: "subwithin", column: "primary" }
    //           from: { type: "table", table: "thier", alias: "subwithin" }
    //           where: {
    //             type: "op"
    //             op: "@>"
    //             exprs: [
    //               { type: "field", tableAlias: "subwithin", column: "path" }
    //               { type: "op", op: "::jsonb", exprs: [{ type: "op", op: "json_build_array", exprs: [{ type: "literal", value: "123" }] }] }
    //             ]
    //           }
    //         }
    //       ]
    //     }
    //   )

    // # DEPRECATED. Use ancestryTable
    // it "compiles within any", ->
    //   @compile(
    //     {
    //       type: "op"
    //       op: "within any"
    //       exprs: [{ type: "id", table: "thier" }, { type: "literal", valueType: "id[]", idTable: "thier", value: [123, 456] }]
    //     }
    //     {
    //       type: "op"
    //       op: "in"
    //       exprs: [
    //         { type: "field", tableAlias: "T1", column: "primary" }
    //         {
    //           type: "scalar"
    //           expr: { type: "field", tableAlias: "subwithin", column: "primary" }
    //           from: { type: "table", table: "thier", alias: "subwithin" }
    //           where: {
    //             type: "op"
    //             op: "?|"
    //             exprs: [
    //               { type: "field", tableAlias: "subwithin", column: "path_text" }
    //               { type: "literal", value: ["123", "456"] }
    //             ]
    //           }
    //         }
    //       ]
    //     }
    //   )

    it("compiles days difference (date)", function () {
      return this.compile(
        {
          type: "op",
          op: "days difference",
          exprs: [this.date1, this.date2]
        },
        {
          type: "op",
          op: "-",
          exprs: [
            { type: "op", op: "::date", exprs: [this.date1JsonQL] },
            { type: "op", op: "::date", exprs: [this.date2JsonQL] }
          ]
        }
      )
    })

    it("compiles days difference (datetime)", function () {
      return this.compile(
        {
          type: "op",
          op: "days difference",
          exprs: [this.datetime1, this.datetime2]
        },
        {
          type: "op",
          op: "/",
          exprs: [
            {
              type: "op",
              op: "-",
              exprs: [
                {
                  type: "op",
                  op: "date_part",
                  exprs: ["epoch", { type: "op", op: "::timestamp", exprs: [this.datetime1JsonQL] }]
                },
                {
                  type: "op",
                  op: "date_part",
                  exprs: ["epoch", { type: "op", op: "::timestamp", exprs: [this.datetime2JsonQL] }]
                }
              ]
            },
            86400
          ]
        }
      )
    })

    it("compiles months difference (date)", function () {
      return this.compile(
        {
          type: "op",
          op: "months difference",
          exprs: [this.date1, this.date2]
        },
        {
          type: "op",
          op: "/",
          exprs: [
            {
              type: "op",
              op: "-",
              exprs: [
                { type: "op", op: "::date", exprs: [this.date1JsonQL] },
                { type: "op", op: "::date", exprs: [this.date2JsonQL] }
              ]
            },
            30.5
          ]
        }
      )
    })

    it("compiles months difference (datetime)", function () {
      return this.compile(
        {
          type: "op",
          op: "months difference",
          exprs: [this.datetime1, this.datetime2]
        },
        {
          type: "op",
          op: "/",
          exprs: [
            {
              type: "op",
              op: "-",
              exprs: [
                {
                  type: "op",
                  op: "date_part",
                  exprs: ["epoch", { type: "op", op: "::timestamp", exprs: [this.datetime1JsonQL] }]
                },
                {
                  type: "op",
                  op: "date_part",
                  exprs: ["epoch", { type: "op", op: "::timestamp", exprs: [this.datetime2JsonQL] }]
                }
              ]
            },
            86400 * 30.5
          ]
        }
      )
    })

    it("compiles months difference (date)", function () {
      return this.compile(
        {
          type: "op",
          op: "years difference",
          exprs: [this.date1, this.date2]
        },
        {
          type: "op",
          op: "/",
          exprs: [
            {
              type: "op",
              op: "-",
              exprs: [
                { type: "op", op: "::date", exprs: [this.date1JsonQL] },
                { type: "op", op: "::date", exprs: [this.date2JsonQL] }
              ]
            },
            365
          ]
        }
      )
    })

    it("compiles months difference (datetime)", function () {
      return this.compile(
        {
          type: "op",
          op: "years difference",
          exprs: [this.datetime1, this.datetime2]
        },
        {
          type: "op",
          op: "/",
          exprs: [
            {
              type: "op",
              op: "-",
              exprs: [
                {
                  type: "op",
                  op: "date_part",
                  exprs: ["epoch", { type: "op", op: "::timestamp", exprs: [this.datetime1JsonQL] }]
                },
                {
                  type: "op",
                  op: "date_part",
                  exprs: ["epoch", { type: "op", op: "::timestamp", exprs: [this.datetime2JsonQL] }]
                }
              ]
            },
            86400 * 365
          ]
        }
      )
    })

    it("compiles enum to text", function () {
      return this.compile(
        {
          type: "op",
          op: "to text",
          exprs: [{ type: "field", table: "t1", column: "enum" }]
        },
        {
          type: "case",
          input: { type: "field", tableAlias: "T1", column: "enum" },
          cases: [
            { when: { type: "literal", value: "a" }, then: { type: "literal", value: "A" } },
            { when: { type: "literal", value: "b" }, then: { type: "literal", value: "B" } }
          ]
        }
      )
    })

    it("compiles number to text", function () {
      return this.compile(
        {
          type: "op",
          table: "t1",
          op: "to text",
          exprs: [{ type: "field", table: "t1", column: "number" }]
        },
        {
          type: "op",
          op: "::text",
          exprs: [{ type: "field", tableAlias: "T1", column: "number" }]
        }
      )
    })

    it("compiles text[] to text", function () {
      return this.compile(
        {
          type: "op",
          table: "t1",
          op: "to text",
          exprs: [{ type: "field", table: "t1", column: "text[]" }]
        },
        {
          type: "op",
          op: "array_to_string",
          exprs: [
            {
              type: "scalar",
              expr: {
                type: "op",
                op: "array_agg",
                exprs: [{ type: "field", tableAlias: "values" }]
              },
              from: {
                type: "subexpr",
                expr: {
                  type: "op",
                  op: "jsonb_array_elements_text",
                  exprs: [
                    { type: "op", op: "to_jsonb", exprs: [{ type: "field", tableAlias: "T1", column: "text[]" }] }
                  ]
                },
                alias: "values"
              }
            },
            // Requires explicit text type
            { type: "op", op: "::text", exprs: [", "] }
          ]
        }
      )
    })

    it("compiles to number", function () {
      return this.compile(
        {
          type: "op",
          table: "t1",
          op: "to number",
          exprs: [{ type: "field", table: "t1", column: "text" }]
        },
        {
          type: "case",
          cases: [
            {
              when: {
                type: "op",
                op: "~",
                exprs: [
                  { type: "field", tableAlias: "T1", column: "text" },
                  { type: "literal", value: "^([0-9]+[.]?[0-9]*|[.][0-9]+)$" }
                ]
              },
              then: {
                type: "op",
                op: "::numeric",
                exprs: [{ type: "op", op: "::text", exprs: [{ type: "field", tableAlias: "T1", column: "text" }] }]
              }
            }
          ],
          else: { type: "literal", value: null }
        }
      )
    })

    it("compiles weekofmonth", function () {
      return this.compile(
        {
          type: "op",
          table: "t1",
          op: "weekofmonth",
          exprs: [{ type: "field", table: "t1", column: "date" }]
        },
        {
          type: "op",
          op: "to_char",
          exprs: [{ type: "op", op: "::timestamp", exprs: [{ type: "field", tableAlias: "T1", column: "date" }] }, "W"]
        }
      )
    })

    it("compiles dayofmonth", function () {
      return this.compile(
        {
          type: "op",
          table: "t1",
          op: "dayofmonth",
          exprs: [{ type: "field", table: "t1", column: "date" }]
        },
        {
          type: "op",
          op: "to_char",
          exprs: [{ type: "op", op: "::timestamp", exprs: [{ type: "field", tableAlias: "T1", column: "date" }] }, "DD"]
        }
      )
    })

    it("compiles month", function () {
      return this.compile(
        {
          type: "op",
          table: "t1",
          op: "month",
          exprs: [{ type: "field", table: "t1", column: "date" }]
        },
        {
          type: "op",
          op: "substr",
          exprs: [{ type: "field", tableAlias: "T1", column: "date" }, 6, 2]
        }
      )
    })

    it("compiles yearmonth", function () {
      return this.compile(
        {
          type: "op",
          table: "t1",
          op: "yearmonth",
          exprs: [{ type: "field", table: "t1", column: "date" }]
        },
        {
          type: "op",
          op: "rpad",
          exprs: [
            { type: "op", op: "substr", exprs: [{ type: "field", tableAlias: "T1", column: "date" }, 1, 7] },
            10,
            "-01"
          ]
        }
      )
    })

    it("compiles yearquarter", function () {
      return this.compile(
        {
          type: "op",
          table: "t1",
          op: "yearquarter",
          exprs: [{ type: "field", table: "t1", column: "date" }]
        },
        {
          type: "op",
          op: "to_char",
          exprs: [{ type: "op", op: "::date", exprs: [{ type: "field", tableAlias: "T1", column: "date" }] }, "YYYY-Q"]
        }
      )
    })

    it("compiles yearweek", function () {
      return this.compile(
        {
          type: "op",
          table: "t1",
          op: "yearweek",
          exprs: [{ type: "field", table: "t1", column: "date" }]
        },
        {
          type: "op",
          op: "to_char",
          exprs: [{ type: "op", op: "::date", exprs: [{ type: "field", tableAlias: "T1", column: "date" }] }, "IYYY-IW"]
        }
      )
    })

    it("compiles weekofyear", function () {
      return this.compile(
        {
          type: "op",
          table: "t1",
          op: "weekofyear",
          exprs: [{ type: "field", table: "t1", column: "date" }]
        },
        {
          type: "op",
          op: "to_char",
          exprs: [{ type: "op", op: "::date", exprs: [{ type: "field", tableAlias: "T1", column: "date" }] }, "IW"]
        }
      )
    })

    it("compiles year", function () {
      return this.compile(
        {
          type: "op",
          table: "t1",
          op: "year",
          exprs: [{ type: "field", table: "t1", column: "date" }]
        },
        {
          type: "op",
          op: "rpad",
          exprs: [
            { type: "op", op: "substr", exprs: [{ type: "field", tableAlias: "T1", column: "date" }, 1, 4] },
            10,
            "-01-01"
          ]
        }
      )
    })

    it("compiles to date", function () {
      return this.compile(
        {
          type: "op",
          table: "t1",
          op: "to date",
          exprs: [{ type: "field", table: "t1", column: "datetime" }]
        },
        {
          type: "op",
          op: "substr",
          exprs: [{ type: "field", tableAlias: "T1", column: "datetime" }, 1, 10]
        }
      )
    })

    it("compiles weekofmonth", function () {
      return this.compile(
        {
          type: "op",
          table: "t1",
          op: "weekofmonth",
          exprs: [{ type: "field", table: "t1", column: "date" }]
        },
        {
          type: "op",
          op: "to_char",
          exprs: [{ type: "op", op: "::timestamp", exprs: [{ type: "field", tableAlias: "T1", column: "date" }] }, "W"]
        }
      )
    })

    describe("relative dates", function () {
      it("thisyear", function () {
        return this.compile(
          {
            type: "op",
            op: "thisyear",
            exprs: [this.date1]
          },
          {
            type: "op",
            op: "and",
            exprs: [
              { type: "op", op: ">=", exprs: [this.date1JsonQL, moment().startOf("year").format("YYYY-MM-DD")] },
              {
                type: "op",
                op: "<",
                exprs: [this.date1JsonQL, moment().startOf("year").add(1, "years").format("YYYY-MM-DD")]
              }
            ]
          }
        )
      })

      it("lastyear", function () {
        return this.compile(
          {
            type: "op",
            op: "lastyear",
            exprs: [this.date1]
          },
          {
            type: "op",
            op: "and",
            exprs: [
              {
                type: "op",
                op: ">=",
                exprs: [this.date1JsonQL, moment().startOf("year").subtract(1, "years").format("YYYY-MM-DD")]
              },
              { type: "op", op: "<", exprs: [this.date1JsonQL, moment().startOf("year").format("YYYY-MM-DD")] }
            ]
          }
        )
      })

      it("thismonth", function () {
        return this.compile(
          {
            type: "op",
            op: "thismonth",
            exprs: [this.date1]
          },
          {
            type: "op",
            op: "and",
            exprs: [
              { type: "op", op: ">=", exprs: [this.date1JsonQL, moment().startOf("month").format("YYYY-MM-DD")] },
              {
                type: "op",
                op: "<",
                exprs: [this.date1JsonQL, moment().startOf("month").add(1, "months").format("YYYY-MM-DD")]
              }
            ]
          }
        )
      })

      it("lastmonth", function () {
        return this.compile(
          {
            type: "op",
            op: "lastmonth",
            exprs: [this.date1]
          },
          {
            type: "op",
            op: "and",
            exprs: [
              {
                type: "op",
                op: ">=",
                exprs: [this.date1JsonQL, moment().startOf("month").subtract(1, "months").format("YYYY-MM-DD")]
              },
              { type: "op", op: "<", exprs: [this.date1JsonQL, moment().startOf("month").format("YYYY-MM-DD")] }
            ]
          }
        )
      })

      it("today", function () {
        return this.compile(
          {
            type: "op",
            op: "today",
            exprs: [this.date1]
          },
          {
            type: "op",
            op: "and",
            exprs: [
              { type: "op", op: ">=", exprs: [this.date1JsonQL, moment().format("YYYY-MM-DD")] },
              { type: "op", op: "<", exprs: [this.date1JsonQL, moment().add(1, "days").format("YYYY-MM-DD")] }
            ]
          }
        )
      })

      it("yesterday", function () {
        return this.compile(
          {
            type: "op",
            op: "yesterday",
            exprs: [this.date1]
          },
          {
            type: "op",
            op: "and",
            exprs: [
              { type: "op", op: ">=", exprs: [this.date1JsonQL, moment().subtract(1, "days").format("YYYY-MM-DD")] },
              { type: "op", op: "<", exprs: [this.date1JsonQL, moment().format("YYYY-MM-DD")] }
            ]
          }
        )
      })

      it("last7days", function () {
        return this.compile(
          {
            type: "op",
            op: "last7days",
            exprs: [this.date1]
          },
          {
            type: "op",
            op: "and",
            exprs: [
              { type: "op", op: ">=", exprs: [this.date1JsonQL, moment().subtract(7, "days").format("YYYY-MM-DD")] },
              { type: "op", op: "<", exprs: [this.date1JsonQL, moment().add(1, "days").format("YYYY-MM-DD")] }
            ]
          }
        )
      })

      it("last30days", function () {
        return this.compile(
          {
            type: "op",
            op: "last30days",
            exprs: [this.date1]
          },
          {
            type: "op",
            op: "and",
            exprs: [
              { type: "op", op: ">=", exprs: [this.date1JsonQL, moment().subtract(30, "days").format("YYYY-MM-DD")] },
              { type: "op", op: "<", exprs: [this.date1JsonQL, moment().add(1, "days").format("YYYY-MM-DD")] }
            ]
          }
        )
      })

      it("last365days", function () {
        return this.compile(
          {
            type: "op",
            op: "last365days",
            exprs: [this.date1]
          },
          {
            type: "op",
            op: "and",
            exprs: [
              { type: "op", op: ">=", exprs: [this.date1JsonQL, moment().subtract(365, "days").format("YYYY-MM-DD")] },
              { type: "op", op: "<", exprs: [this.date1JsonQL, moment().add(1, "days").format("YYYY-MM-DD")] }
            ]
          }
        )
      })

      return it("compiles days since (date)", function () {
        return this.compile(
          {
            type: "op",
            op: "days since",
            exprs: [this.date1]
          },
          {
            type: "op",
            op: "-",
            exprs: [
              { type: "op", op: "::date", exprs: [moment().format("YYYY-MM-DD")] },
              { type: "op", op: "::date", exprs: [this.date1JsonQL] }
            ]
          }
        )
      })
    })

    describe("relative datetimes", function () {
      before(function () {
        return (this.clock = sinon.useFakeTimers(new Date().getTime()))
      })

      after(function () {
        return this.clock.restore()
      })

      it("thisyear", function () {
        return this.compile(
          {
            type: "op",
            op: "thisyear",
            exprs: [this.datetime1]
          },
          {
            type: "op",
            op: "and",
            exprs: [
              { type: "op", op: ">=", exprs: [this.datetime1JsonQL, moment().startOf("year").toISOString()] },
              {
                type: "op",
                op: "<",
                exprs: [this.datetime1JsonQL, moment().startOf("year").add(1, "years").toISOString()]
              }
            ]
          }
        )
      })

      it("lastyear", function () {
        return this.compile(
          {
            type: "op",
            op: "lastyear",
            exprs: [this.datetime1]
          },
          {
            type: "op",
            op: "and",
            exprs: [
              {
                type: "op",
                op: ">=",
                exprs: [this.datetime1JsonQL, moment().startOf("year").subtract(1, "years").toISOString()]
              },
              { type: "op", op: "<", exprs: [this.datetime1JsonQL, moment().startOf("year").toISOString()] }
            ]
          }
        )
      })

      it("thismonth", function () {
        return this.compile(
          {
            type: "op",
            op: "thismonth",
            exprs: [this.datetime1]
          },
          {
            type: "op",
            op: "and",
            exprs: [
              { type: "op", op: ">=", exprs: [this.datetime1JsonQL, moment().startOf("month").toISOString()] },
              {
                type: "op",
                op: "<",
                exprs: [this.datetime1JsonQL, moment().startOf("month").add(1, "months").toISOString()]
              }
            ]
          }
        )
      })

      it("lastmonth", function () {
        return this.compile(
          {
            type: "op",
            op: "lastmonth",
            exprs: [this.datetime1]
          },
          {
            type: "op",
            op: "and",
            exprs: [
              {
                type: "op",
                op: ">=",
                exprs: [this.datetime1JsonQL, moment().startOf("month").subtract(1, "months").toISOString()]
              },
              { type: "op", op: "<", exprs: [this.datetime1JsonQL, moment().startOf("month").toISOString()] }
            ]
          }
        )
      })

      it("today", function () {
        return this.compile(
          {
            type: "op",
            op: "today",
            exprs: [this.datetime1]
          },
          {
            type: "op",
            op: "and",
            exprs: [
              { type: "op", op: ">=", exprs: [this.datetime1JsonQL, moment().startOf("day").toISOString()] },
              {
                type: "op",
                op: "<",
                exprs: [this.datetime1JsonQL, moment().startOf("day").add(1, "days").toISOString()]
              }
            ]
          }
        )
      })

      it("yesterday", function () {
        return this.compile(
          {
            type: "op",
            op: "yesterday",
            exprs: [this.datetime1]
          },
          {
            type: "op",
            op: "and",
            exprs: [
              {
                type: "op",
                op: ">=",
                exprs: [this.datetime1JsonQL, moment().startOf("day").subtract(1, "days").toISOString()]
              },
              { type: "op", op: "<", exprs: [this.datetime1JsonQL, moment().startOf("day").toISOString()] }
            ]
          }
        )
      })

      it("last24hours", function () {
        return this.compile(
          {
            type: "op",
            op: "last24hours",
            exprs: [this.datetime1]
          },
          {
            type: "op",
            op: "and",
            exprs: [
              { type: "op", op: ">=", exprs: [this.datetime1JsonQL, nowMinus24HoursExpr] },
              { type: "op", op: "<=", exprs: [this.datetime1JsonQL, nowExpr] }
            ]
          }
        )
      })

      it("last7days", function () {
        return this.compile(
          {
            type: "op",
            op: "last7days",
            exprs: [this.datetime1]
          },
          {
            type: "op",
            op: "and",
            exprs: [
              {
                type: "op",
                op: ">=",
                exprs: [this.datetime1JsonQL, moment().startOf("day").subtract(7, "days").toISOString()]
              },
              {
                type: "op",
                op: "<",
                exprs: [this.datetime1JsonQL, moment().startOf("day").add(1, "days").toISOString()]
              }
            ]
          }
        )
      })

      it("last30days", function () {
        return this.compile(
          {
            type: "op",
            op: "last30days",
            exprs: [this.datetime1]
          },
          {
            type: "op",
            op: "and",
            exprs: [
              {
                type: "op",
                op: ">=",
                exprs: [this.datetime1JsonQL, moment().startOf("day").subtract(30, "days").toISOString()]
              },
              {
                type: "op",
                op: "<",
                exprs: [this.datetime1JsonQL, moment().startOf("day").add(1, "days").toISOString()]
              }
            ]
          }
        )
      })

      it("last365days", function () {
        this.compile(
          {
            type: "op",
            op: "last365days",
            exprs: [this.datetime1]
          },
          {
            type: "op",
            op: "and",
            exprs: [
              {
                type: "op",
                op: ">=",
                exprs: [this.datetime1JsonQL, moment().startOf("day").subtract(365, "days").toISOString()]
              },
              {
                type: "op",
                op: "<",
                exprs: [this.datetime1JsonQL, moment().startOf("day").add(1, "days").toISOString()]
              }
            ]
          }
        )

        it("last3months", function () {})
        return this.compile(
          {
            type: "op",
            op: "last3months",
            exprs: [this.datetime1]
          },
          {
            type: "op",
            op: "and",
            exprs: [
              {
                type: "op",
                op: ">=",
                exprs: [this.datetime1JsonQL, moment().subtract(2, "months").startOf("month").toISOString()]
              },
              {
                type: "op",
                op: "<",
                exprs: [this.datetime1JsonQL, moment().startOf("day").add(1, "days").toISOString()]
              }
            ]
          }
        )
      })

      it("last6months", function () {
        return this.compile(
          {
            type: "op",
            op: "last6months",
            exprs: [this.datetime1]
          },
          {
            type: "op",
            op: "and",
            exprs: [
              {
                type: "op",
                op: ">=",
                exprs: [this.datetime1JsonQL, moment().subtract(5, "months").startOf("month").toISOString()]
              },
              {
                type: "op",
                op: "<",
                exprs: [this.datetime1JsonQL, moment().startOf("day").add(1, "days").toISOString()]
              }
            ]
          }
        )
      })

      it("last12months", function () {
        return this.compile(
          {
            type: "op",
            op: "last12months",
            exprs: [this.datetime1]
          },
          {
            type: "op",
            op: "and",
            exprs: [
              {
                type: "op",
                op: ">=",
                exprs: [this.datetime1JsonQL, moment().subtract(11, "months").startOf("month").toISOString()]
              },
              {
                type: "op",
                op: "<",
                exprs: [this.datetime1JsonQL, moment().startOf("day").add(1, "days").toISOString()]
              }
            ]
          }
        )
      })

      it("compiles days since (datetime)", function () {
        return this.compile(
          {
            type: "op",
            op: "days since",
            exprs: [this.datetime1]
          },
          {
            type: "op",
            op: "/",
            exprs: [
              {
                type: "op",
                op: "-",
                exprs: [
                  {
                    type: "op",
                    op: "date_part",
                    exprs: ["epoch", { type: "op", op: "::timestamp", exprs: [nowExpr] }]
                  },
                  {
                    type: "op",
                    op: "date_part",
                    exprs: ["epoch", { type: "op", op: "::timestamp", exprs: [this.datetime1JsonQL] }]
                  }
                ]
              },
              86400
            ]
          }
        )
      })

      it("future", function () {
        return this.compile(
          {
            type: "op",
            op: "future",
            exprs: [this.datetime1]
          },
          {
            type: "op",
            op: ">",
            exprs: [this.datetime1JsonQL, nowExpr]
          }
        )
      })

      return it("notfuture", function () {
        return this.compile(
          {
            type: "op",
            op: "notfuture",
            exprs: [this.datetime1]
          },
          {
            type: "op",
            op: "<=",
            exprs: [this.datetime1JsonQL, nowExpr]
          }
        )
      })
    })

    it("distance", function () {
      return this.compile(
        {
          type: "op",
          op: "distance",
          exprs: [this.geometry, this.geometry]
        },
        // ST_DistanceSphere(ST_Transform(x, 4326::integer), ST_Transform(y, 4326::integer))
        {
          type: "op",
          op: "ST_DistanceSphere",
          exprs: [
            {
              type: "op",
              op: "ST_Transform",
              exprs: [this.geometryJsonQL, { type: "op", op: "::integer", exprs: [4326] }]
            },
            {
              type: "op",
              op: "ST_Transform",
              exprs: [this.geometryJsonQL, { type: "op", op: "::integer", exprs: [4326] }]
            }
          ]
        }
      )
    })

    return it("is latest", function () {
      return this.compile(
        {
          type: "op",
          op: "is latest",
          table: "t2", // t2 is ordered by number. Note that alias is still T1 in the testing, but just ignore that
          exprs: [this.enum1, this.bool1]
        },
        // _id in (select id from (select id, row_number() over (partition by EXPR1 order by ORDERING desc) as rn from the_table as innerrn where filter) as outerrn where outerrn.rn = 1)
        {
          type: "op",
          op: "in",
          exprs: [
            { type: "field", tableAlias: "T1", column: "primary" },
            {
              type: "scalar",
              expr: { type: "field", tableAlias: "outerrn", column: "id" },
              from: {
                type: "subquery",
                query: {
                  type: "query",
                  selects: [
                    { type: "select", expr: { type: "field", tableAlias: "innerrn", column: "primary" }, alias: "id" },
                    {
                      type: "select",
                      expr: {
                        type: "op",
                        op: "row_number",
                        exprs: [],
                        over: {
                          partitionBy: [this.enum1JsonQL],
                          orderBy: [
                            { expr: { type: "field", tableAlias: "innerrn", column: "number" }, direction: "desc" }
                          ]
                        }
                      },
                      alias: "rn"
                    }
                  ],
                  from: { type: "table", table: "t2", alias: "innerrn" },
                  where: this.bool1JsonQL
                },
                alias: "outerrn"
              },
              where: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "outerrn", column: "rn" }, 1] }
            }
          ]
        }
      )
    })
  })

  describe("custom jsonql", function () {
    describe("table", () =>
      it("substitutes table", function () {
        const schema = fixtures.simpleSchema()
        const tableJsonql: JsonQLQuery = {
          type: "query",
          selects: [
            { 
              type: "select",
              expr: {
                type: "field",
                tableAlias: "abc",
                column: "number"
              },
              alias: "number"
            }
          ],
          from: { type: "table", table: "t2", alias: "abc" }
        }

        // Customize t2
        schema.getTable("t2")!.jsonql = tableJsonql

        const ec = new ExprCompiler(schema)

        const jql = ec.compileExpr({
          expr: { type: "scalar", table: "t1", joins: ["1-2"], expr: { type: "field", table: "t2", column: "number" } },
          tableAlias: "T1"
        })

        const from = {
          type: "subquery",
          query: {
            type: "query",
            selects: [
              { 
                type: "select",
                expr: {
                  type: "field",
                  tableAlias: "abc",
                  column: "number"
                },
                alias: "number"
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

        assert(_.isEqual((jql as any).from, from), JSON.stringify(jql, null, 2))
      }))

    // describe "join"
    return describe("column", () =>
      it("substitutes {alias}", function () {
        let schema = fixtures.simpleSchema()
        const columnJsonql: JsonQLExpr = {
          type: "op",
          op: "sum",
          exprs: [
            {
              type: "field",
              tableAlias: "{alias}", // Should be replaced!
              column: "number"
            }
          ]
        }

        schema = schema.addTable({
          id: "t1",
          name: { _base: "en", en: "T1" },
          contents: [{ id: "custom", name: { _base: "en", en: "Custom" }, type: "text", jsonql: columnJsonql }]
        })

        const ec = new ExprCompiler(schema)

        const jql = ec.compileExpr({ expr: { type: "field", table: "t1", column: "custom" }, tableAlias: "T1" })

        assert(
          _.isEqual(jql, {
            type: "op",
            op: "sum",
            exprs: [
              {
                type: "field",
                tableAlias: "T1", // Replaced with table alias
                column: "number"
              }
            ]
          })
        )
      }))
  })

  describe("comparisons (deprecated)", function () {
    it("compiles =", function () {
      return this.compile(
        {
          type: "comparison",
          op: "=",
          lhs: { type: "field", table: "t1", column: "number" },
          rhs: { type: "literal", valueType: "number", value: 3 }
        },
        {
          type: "op",
          op: "=",
          exprs: [
            { type: "field", tableAlias: "T1", column: "number" },
            { type: "literal", value: 3 }
          ]
        }
      )
    })

    it("compiles = any", function () {
      return this.compile(
        {
          type: "comparison",
          op: "= any",
          lhs: { type: "field", table: "t1", column: "enum" },
          rhs: { type: "literal", valueType: "enum[]", value: ["a", "b"] }
        },
        {
          type: "op",
          op: "=",
          modifier: "any",
          exprs: [
            { type: "field", tableAlias: "T1", column: "enum" },
            { type: "literal", value: ["a", "b"] }
          ]
        }
      )
    })

    it("compiles no rhs as null", function () {
      return this.compile(
        {
          type: "comparison",
          op: "=",
          lhs: { type: "field", table: "t1", column: "number" }
        },
        null
      )
    })

    return it("compiles daterange", function () {
      return this.compile(
        {
          type: "comparison",
          op: "between",
          lhs: { type: "field", table: "t1", column: "date" },
          rhs: { type: "literal", valueType: "daterange", value: ["2014-01-01", "2014-12-31"] }
        },
        {
          type: "op",
          op: "between",
          exprs: [
            { type: "field", tableAlias: "T1", column: "date" },
            { type: "literal", value: "2014-01-01" },
            { type: "literal", value: "2014-12-31" }
          ]
        }
      )
    })
  })

  describe("logicals (deprecated)", function () {
    it("simplifies logical", function () {
      const expr1 = { type: "comparison", op: "= false", lhs: { type: "field", table: "t1", column: "boolean" } }

      return this.compile(
        { type: "logical", op: "and", exprs: [expr1] },
        {
          type: "op",
          op: "=",
          exprs: [
            { type: "field", tableAlias: "T1", column: "boolean" },
            { type: "literal", value: false }
          ]
        }
      )
    })

    it("compiles logical", function () {
      const expr1 = {
        type: "comparison",
        op: "=",
        lhs: { type: "field", table: "t1", column: "number" },
        rhs: { type: "literal", valueType: "number", value: 3 }
      }

      const expr2 = { type: "comparison", op: "= false", lhs: { type: "field", table: "t1", column: "boolean" } }

      return this.compile(
        { type: "logical", op: "and", exprs: [expr1, expr2] },
        {
          type: "op",
          op: "and",
          exprs: [
            {
              type: "op",
              op: "=",
              exprs: [
                { type: "field", tableAlias: "T1", column: "number" },
                { type: "literal", value: 3 }
              ]
            },
            {
              type: "op",
              op: "=",
              exprs: [
                { type: "field", tableAlias: "T1", column: "boolean" },
                { type: "literal", value: false }
              ]
            }
          ]
        }
      )
    })

    return it("excluded blank condition", function () {
      const expr1 = { type: "comparison", op: "= true", lhs: { type: "field", table: "t1", column: "number" } }

      const expr2 = { type: "comparison", op: "=", lhs: { type: "field", table: "t1", column: "number" } } // No RHS

      return this.compile(
        { type: "logical", op: "and", exprs: [expr1, expr2] },
        {
          type: "op",
          op: "=",
          exprs: [
            { type: "field", tableAlias: "T1", column: "number" },
            { type: "literal", value: true }
          ]
        }
      )
    })
  })

  describe("variable", function () {
    it("compiles literal", function () {
      const expr = { type: "variable", variableId: "varnumber" }
      return this.compile(expr, { type: "literal", value: 123 })
    })

    return it("compiles expression", function () {
      const expr = { type: "variable", variableId: "varnumberexpr", table: "t1" }
      return this.compile(expr, {
        type: "op",
        op: "+",
        exprs: [
          {
            type: "op",
            op: "coalesce",
            exprs: [{ type: "op", op: "::decimal", exprs: [{ type: "field", tableAlias: "T1", column: "number" }] }, 0]
          },
          {
            type: "op",
            op: "coalesce",
            exprs: [{ type: "op", op: "::decimal", exprs: [{ type: "literal", value: 2 }] }, 0]
          }
        ]
      })
    })
  })

  // describe "spatial join", ->
  //   it "compiles", ->
  //     expr = {
  //       type: "spatial join"
  //       valueExpr: { type: "op", op: "count", table: "t2", exprs: [] }
  //       table: "t1"
  //       toTable: "t2"
  //       fromGeometryExpr: { type: "field", table: "t1", column: "geometry" }
  //       toGeometryExpr: { type: "field", table: "t2", column: "geometry" }
  //       radiusExpr: { type: "literal", valueType: "number", value: 10 }
  //       filterExpr: { type: "field", table: "t2", column: "boolean" }
  //     }

  //     radiusExpr = {
  //       type: "op"
  //       op: "/"
  //       exprs: [
  //         { type: "literal", value: 10 }
  //         { type: "op", op: "cos", exprs: [
  //           { type: "op", op: "/", exprs: [
  //             { type: "op", op: "ST_YMin", exprs: [
  //               { type: "op", op: "ST_Transform", exprs: [
  //                 { type: "field", tableAlias: "T1", column: "geometry" }
  //                 4326
  //               ]}
  //             ]}
  //             57.3
  //           ]}
  //         ]}
  //       ]
  //     }

  //     @compile(expr, {
  //       type: "scalar",
  //       expr: { type: "op", op: "count", exprs: [] },
  //       from: { type: "table", table: "t2", alias: "spatial" }
  //       where: { type: "op", op: "and", exprs: [
  //         {
  //           type: "op"
  //           op: "&&"
  //           exprs: [
  //             { type: "field", tableAlias: "spatial", column: "geometry" }
  //             { type: "op", op: "ST_Expand", exprs: [
  //               { type: "field", tableAlias: "T1", column: "geometry" }
  //               radiusExpr
  //             ]}
  //           ]
  //         },
  //         {
  //           type: "op"
  //           op: "<="
  //           exprs: [
  //             { type: "op", op: "ST_Distance", exprs: [
  //               { type: "field", tableAlias: "spatial", column: "geometry" }
  //               { type: "field", tableAlias: "T1", column: "geometry" }
  //             ]}
  //             radiusExpr
  //           ]
  //         },
  //         { type: "field", tableAlias: "spatial", column: "boolean" }
  //       ]}
  //     })

  return describe("extension", () =>
    it("compiles", function () {
      return this.compile({ type: "extension", extension: "test" }, { type: "literal", value: 4 })
    }))
})
