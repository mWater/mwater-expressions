assert = require('chai').assert
_ = require 'lodash'
Schema = require('../src/Schema').default
ExprValidator = require '../src/ExprValidator'
fixtures = require './fixtures'

canonical = require 'canonical-json'

describe "ExprValidator", ->
  beforeEach ->
    @schema = fixtures.simpleSchema()
    @exprValidator = new ExprValidator(@schema)
    @isValid = (expr, options) =>
      assert.isNull @exprValidator.validateExpr(expr, options), "Expected to be valid"

    @notValid = (expr, options) =>
      assert @exprValidator.validateExpr(expr, options), "Expected to be invalid"

  it "invalid if wrong table", ->
    @notValid({ type: "field", table: "t1", column: "text" }, table: "t2")

  it "invalid if wrong type", ->
    @notValid({ type: "field", table: "t1", column: "enum" }, types: ["text"])

  it "invalid if wrong idTable", ->
    field = { type: "id", table: "t1" }
    @isValid(field, types: ["id"], idTable: "t1")
    @notValid(field, types: ["id"], idTable: "t2")

  it "invalid if wrong enums", ->
    field = { type: "field", table: "t1", column: "enum" }
    @isValid(field, enumValueIds: ["a", "b", "c"])
    @notValid(field, enumValueIds: ["a"])

  it "invalid if wrong enums expression", ->
    field = { type: "field", table: "t1", column: "expr_enum" }
    @isValid(field, enumValueIds: ["a", "b", "c"])
    @notValid(field, enumValueIds: ["a"])

  it "valid if ok", ->
    @isValid({ type: "field", table: "t1", column: "text"})

  it "invalid if missing field", ->
    @notValid({ type: "field", table: "t1", column: "xyz"})

  it "invalid if field expr invalid", ->
    table = @schema.getTable("t1")
    table.contents.push(
      { id: "expr_invalid", name: { en: "Expr Invalid"}, type: "expr", expr: { type: "field", table: "t1", column: "xyz" }}
    )
    schema = @schema.addTable(table)

    exprValidator = new ExprValidator(schema)
    assert exprValidator.validateExpr({ type: "field", table: "t1", column: "expr_invalid" })

  it "handles recursive field expr", ->
    table = @schema.getTable("t1")
    table.contents.push(
      { id: "expr_recursive", name: { en: "Expr Recursive"}, type: "expr", expr: { type: "field", table: "t1", column: "expr_recursive" }}
    )
    schema = @schema.addTable(table)

    exprValidator = new ExprValidator(schema)
    assert exprValidator.validateExpr({ type: "field", table: "t1", column: "expr_recursive" })

  describe "scalar", ->
    it "valid", ->
      expr = { 
        type: "scalar"
        table: "t2"
        joins: ["2-1"]
        expr: { type: "field", table: "t1", column: "number" }
      }
      @isValid(expr)

    it "bad join", ->
      expr = { 
        type: "scalar"
        table: "t2"
        joins: ["xyz"]
        expr: { type: "field", table: "t1", column: "number" }
      }
      @notValid(expr)

    it "bad expr", ->
      expr = { 
        type: "scalar"
        table: "t2"
        joins: ["2-1"]
        expr: { type: "field", table: "t1", column: "xyz" }
      }
      @notValid(expr)

  describe "op", ->
    it "invalid if mixed aggregate and individual"

    it "valid", ->
      expr = { 
        type: "op"
        table: "t1"
        op: "+"
        exprs: [{ type: "field", table: "t1", column: "number" }]
      }
      @isValid(expr)

    it "invalid if expr invalid", ->
      expr = { 
        type: "op"
        table: "t1"
        op: "+"
        exprs: [{ type: "field", table: "t1", column: "xyz" }]
      }
      @notValid(expr)

    it "invalid if wrong expr types", ->
      expr = { 
        type: "op"
        table: "t1"
        op: "+"
        exprs: [{ type: "field", table: "t1", column: "text" }]
      }
      @notValid(expr)

  describe "case", ->
    it "validates else", ->
      expr = { 
        type: "case"
        table: "t1"
        cases: [{ when: { type: "literal", valueType: "boolean", value: true }, then: { type: "literal", valueType: "number", value: 123 }}]
        else: { type: "literal", valueType: "text", value: "abc" }
      }
      @isValid(expr)

      expr = _.cloneDeep(expr)
      expr.else = { type: "field", table: "t1", column: "xyz" }
      @notValid(expr)

    it "validates cases whens boolean", ->
      expr = { 
        type: "case"
        table: "t1"
        cases: [{ when: { type: "literal", valueType: "boolean", value: true }, then: { type: "literal", valueType: "number", value: 123 }}]
        else: { type: "literal", valueType: "text", value: "abc" }
      }
      @isValid(expr)

      expr = _.cloneDeep(expr)
      expr.cases[0].when = { type: "field", table: "t1", column: "text" }
      @notValid(expr)

    it "validates cases thens", ->
      expr = { 
        type: "case"
        table: "t1"
        cases: [{ when: { type: "literal", valueType: "boolean", value: true }, then: { type: "literal", valueType: "number", value: 123 }}]
        else: { type: "literal", valueType: "text", value: "abc" }
      }
      @isValid(expr)

      expr = _.cloneDeep(expr)
      expr.cases[0].then = { type: "field", table: "t1", column: "xyz" }
      @notValid(expr)

  describe "score", ->
    it "validates input", ->
      expr = { type: "score", table: "t1", input: { type: "field", table: "t1", column: "enum" }, scores: {} }
      @isValid(expr)

      expr = { type: "score", table: "t1", input: { type: "field", table: "t1", column: "text" }, scores: {} }
      @notValid(expr)

      expr = { type: "score", table: "t1", input: { type: "field", table: "t1", column: "xyz" }, scores: {} }
      @notValid(expr)

    it "validates score keys", ->
      expr = { type: "score", table: "t1", input: { type: "field", table: "t1", column: "enum" }, scores: {
          a: { type: "field", table: "t1", column: "number" }
        } 
      }
      @isValid(expr)

      expr = { type: "score", table: "t1", input: { type: "field", table: "t1", column: "enum" }, scores: {
          xyz: { type: "field", table: "t1", column: "number" }
        } 
      }
      @notValid(expr)

    it "validates score values", ->
      expr = { type: "score", table: "t1", input: { type: "field", table: "t1", column: "enum" }, scores: {
          a: { type: "field", table: "t1", column: "number" }
        } 
      }
      @isValid(expr)

      expr = { type: "score", table: "t1", input: { type: "field", table: "t1", column: "enum" }, scores: {
          a: { type: "field", table: "t1", column: "text" }
        } 
      }
      @notValid(expr)

  describe "variable", -> 
    it "fails if non-existent", ->
      @notValid({ type: "variable", variableId: "varxyz" })

    it "success if exists", ->
      @isValid({ type: "variable", variableId: "varnumber" })

    it "checks idTable", ->
      @isValid({ type: "variable", variableId: "varid" })
      @isValid({ type: "variable", variableId: "varid" }, { table: "t2" })
      @isValid({ type: "variable", variableId: "varid" }, { table: "t2", idTable: "t2" })
      @notValid({ type: "variable", variableId: "varid" }, { table: "t2", idTable: "t1" })
