assert = require('chai').assert
fixtures = require './fixtures'
_ = require 'lodash'

ExprUtils = require '../src/ExprUtils'
Schema = require '../src/Schema'

canonical = require 'canonical-json'

variables = [
  { id: "varenum", name: { _base: "en", en: "Varenum" }, type: "enum", enumValues: [{ id: "a", name: { en: "A" }}, { id: "b", name: { en: "B" }}] }
  { id: "varnumber", name: { _base: "en", en: "Varnumber" }, type: "number" }
  { id: "varidexpr", name: { _base: "en", en: "Varidexpr" }, type: "id", table: "t1", idTable: "t2" }
]

compare = (actual, expected) ->
  assert.equal canonical(actual), canonical(expected), "\ngot: " + canonical(actual) + "\nexp: " + canonical(expected) + "\n"

describe "ExprUtils", ->
  beforeEach ->
    @exprUtils = new ExprUtils(fixtures.simpleSchema(), variables)

  it "determines if multiple joins", ->
    assert.isTrue @exprUtils.isMultipleJoins("t1", ["1-2"])
    assert.isFalse @exprUtils.isMultipleJoins("t2", ["2-1"])

  it "follows joins", ->
    assert.equal @exprUtils.followJoins("t1", []), "t1"
    assert.equal @exprUtils.followJoins("t1", ["1-2"]), "t2"

  it "localizes strings", ->
    assert.equal @exprUtils.localizeString("apple", "en"), "apple"
    assert.equal @exprUtils.localizeString({ en: "apple", fr: "pomme" }, "fr"), "pomme"
    assert.equal @exprUtils.localizeString({ en: "apple", fr: "pomme" }, null), "apple"
    assert.equal @exprUtils.localizeString({ _base: "fr", fr: "pomme" }, null), "pomme"
    assert.equal @exprUtils.localizeString({ _base: "fr", en: "apple", fr: "pomme" }, null), "pomme", "_base wins if no locale"
    assert.equal @exprUtils.localizeString({ _base: "fr", en: "apple", fr: "pomme" }, "en"), "apple", "_base wins if no locale"

  it "getExprTable", ->
    assert.equal @exprUtils.getExprTable({ table: "xyz", type: "id" }), "xyz"

  describe "getExprIdTable", ->
    it "gets for literal", ->
      assert.equal @exprUtils.getExprIdTable({ type: "literal", valueType: "id", idTable: "xyz", value: "123" }), "xyz"

    it "gets for id field", ->
      assert.equal @exprUtils.getExprIdTable({ table: "xyz", type: "id" }), "xyz"

    it "gets for id expr field", ->
      assert.equal @exprUtils.getExprIdTable({ type: "field", table: "t1", column: "expr_id" }), "t1"

    it "gets for scalar", ->
      assert.equal @exprUtils.getExprIdTable({ type: "scalar", table: "t2", joins: ["2-1"], expr: { type: "id", table: "t1" }}), "t1"

    it "gets for variable", ->
      assert.equal @exprUtils.getExprIdTable({ type: "variable", table: "t1", variableId: "varidexpr" }), "t2"

  describe "getExprAggrStatus", ->
    it "gets for literal", ->
      assert.equal @exprUtils.getExprAggrStatus({ type: "literal", valueType: "id", idTable: "xyz", value: "123" }), "literal"

    it "gets for id", ->
      assert.equal @exprUtils.getExprAggrStatus({ table: "xyz", type: "id" }), "individual"

    it "gets for field", ->
      assert.equal @exprUtils.getExprAggrStatus({ type: "field", table: "t1", column: "number" }), "individual"

    it "gets for expr field", ->
      assert.equal @exprUtils.getExprAggrStatus({ type: "field", table: "t1", column: "expr_sum" }), "aggregate"

    it "gets for aggregate", ->
      assert.equal @exprUtils.getExprAggrStatus({ type: "op", op: "sum", exprs: [{ type: "field", table: "xyz", column: "abc" }]}), "aggregate"

    it "gets for aggregate + literal", ->
      assert.equal @exprUtils.getExprAggrStatus({ type: "op", op: "+", exprs: [
        { type: "op", op: "sum", exprs: [{ type: "field", table: "xyz", column: "abc" }]}
        { type: "literal", valueType: "number", value: 123 }
        ]}), "aggregate"

    it "gets for scalar", ->
      assert.equal @exprUtils.getExprAggrStatus({ type: "scalar", table: "t2", joins: ["2-1"], expr: { type: "id", table: "t1" }}), "individual"

    it "gets for literal variable", ->
      assert.equal @exprUtils.getExprAggrStatus({ type: "variable", variableId: "varnumber" }), "literal"

    it "gets for individual variable", ->
      assert.equal @exprUtils.getExprAggrStatus({ type: "variable", table: "t1", variableId: "varidexpr" }), "individual"

  describe "findMatchingOpItems", ->
    it "finds = for number", ->
      assert.equal @exprUtils.findMatchingOpItems(lhsExpr: { type: "field", table: "t1", column: "number" })[0].op, "="

    it "finds = for expr number", ->
      assert.equal @exprUtils.findMatchingOpItems(lhsExpr: { type: "field", table: "t1", column: "expr_number" })[0].op, "="

    it "first = for id type non-hierarchical", ->
      assert.equal @exprUtils.findMatchingOpItems(lhsExpr: { type: "id", table: "t1" })[0].op, "="

    it "first within for id type hierarchical", ->
      assert.equal @exprUtils.findMatchingOpItems(lhsExpr: { type: "id", table: "thier" })[0].op, "within"

  describe "getAggrTypes", ->
    beforeEach ->
      @schema = new Schema()
        .addTable({ id: "a", name: "A", ordering: "z", contents: [
          { id: "y", name: "Y", type: "text" }
          { id: "z", name: "Z", type: "number" }
          ]})
      @exprUtils = new ExprUtils(@schema)

    it "includes text (last)", ->
      field = { type: "field", table: "a", column: "y" }
      types = @exprUtils.getAggrTypes(field)
      assert.isTrue "text" in types, JSON.stringify(types)

    it "doesn't include last normally", ->
      @schema = @schema.addTable({ id: "b", name: "B", contents:[{ id: "x", name: "X", type: "text" }]})
      @exprUtils = new ExprUtils(@schema)

      field = { type: "field", table: "b", column: "x" }
      types = @exprUtils.getAggrTypes(field)
      assert.deepEqual types, ["text[]", 'number']
 
  describe "getExprType", ->
    it 'gets field type', ->
      assert.equal @exprUtils.getExprType({ type: "field", table: "t1", column: "text" }), "text"

    it 'gets expr field type', ->
      assert.equal @exprUtils.getExprType({ type: "field", table: "t1", column: "expr_number" }), "number"

    it 'gets join field type', ->
      assert.equal @exprUtils.getExprType({ type: "field", table: "t1", column: "1-2" }), "id[]"

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
    
    it "variable type", ->
      assert.equal @exprUtils.getExprType({ type: "variable", variableId: "varnumber" }), "number"

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
      assert.equal @exprUtils.summarizeExpr(scalarExpr), "T1->T2 > Total Number"

    it "simplifies when count id", ->
      scalarExpr = { type: "scalar", table: "t1", joins: ['1-2'], expr: { type: "id", table: "t2" }, aggr: "count" }
      assert.equal @exprUtils.summarizeExpr(scalarExpr), "T1->T2 > Number of T2" 

    it "simplifies when id", ->
      scalarExpr = { type: "scalar", table: "t2", joins: ['2-1'], expr: { type: "id", table: "t1" } }
      assert.equal @exprUtils.summarizeExpr(scalarExpr), "T2->T1"

    it "shows join types", ->
      scalarExpr = { type: "scalar", table: "t3", joins: ['3-2'], expr: { type: "field", table: "t2", column: "2-1" } }
      assert.equal @exprUtils.summarizeExpr(scalarExpr), "T3->T2 > T2->T1"

    it "summarizes +/-/*//", ->
      fieldExpr = { type: "field", table: "t2", column: "number" }
      literalExpr = { type: "literal", valueType: "number", value: 5 }
      opExpr = { type: "op", op: "+", exprs: [fieldExpr, literalExpr]}
      assert.equal @exprUtils.summarizeExpr(opExpr), "Number + 5"

    it "summarizes case with else", ->
      expr = { 
        type: "case"
        cases: [
          {
            when: { type: "field", table: "t1", column: "boolean" }
            then: { type: "field", table: "t1", column: "text" }
          }
        ]
        else: { type: "field", table: "t1", column: "text" }
      }      
      assert.equal @exprUtils.summarizeExpr(expr), "If Boolean Then Text Else Text"

    it "summarizes = with enum literal", ->
      fieldExpr = { type: "field", table: "t1", column: "enum" }
      literalExpr = { type: "literal", valueType: "enum", value: "a" }
      opExpr = { type: "op", op: "=", exprs: [fieldExpr, literalExpr]}
      assert.equal @exprUtils.summarizeExpr(opExpr), "Enum is A"

    it "summarizes = any with enumset literal", ->
      fieldExpr = { type: "field", table: "t1", column: "enum" }
      literalExpr = { type: "literal", valueType: "enumset", value: ["a", "b"] }
      opExpr = { type: "op", op: "= any", exprs: [fieldExpr, literalExpr]}
      assert.equal @exprUtils.summarizeExpr(opExpr), "Enum is any of A, B"

    it "summarizes contains with enumset literal", ->
      fieldExpr = { type: "field", table: "t1", column: "enumset" }
      literalExpr = { type: "literal", valueType: "enumset", value: ["a"] }
      opExpr = { type: "op", op: "contains", exprs: [fieldExpr, literalExpr]}
      assert.equal @exprUtils.summarizeExpr(opExpr), "EnumSet includes all of A"

    it "summarizes intersects with enumset literal", ->
      fieldExpr = { type: "field", table: "t1", column: "enumset" }
      literalExpr = { type: "literal", valueType: "enumset", value: ["a"] }
      opExpr = { type: "op", op: "intersects", exprs: [fieldExpr, literalExpr]}
      assert.equal @exprUtils.summarizeExpr(opExpr), "EnumSet includes any of A"

    it "summarizes sum(field) expr", ->
      expr = { type: "op", op: "sum", table: "t2", exprs: [{ type: "field", table: "t2", column: "number" }] }
      assert.equal @exprUtils.summarizeExpr(expr), "Total Number"

    it "summarizes count", ->
      expr = { type: "op", op: "count", table: "t1", exprs: [] }
      assert.equal @exprUtils.summarizeExpr(expr), "Number of T1"

    it "summarizes max where expr", ->
      expr = { type: "op", op: "max where", table: "t1", exprs: [{ type: "field", table: "t1", column: "number" }, { type: "field", table: "t1", column: "boolean" }] }
      assert.equal @exprUtils.summarizeExpr(expr), "Maximum Number of Boolean"

    it "summarizes max where expr without rhs", ->
      expr = { type: "op", op: "max where", table: "t1", exprs: [{ type: "field", table: "t1", column: "number" }, null] }
      assert.equal @exprUtils.summarizeExpr(expr), "Maximum Number of All"

    it "summarizes max where without expr or rhs", ->
      expr = { type: "op", op: "max where", table: "t1", exprs: [] }
      assert.equal @exprUtils.summarizeExpr(expr), "Maximum "

    it "summarizes date ops", ->
      expr = { type: "op", op: "thisyear", table: "t1", exprs: [{ type: "field", table: "t1", column: "date" }] }
      assert.equal @exprUtils.summarizeExpr(expr), "Date is this year"

    it "summarizes variable", ->
      expr = { type: "variable", variableId: "varnumber" }
      assert.equal @exprUtils.summarizeExpr(expr), "Varnumber"

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

  describe "stringifyExprLiteral", ->
    it "stringifies number", ->
      str = @exprUtils.stringifyExprLiteral({ type: "field", table: "t1", column: "number" }, 2.34)
      assert.equal str, "2.34"

    it "stringifies expr number", ->
      str = @exprUtils.stringifyExprLiteral({ type: "field", table: "t1", column: "expr_number" }, 2.34)
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

    it "looks up enumset", ->
      str = @exprUtils.stringifyExprLiteral({ type: "field", table: "t1", column: "enumset" }, ["a", "b"])
      assert.equal str, "A, B"

    it "handles null enumset", ->
      str = @exprUtils.stringifyExprLiteral({ type: "field", table: "t1", column: "enumset" }, null)
      assert.equal str, "None"

    it "handles invalid enumset", ->
      str = @exprUtils.stringifyExprLiteral({ type: "field", table: "t1", column: "enumset" }, ["xyz", "b"])
      assert.equal str, "???, B"

    it "handles text[]", ->
      str = @exprUtils.stringifyExprLiteral({ type: "field", table: "t1", column: "text[]" }, ["xyz", "b"])
      assert.equal str, "xyz, b"

  describe "findMatchingOpItems", ->
    it "finds = any for text lhs with boolean result", ->
      opItem = @exprUtils.findMatchingOpItems(resultType: "boolean", exprTypes: ["text"])[0]
      assert.equal opItem.op, "= any"
      assert.equal opItem.exprTypes[0], "text"

  describe "getExprEnumValues", ->
    it "finds in field", ->
      assert.deepEqual @exprUtils.getExprEnumValues({ type: "field", table: "t1", column: "enum" }), [{ id: "a", name: { en: "A" }}, { id: "b", name: { en: "B" }}]

    it "finds in case statement thens", ->
      expr = {
        type: "case"
        cases: [
          { when: null, then: { type: "field", table: "t1", column: "enum" } }
        ]
      }
      assert.deepEqual @exprUtils.getExprEnumValues(expr), [{ id: "a", name: { en: "A" }}, { id: "b", name: { en: "B" }}]

    it "finds in case statement else", ->
      expr = {
        type: "case"
        cases: [
          { when: null, then: null }
        ]
        else: { type: "field", table: "t1", column: "enum" }
      }
      assert.deepEqual @exprUtils.getExprEnumValues(expr), [{ id: "a", name: { en: "A" }}, { id: "b", name: { en: "B" }}]

    it "handles special month case", ->
      expr = {
        type: "op"
        op: "month"
        exprs: [{ type: "field", table: "t1", column: "date" }]
      }
      assert.deepEqual @exprUtils.getExprEnumValues(expr), [
        { id: "01", name: { en: "January" } }
        { id: "02", name: { en: "February" } }
        { id: "03", name: { en: "March" } }
        { id: "04", name: { en: "April" } }
        { id: "05", name: { en: "May" } }
        { id: "06", name: { en: "June" } }
        { id: "07", name: { en: "July" } }
        { id: "08", name: { en: "August" } }
        { id: "09", name: { en: "September" } }
        { id: "10", name: { en: "October" } }
        { id: "11", name: { en: "November" } }
        { id: "12", name: { en: "December" } }
      ]

    it "finds in field", ->
      assert.deepEqual @exprUtils.getExprEnumValues({ type: "variable", variableId: "varenum" }), [{ id: "a", name: { en: "A" }}, { id: "b", name: { en: "B" }}]

  describe "getReferencedFields", ->
    it "gets field", ->
      cols = @exprUtils.getReferencedFields({ type: "field", table: "t1", column: "number" })
      compare(cols, [{ type: "field", table: "t1", column: "number" }])

    it "gets expr field", ->
      cols = @exprUtils.getReferencedFields({ type: "field", table: "t1", column: "expr_number" })
      compare(cols, [{ type: "field", table: "t1", column: "expr_number" }, { type: "field", table: "t1", column: "number" }])

    it "gets join", ->
      cols = @exprUtils.getReferencedFields({ type: "scalar", table: "t1", joins: ["1-2"], expr: { type: "op", table: "t2", exprs: [{ type: "field", table: "t2", column: "number" }]}})
      compare(cols, [{ type: "field", table: "t1", column: "1-2" }, { type: "field", table: "t2", column: "number" }])

    it "recurses into ops", ->
      cols = @exprUtils.getReferencedFields({ type: "op", op: "+", exprs: [{ type: "field", table: "t1", column: "number" }]})
      compare(cols, [{ type: "field", table: "t1", column: "number" }])

    it "includes cases", ->
      cols = @exprUtils.getReferencedFields({ 
        type: "case"
        cases: [
          {
            when: { type: "field", table: "t1", column: "boolean" }
            then: { type: "field", table: "t1", column: "enum" }
          }
        ]
        else: { type: "field", table: "t1", column: "text" }
      })

      compare(cols, [{ type: "field", table: "t1", column: "boolean" }, { type: "field", table: "t1", column: "enum" }, { type: "field", table: "t1", column: "text" }])

    it "includes build enumset", ->
      cols = @exprUtils.getReferencedFields({ 
        type: "build enumset"
        table: "t1"
        values: {
          a: { type: "field", table: "t1", column: "boolean" }
        }
      })

      compare(cols, [{ type: "field", table: "t1", column: "boolean" }])

    it "de-duplicates", ->
      cols = @exprUtils.getReferencedFields({ type: "op", op: "+", exprs: [{ type: "field", table: "t1", column: "number" }, { type: "field", table: "t1", column: "number" }]})
      compare(cols, [{ type: "field", table: "t1", column: "number" }])

  describe "andExprs", ->
    it "handles trivial case", ->
      assert.isNull ExprUtils.andExprs("xyz")
      assert.isNull ExprUtils.andExprs("xyz", null)
      assert.isNull ExprUtils.andExprs("xyz", null, null)

    it "denests", ->
      compare(ExprUtils.andExprs("t1", { type: "field", table: "t1", column: "b1" }, { type: "op", op: "and", exprs: [{ type: "field", table: "t1", column: "b2" }, { type: "field", table: "t1", column: "b3" }] }),
        { type: "op", op: "and", table: "t1", exprs: [
          { type: "field", table: "t1", column: "b1" }
          { type: "field", table: "t1", column: "b2" }
          { type: "field", table: "t1", column: "b3" }
        ]})
