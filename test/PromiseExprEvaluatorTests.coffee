assert = require('chai').assert
_ = require 'lodash'
canonical = require 'canonical-json'
moment = require 'moment'
sinon = require 'sinon'
fixtures = require './fixtures'

PromiseExprEvaluator = require('../src/PromiseExprEvaluator').PromiseExprEvaluator
testExprs = require './testExprs'

compare = (actual, expected) ->
  if _.isObject(actual) or _.isArray(actual)
    assert.equal canonical(actual), canonical(expected), "\ngot:" + canonical(actual) + "\nexp:" + canonical(expected) + "\n"
  else
    assert.equal actual, expected
 
variableValues = {
  varenum: { type: "literal", valueType: "enum", value: "a" } 
  varnumber: { type: "literal", valueType: "number", value: 123 } 
  varnumberexpr: { type: "op", op: "+", table: "t1", exprs: [
    { type: "field", table: "t1", column: "number" }
    { type: "literal", valueType: "number", value: 2 }
  ]}
}

describe "PromiseExprEvaluator", ->
  for testExpr in testExprs
    do (testExpr) =>
      it JSON.stringify(testExpr.expr), =>
        ev = new PromiseExprEvaluator({ schema: fixtures.simpleSchema(), locale: "en", variableValues: variableValues })
        value = await ev.evaluate(testExpr.expr, testExpr.context)
        if _.isFunction(testExpr.value)
          assert.isTrue testExpr.value(value)
        else
          compare(value, testExpr.value)

  it "does simple expressions synchronously", -> 
    ev = new PromiseExprEvaluator({ schema: fixtures.simpleSchema(), locale: "en", variableValues: variableValues })
    
    assert.equal ev.evaluateSync({ type: "literal", valueType: "number", value: 1234 }), 1234
    
    assert.equal ev.evaluateSync({ type: "op", op: "+", exprs: [{ type: "literal", valueType: "number", value: 1234 }, { type: "literal", valueType: "number", value: 1 }]}), 1235

    assert.equal ev.evaluateSync({ type: "op", op: "+", exprs: [{ type: "variable", variableId: "varnumber" }, { type: "literal", valueType: "number", value: 1 }]}), 124
