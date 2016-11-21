assert = require('chai').assert
_ = require 'lodash'
canonical = require 'canonical-json'
moment = require 'moment'
sinon = require 'sinon'

ExprEvaluator = require '../src/ExprEvaluator'
testExprs = require './testExprs'

compare = (actual, expected) ->
  if _.isObject(actual) or _.isArray(actual)
    assert.equal canonical(actual), canonical(expected), "\ngot:" + canonical(actual) + "\nexp:" + canonical(expected) + "\n"
  else
    assert.equal actual, expected
 
describe "ExprEvaluator", ->
  for testExpr in testExprs
    do (testExpr) =>
      it JSON.stringify(testExpr.expr), (done) ->
        ev = new ExprEvaluator()
        ev.evaluate(testExpr.expr, testExpr.context, (error, value) =>
          if error
            throw error

          if _.isFunction(testExpr.value)
            assert.isTrue testExpr.value(value)
          else
            compare(value, testExpr.value)
          done()
          )

