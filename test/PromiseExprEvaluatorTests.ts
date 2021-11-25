// TODO: This file was created by bulk-decaffeinate.
// Sanity-check the conversion and remove this comment.
import { assert } from "chai"
import _ from "lodash"
import canonical from "canonical-json"
import moment from "moment"
import sinon from "sinon"
import * as fixtures from "./fixtures"
import { PromiseExprEvaluator } from "../src/PromiseExprEvaluator"
import testExprs from "./testExprs"
import { setupTestExtension } from "./extensionSetup"
import { Variable } from "../src"
setupTestExtension()

function compare(actual: any, expected: any) {
  if (_.isObject(actual) || _.isArray(actual)) {
    assert.equal(
      canonical(actual),
      canonical(expected),
      "\ngot:" + canonical(actual) + "\nexp:" + canonical(expected) + "\n"
    )
  } else {
    assert.equal(actual, expected)
  }
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

const variableValues = {
  varenum: { type: "literal", valueType: "enum", value: "a" },
  varnumber: { type: "literal", valueType: "number", value: 123 },
  varnumberexpr: {
    type: "op",
    op: "+",
    table: "t1",
    exprs: [
      { type: "field", table: "t1", column: "number" },
      { type: "literal", valueType: "number", value: 2 }
    ]
  }
}

describe("PromiseExprEvaluator", function () {
  for (let testExpr of testExprs) {
    ;((testExpr) => {
      return it(JSON.stringify(testExpr.expr), async () => {
        const ev = new PromiseExprEvaluator({
          schema: fixtures.simpleSchema(),
          locale: "en",
          variables,
          variableValues
        })
        const value = await ev.evaluate(testExpr.expr, testExpr.context)
        if (_.isFunction(testExpr.value)) {
          assert.isTrue(testExpr.value(value))
        } else {
          return compare(value, testExpr.value)
        }
      })
    })(testExpr)
  }

  return it("does simple expressions synchronously", function () {
    const ev = new PromiseExprEvaluator({ schema: fixtures.simpleSchema(), locale: "en", variables, variableValues })

    assert.equal(ev.evaluateSync({ type: "literal", valueType: "number", value: 1234 }), 1234)

    assert.equal(
      ev.evaluateSync({
        type: "op",
        op: "+",
        exprs: [
          { type: "literal", valueType: "number", value: 1234 },
          { type: "literal", valueType: "number", value: 1 }
        ]
      }),
      1235
    )

    assert.equal(
      ev.evaluateSync({
        type: "op",
        op: "+",
        exprs: [
          { type: "variable", variableId: "varnumber" },
          { type: "literal", valueType: "number", value: 1 }
        ]
      }),
      124
    )
  })
})
