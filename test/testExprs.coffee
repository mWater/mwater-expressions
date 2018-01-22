_ = require 'lodash' 
moment = require 'moment'

# List of test expressions with their value. Each is { expr:, value: }. Can also have context to use to test field, scalar and aggregates
# Value can be truth function
testExprs = []
module.exports = testExprs

add = (expr, value, context) -> testExprs.push({ expr: expr, value: value, context: context })
literal = (value, type) -> { type: "literal", valueType: type, value: value }

# Adds a test for an op. Pass result, op, exprs
addOp = (result, op, exprs...) -> add({ type: "op", op: op, exprs: exprs }, result)

# Create sample rows for testing 
makeRow = (data) ->
  return {
    getPrimaryKey: (callback) -> callback(null, data.id)
    getField: (columnId, callback) -> callback(null, data[columnId])
    getOrdering: (callback) -> callback(null, data.ordering)
  }

# Null
add(null, null)

# Literal
add(literal(3, "number"), 3)

# === Basic ops
addOp(5, "+", literal(3, "number"), literal(2, "number"))
addOp(3, "+", literal(3, "number"), literal(null, "number")) # Note that is different from SQL standard in null handling

addOp(2, "-", literal(3, "number"), literal(1, "number"))
addOp(null, "-", literal(3, "number"), literal(null, "number")) 

addOp(6, "*", literal(3, "number"), literal(2, "number"))
addOp(null, "*", literal(3, "number"), literal(null, "number"))

addOp(3, "/", literal(6, "number"), literal(2, "number"))
addOp(null, "/", literal(6, "number"), literal(null, "number"))
# Divide by zero gives null to prevent SQL errors
addOp(null, "/", literal(6, "number"), literal(0, "number"))

# === and or not
addOp(true, "and", literal(true, "boolean"), literal(true, "boolean"), literal(true, "boolean"))
addOp(false, "and", literal(true, "boolean"), literal(true, "boolean"), literal(false, "boolean"))

# Null handling matches SQL
addOp(null, "and", literal(true, "boolean"), literal(true, "boolean"), literal(null, "boolean"))
addOp(false, "and", literal(false, "boolean"), literal(true, "boolean"), literal(null, "boolean"))

addOp(true, "or", literal(true, "boolean"), literal(true, "boolean"), literal(false, "boolean"))
addOp(false, "or", literal(false, "boolean"), literal(false, "boolean"), literal(false, "boolean"))

# Null handling matches SQL
addOp(null, "or", literal(false, "boolean"), literal(null, "boolean"))
addOp(true, "or", literal(true, "boolean"), literal(null, "boolean"))

# Null handling different than SQL!
addOp(false, "not", literal(true, "boolean"))
addOp(true, "not", literal(false, "boolean"))
addOp(true, "not", literal(null, "boolean"))

addOp(false, "=", literal(1, "number"), literal(2, "number"))
addOp(true, "=", literal(2, "number"), literal(2, "number"))
addOp(null, "=", literal(2, "number"), literal(null, "number"))
addOp(true, "=", literal("a", "text"), literal("a", "text"))

addOp(false, ">", literal(2, "number"), literal(2, "number"))
addOp(true, ">", literal(2, "number"), literal(1, "number"))
addOp(null, ">", literal(2, "number"), literal(null, "number"))

addOp(true, ">=", literal(2, "number"), literal(2, "number"))
addOp(true, ">=", literal(2, "number"), literal(1, "number"))
addOp(null, ">=", literal(2, "number"), literal(null, "number"))

addOp(false, "<", literal(2, "number"), literal(2, "number"))
addOp(true, "<", literal(1, "number"), literal(2, "number"))
addOp(null, "<", literal(1, "number"), literal(null, "number"))

addOp(true, "<=", literal(2, "number"), literal(2, "number"))
addOp(true, "<=", literal(1, "number"), literal(2, "number"))
addOp(null, "<=", literal(1, "number"), literal(null, "number"))

addOp(false, "<>", literal(2, "number"), literal(2, "number"))
addOp(true, "<>", literal(1, "number"), literal(2, "number"))
addOp(null, "<>", literal(1, "number"), literal(null, "number"))

addOp(true, "~*", literal("abc", "text"), literal("ab", "text"))
addOp(true, "~*", literal("ABC", "text"), literal("ab", "text"))
addOp(false, "~*", literal("C", "text"), literal("ab", "text"))

addOp(true, "= false", literal(false, "boolean"))
addOp(false, "= false", literal(true, "boolean"))

addOp(false, "is null", literal(false, "boolean"))
addOp(true, "is null", literal(null, "boolean"))

addOp(true, "is not null", literal(false, "boolean"))
addOp(false, "is not null", literal(null, "boolean"))

addOp(true, "= any", literal("a", "enum"), literal(["a", "b"], "enumset"))
addOp(false, "= any", literal("a", "enum"), literal(["c", "b"], "enumset"))
addOp(null, "= any", literal(null, "enum"), literal(["c", "b"], "enumset"))

addOp(true, "between", literal(3, "number"), literal(2, "number"), literal(4, "number"))
addOp(true, "between", literal(2, "number"), literal(2, "number"), literal(4, "number"))
addOp(false, "between", literal(1, "number"), literal(2, "number"), literal(4, "number"))

addOp(3, "round", literal(3.4, "number"))
addOp(4, "round", literal(3.6, "number"))

addOp(3, "floor", literal(3.6, "number"))
     
addOp(4, "ceiling", literal(3.6, "number"))

addOp(2, "latitude", literal({ type: "Point", coordinates: [1, 2]}, "geometry")) 
addOp(1, "longitude", literal({ type: "Point", coordinates: [1, 2]}, "geometry")) 

addOp(0, "distance", literal({ type: "Point", coordinates: [1, 2]}, "geometry"), literal({ type: "Point", coordinates: [1, 2]}, "geometry"))
addOp(((v) -> v > 310000 and v < 320000), "distance", literal({ type: "Point", coordinates: [1, 2]}, "geometry"), literal({ type: "Point", coordinates: [3, 4]}, "geometry"))

addOp(30, "days difference", literal("2015-12-31", "date"), literal("2015-12-01", "date"))
addOp(1.5, "days difference", literal('2016-06-23T17:36:51.412Z', "datetime"), literal('2016-06-22T05:36:51.412Z', "datetime"))
addOp(null, "days difference", literal('2016-06-23T17:36:51.412Z', "datetime"), literal(null, "datetime"))

addOp(((v) -> v > 0.9 and v < 1.1), "days since", literal(moment().subtract(1, "days").toISOString(), "datetime"))

addOp(true, "contains", literal(["a", "b", "c"], "enumset"), literal(["a", "b"], "enumset"))
addOp(false, "contains", literal(["a", "b", "c"], "enumset"), literal(["a", "b", "d"], "enumset"))
addOp(false, "contains", literal(["a", "b"], "enumset"), literal(["a", "b", "c"], "enumset"))
addOp(null, "contains", literal(null, "enumset"), literal(["c", "b"], "enumset"))

addOp(true, "intersects", literal(["a", "b", "c"], "enumset"), literal(["a", "x"], "enumset"))
addOp(false, "intersects", literal(["a", "b", "c"], "enumset"), literal(["d"], "enumset"))

addOp(2, "length", literal(["a", "b"], "enumset"))
addOp(null, "length", literal(null, "enumset"))

sampleRow = makeRow({ enum: "a" })
add({ type: "op", table: "t1", op: "to text", exprs: [{ type: "field", table: "t1", column: "enum" }] }, "A", { row: sampleRow })

addOp("2.5", "to text", literal(2.5, "number"))

addOp(true, "thisyear", literal(moment().subtract(1, "minutes").format("YYYY-MM-DD"), "date"))
addOp(false, "thisyear", literal(moment().subtract(1, "minutes").add(1, "years").format("YYYY-MM-DD"), "date"))

addOp(false, "lastyear", literal(moment().subtract(1, "minutes").format("YYYY-MM-DD"), "date"))
addOp(true, "lastyear", literal(moment().subtract(1, "minutes").subtract(1, "years").format("YYYY-MM-DD"), "date"))

addOp(true, "thismonth", literal(moment().subtract(1, "minutes").format("YYYY-MM-DD"), "date"))
addOp(false, "thismonth", literal(moment().subtract(1, "minutes").add(1, "years").format("YYYY-MM-DD"), "date"))

addOp(false, "lastmonth", literal(moment().subtract(1, "minutes").format("YYYY-MM-DD"), "date"))
addOp(true, "lastmonth", literal(moment().subtract(1, "minutes").subtract(1, "months").format("YYYY-MM-DD"), "date"))

addOp(true, "today", literal(moment().subtract(1, "minutes").format("YYYY-MM-DD"), "date"))
addOp(false, "today", literal(moment().subtract(1, "minutes").add(1, "years").format("YYYY-MM-DD"), "date"))

addOp(false, "yesterday", literal(moment().subtract(1, "minutes").format("YYYY-MM-DD"), "date"))
addOp(true, "yesterday", literal(moment().subtract(1, "minutes").subtract(1, "days").format("YYYY-MM-DD"), "date"))

addOp(false, "last7days", literal(moment().subtract(1, "minutes").add(3, "days").toISOString(), "date"))
addOp(true, "last7days", literal(moment().subtract(1, "minutes").subtract(1, "days").format("YYYY-MM-DD"), "date"))

addOp(false, "last30days", literal(moment().subtract(1, "minutes").add(3, "days").toISOString(), "date"))
addOp(true, "last30days", literal(moment().subtract(1, "minutes").subtract(1, "days").format("YYYY-MM-DD"), "date"))

addOp(false, "last365days", literal(moment().subtract(1, "minutes").add(3, "days").toISOString(), "date"))
addOp(true, "last365days", literal(moment().subtract(1, "minutes").subtract(1, "days").format("YYYY-MM-DD"), "date"))

addOp(true, "thisyear", literal(new Date().toISOString(), "datetime"))
addOp(false, "thisyear", literal(moment().subtract(1, "minutes").add(1, "years").toISOString(), "datetime"))

addOp(false, "lastyear", literal(new Date().toISOString(), "datetime"))
addOp(true, "lastyear", literal(moment().subtract(1, "minutes").subtract(1, "years").toISOString(), "datetime"))

addOp(true, "thismonth", literal(new Date().toISOString(), "datetime"))
addOp(false, "thismonth", literal(moment().subtract(1, "minutes").add(1, "years").toISOString(), "datetime"))

addOp(false, "lastmonth", literal(new Date().toISOString(), "datetime"))
addOp(true, "lastmonth", literal(moment().subtract(1, "minutes").subtract(1, "months").toISOString(), "datetime"))

addOp(true, "today", literal(new Date().toISOString(), "datetime"))
addOp(false, "today", literal(moment().subtract(1, "minutes").add(1, "years").toISOString(), "datetime"))

addOp(false, "yesterday", literal(new Date().toISOString(), "datetime"))
addOp(true, "yesterday", literal(moment().subtract(1, "minutes").subtract(1, "days").toISOString(), "datetime"))

addOp(false, "last24hours", literal(moment().subtract(1, "minutes").subtract(1, "days").toISOString(), "datetime"))
addOp(true, "last24hours", literal(moment().add(1, "minutes").subtract(1, "days").toISOString(), "datetime"))

addOp(false, "last7days", literal(moment().subtract(1, "minutes").add(3, "days").toISOString(), "datetime"))
addOp(true, "last7days", literal(moment().subtract(1, "minutes").subtract(1, "days").toISOString(), "datetime"))

addOp(false, "last30days", literal(moment().subtract(1, "minutes").add(3, "days").toISOString(), "datetime"))
addOp(true, "last30days", literal(moment().subtract(1, "minutes").subtract(1, "days").toISOString(), "datetime"))

addOp(false, "last365days", literal(moment().subtract(1, "minutes").add(3, "days").toISOString(), "datetime"))
addOp(true, "last365days", literal(moment().subtract(1, "minutes").subtract(1, "days").toISOString(), "datetime"))

addOp(false, "last12months", literal(moment().subtract(1, "minutes").add(3, "days").toISOString(), "datetime"))
addOp(false, "last12months", literal(moment().subtract(1, "minutes").subtract(12, "months").toISOString(), "datetime"))
addOp(true, "last12months", literal(moment().subtract(1, "minutes").subtract(3, "days").toISOString(), "datetime"))

addOp(false, "last6months", literal(moment().subtract(1, "minutes").add(3, "days").toISOString(), "datetime"))
addOp(false, "last6months", literal(moment().subtract(1, "minutes").subtract(6, "months").toISOString(), "datetime"))
addOp(true, "last6months", literal(moment().subtract(1, "minutes").subtract(3, "days").toISOString(), "datetime"))

addOp(false, "last3months", literal(moment().subtract(1, "minutes").add(3, "days").toISOString(), "datetime"))
addOp(false, "last3months", literal(moment().subtract(1, "minutes").subtract(3, "months").toISOString(), "datetime"))
addOp(true, "last3months", literal(moment().subtract(1, "minutes").subtract(3, "days").toISOString(), "datetime"))

addOp("1", "weekofmonth", literal("2015-05-07", "date"))
addOp("2", "weekofmonth", literal("2015-05-08", "date"))

addOp("1", "weekofmonth", literal("2015-05-07", "datetime"))
addOp("2", "weekofmonth", literal("2015-05-08", "datetime"))

addOp("05", "month", literal("2015-05-08", "date"))
addOp("05", "month", literal("2015-05-08", "datetime"))

addOp("2015-05-01", "yearmonth", literal("2015-05-08", "date"))
addOp("2015-05-01", "yearmonth", literal("2015-05-08", "datetime"))

addOp("2015-01-01", "year", literal("2015-05-08", "date"))
addOp("2015-01-01", "year", literal("2015-05-08", "datetime"))

sampleRows = [
  makeRow(id: "1", a: 1, b: 1, c: true, d: true, e: "x", f: 1, ordering: 3)
  makeRow(id: "2", a: 2, b: 4, c: false, d: true, e: "y", f: null, ordering: 4)
  makeRow(id: "3", a: 3, b: 9, c: true, d: true, e: "z", f: 3, ordering: 1)
  makeRow(id: "4", a: 4, b: 16, c: false, d: false, e: "z", f: 4, ordering: 2)
]

add({ type: "field", table: "t1", column: "a" }, 4, { row: makeRow(a: 4) })

# expression columns
add({ type: "field", table: "t1", column: "expr_number" }, 4, { row: makeRow(number: 4) })

add({ type: "id", table: "t1" }, "1", { row: makeRow(id: "1", a: 4) })

add({ type: "op", table: "t1", op: "sum", exprs: [{ type: "field", table: "t1", column: "a" }] }, 10, { rows: sampleRows })
add({ type: "op", table: "t1", op: "avg", exprs: [{ type: "field", table: "t1", column: "a" }] }, 2.5, { rows: sampleRows })
add({ type: "op", table: "t1", op: "min", exprs: [{ type: "field", table: "t1", column: "a" }] }, 1, { rows: sampleRows })
add({ type: "op", table: "t1", op: "max", exprs: [{ type: "field", table: "t1", column: "a" }] }, 4, { rows: sampleRows })
add({ type: "op", table: "t1", op: "count", exprs: [] }, 4, { rows: sampleRows })

# TODO: Doesn't work for evaluating correctly as is a window function. Use 100 for now
add({ type: "op", table: "t1", op: "percent", exprs: [] }, 100, { rows: sampleRows })

add({ type: "op", table: "t1", op: "last", exprs: [{ type: "field", table: "t1", column: "a" }] }, 2, { rows: sampleRows })
add({ type: "op", table: "t1", op: "last", exprs: [{ type: "field", table: "t1", column: "f" }] }, 1, { rows: sampleRows })

add({ type: "op", table: "t1", op: "last where", exprs: [{ type: "field", table: "t1", column: "a" }, { type: "field", table: "t1", column: "c" }] }, 1, { rows: sampleRows })
add({ type: "op", table: "t1", op: "last where", exprs: [{ type: "field", table: "t1", column: "a" }, null] }, 2, { rows: sampleRows })
add({ type: "op", table: "t1", op: "last where", exprs: [{ type: "field", table: "t1", column: "f" }, null] }, 1, { rows: sampleRows })

add({ type: "op", table: "t1", op: "count where", exprs: [{ type: "field", table: "t1", column: "c" }] }, 2, { rows: sampleRows })

add({ type: "op", table: "t1", op: "sum where", exprs: [{ type: "field", table: "t1", column: "a" }, { type: "field", table: "t1", column: "c" }] }, 4, { rows: sampleRows })

add({ type: "op", table: "t1", op: "percent where", exprs: [{ type: "field", table: "t1", column: "c" }] }, 50, { rows: sampleRows })
add({ type: "op", table: "t1", op: "percent where", exprs: [{ type: "field", table: "t1", column: "c" }, { type: "field", table: "t1", column: "d" }] }, ((v) -> Math.abs(v- 200/3) < 0.1), { rows: sampleRows })

add({ type: "op", table: "t1", op: "count distinct", exprs: [{ type: "field", table: "t1", column: "e" }] }, 3, { rows: sampleRows })

# Row with join
singleJoinRow = makeRow(j: makeRow(id: "j1", a:1, b:2))

add({ type: "scalar", joins: ["j"], expr: { type: "field", table: "t2", column: "b" }}, 2, { row: singleJoinRow })

add({ type: "field", table: "t1", column: "j" }, "j1", { row: singleJoinRow })

# Row with join
multipleJoinRow = makeRow(j: sampleRows)

add({ type: "scalar", joins: ["j"], expr: { type: "op", table: "t2", op: "sum", exprs: [{ type: "field", table: "t2", column: "a" }] }}, 10, { row: multipleJoinRow })

add({ type: "field", table: "t1", column: "j" }, ["1", "2", "3", "4"], { row: multipleJoinRow })


# Row with joins
multipleJoinsRow = makeRow(j1: makeRow(j2: sampleRows))
add({ type: "scalar", joins: ["j1", "j2"], expr: { type: "op", table: "t3", op: "sum", exprs: [{ type: "field", table: "t3", column: "a" }] }}, 10, { row: multipleJoinsRow })

multipleJoinsRow = makeRow(j1: null)
add({ type: "scalar", joins: ["j1", "j2"], expr: { type: "op", table: "t3", op: "sum", exprs: [{ type: "field", table: "t3", column: "a" }] }}, null, { row: multipleJoinsRow })


# Scalar with no row
add({ type: "scalar", joins: ["j"], expr: { type: "field", table: "t2", column: "b" }}, null, { row: makeRow(j: null) })


# addOpItem(op: "within", name: "in", resultType: "boolean", exprTypes: ["id", "id"], lhsCond: (lhsExpr, exprUtils) => 
#   lhsIdTable = exprUtils.getExprIdTable(lhsExpr)
#   if lhsIdTable
#     return exprUtils.schema.getTable(lhsIdTable).ancestry?
#   return false
# )

add({ 
  type: "case"
  cases: [
    { when: literal(true, "boolean"), then: { type: "literal", valueType: "number", value: 1 } }
    { when: literal(true, "boolean"), then: { type: "literal", valueType: "number", value: 2 } }
  ]
  else: { type: "literal", valueType: "number", value: 3 }
}, 1)

add({ 
  type: "case"
  cases: [
    { when: literal(false, "boolean"), then: { type: "literal", valueType: "number", value: 1 } }
    { when: literal(true, "boolean"), then: { type: "literal", valueType: "number", value: 2 } }
  ]
  else: { type: "literal", valueType: "number", value: 3 }
}, 2)

add({ 
  type: "case"
  cases: [
    { when: literal(false, "boolean"), then: { type: "literal", valueType: "number", value: 1 } }
    { when: literal(false, "boolean"), then: { type: "literal", valueType: "number", value: 2 } }
  ]
  else: { type: "literal", valueType: "number", value: 3 }
}, 3)

add({
  type: "score"
  input: literal("a", "enum")
  scores: {
    a: { type: "literal", valueType: "number", value: 3 }
    b: { type: "literal", valueType: "number", value: 4 }
  }
}, 3)

add({
  type: "score"
  input: literal("c", "enum")
  scores: {
    a: { type: "literal", valueType: "number", value: 3 }
    b: { type: "literal", valueType: "number", value: 4 }
  }
}, 0)


add({
  type: "score"
  input: literal(["a"], "enumset")
  scores: {
    a: { type: "literal", valueType: "number", value: 3 }
    b: { type: "literal", valueType: "number", value: 4 }
  }
}, 3)

add({
  type: "score"
  input: literal(["a", "b"], "enumset")
  scores: {
    a: { type: "literal", valueType: "number", value: 3 }
    b: { type: "literal", valueType: "number", value: 4 }
  }
}, 7)

# Build enumset
add({
  type: "build enumset"
  values: {
    a: { type: "literal", valueType: "boolean", value: true }
    b: { type: "literal", valueType: "boolean", value: false }
    c: { type: "literal", valueType: "boolean", value: true }
  }
}, ["a", "c"])

# is latest
sampleRows = [
  makeRow(id: "1", a: "x", b: true, ordering: 3)
  makeRow(id: "2", a: "y", b: true, ordering: 4)
  makeRow(id: "3", a: "x", b: false, ordering: 1)
  makeRow(id: "4", a: "y", b: false, ordering: 2)
]

add({ type: "op", op: "is latest", table: "t1", exprs: [{ type: "field", table: "t1", column: "a" }] }, true, { row: sampleRows[0], rows: sampleRows })
add({ type: "op", op: "is latest", table: "t1", exprs: [{ type: "field", table: "t1", column: "a" }] }, true, { row: sampleRows[1], rows: sampleRows })
add({ type: "op", op: "is latest", table: "t1", exprs: [{ type: "field", table: "t1", column: "a" }] }, false, { row: sampleRows[2], rows: sampleRows })
add({ type: "op", op: "is latest", table: "t1", exprs: [{ type: "field", table: "t1", column: "a" }] }, false, { row: sampleRows[3], rows: sampleRows })

# expression columns
add({ type: "field", table: "t1", column: "expr_number" }, 4, { row: makeRow(number: 4) })





#   describe "scalar", ->
#     it "n-1 scalar", ->
#       @check({ type: "scalar", joins: ['x'], expr: { type: "field", table: "t2", column: "y" }}, { x: { getField: (col) -> (if col == "y" then 4) }}, 4)

#     it "n-1 null scalar", ->
#       @check({ type: "scalar", joins: ['x'], expr: { type: "field", table: "t2", column: "y" }}, { x: null }, null)
