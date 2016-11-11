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

# Null
add(null, null)

# Literal
add(literal(3, "number"), 3)

# === Basic ops
addOp(5, "+", literal(3, "number"), literal(2, "number"))
addOp(null, "+", literal(3, "number"), literal(null, "number"))

addOp(2, "-", literal(3, "number"), literal(1, "number"))
addOp(null, "-", literal(3, "number"), literal(null, "number"))

addOp(6, "*", literal(3, "number"), literal(2, "number"))
addOp(null, "*", literal(3, "number"), literal(null, "number"))

addOp(3, "/", literal(6, "number"), literal(2, "number"))
addOp(null, "/", literal(6, "number"), literal(null, "number"))

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

addOp(false, "not", literal(true, "boolean"))
addOp(true, "not", literal(false, "boolean"))
addOp(null, "not", literal(null, "boolean"))

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

addOp(2, "length", literal(["a", "b"], "enumset"))
addOp(null, "length", literal(null, "enumset"))

# TODO "to text" requires a schema! 
# addOpItem(op: "to text", name: "Convert to text", resultType: "text", exprTypes: ["enum"], prefix: true)

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

addOp(false, "last7days", literal(moment().subtract(1, "minutes").add(3, "days").toISOString(), "datetime"))
addOp(true, "last7days", literal(moment().subtract(1, "minutes").subtract(1, "days").toISOString(), "datetime"))

addOp(false, "last30days", literal(moment().subtract(1, "minutes").add(3, "days").toISOString(), "datetime"))
addOp(true, "last30days", literal(moment().subtract(1, "minutes").subtract(1, "days").toISOString(), "datetime"))

addOp(false, "last365days", literal(moment().subtract(1, "minutes").add(3, "days").toISOString(), "datetime"))
addOp(true, "last365days", literal(moment().subtract(1, "minutes").subtract(1, "days").toISOString(), "datetime"))


# Create sample rows for testing aggregation
sampleRow = (data) ->
  return {
    getPrimaryKey: (callback) -> callback(null, data.id)
    getField: (columnId, callback) -> callback(null, data[columnId])
    getOrdering: (columnId, callback) -> callback(null, data.ordering)
  }

sampleRows = [
  sampleRow(id: "1", a: 1, b: 1, ordering: 4)
  sampleRow(id: "1", a: 2, b: 4, ordering: 1)
  sampleRow(id: "1", a: 3, b: 9, ordering: 2)
  sampleRow(id: "1", a: 4, b: 16, ordering: 3)
]

add({ type: "field", table: "t1", column: "a" }, 4, { row: sampleRow(a: 4) })

add({ type: "op", table: "t1", op: "sum", exprs: [{ type: "field", table: "t1", column: "a" }] }, 10, { rows: sampleRows })
add({ type: "op", table: "t1", op: "avg", exprs: [{ type: "field", table: "t1", column: "a" }] }, 2.5, { rows: sampleRows })
add({ type: "op", table: "t1", op: "min", exprs: [{ type: "field", table: "t1", column: "a" }] }, 1, { rows: sampleRows })
add({ type: "op", table: "t1", op: "max", exprs: [{ type: "field", table: "t1", column: "a" }] }, 4, { rows: sampleRows })

# for type in ['text', 'number', 'enum', 'enumset', 'boolean', 'date', 'datetime', 'geometry']
#   addOpItem(op: "last", name: "Latest", resultType: type, exprTypes: [type], prefix: true, aggr: true, ordered: true)
#   addOpItem(op: "last where", name: "Latest that", resultType: type, exprTypes: [type, "boolean"], prefix: true, prefixLabel: "Latest", aggr: true, ordered: true, rhsLiteral: false, joiner: "that", rhsPlaceholder: "All")

# addOpItem(op: "percent where", name: "Percent that", resultType: "number", exprTypes: ["boolean", "boolean"], prefix: true, aggr: true, rhsLiteral: false, joiner: "of", rhsPlaceholder: "All")
# addOpItem(op: "count where", name: "Number that", resultType: "number", exprTypes: ["boolean"], prefix: true, aggr: true)
# addOpItem(op: "sum where", name: "Total that", resultType: "number", exprTypes: ["number", "boolean"], prefix: true, prefixLabel: "Total", aggr: true, rhsLiteral: false, joiner: "that", rhsPlaceholder: "All")

# addOpItem(op: "within", name: "in", resultType: "boolean", exprTypes: ["id", "id"], lhsCond: (lhsExpr, exprUtils) => 
#   lhsIdTable = exprUtils.getExprIdTable(lhsExpr)
#   if lhsIdTable
#     return exprUtils.schema.getTable(lhsIdTable).ancestry?
#   return false
# )

# addOpItem(op: "count", name: "Number of", resultType: "number", exprTypes: [], prefix: true, aggr: true)

