

# Evaluates an expression given a row
# row is a plain object that has the following functions as properties:
#  getPrimaryKey() : returns primary key of row
#  getField(columnId) : gets the value of a column
# For joins, getField will return array of rows for 1-n and n-n joins and a row for n-1 and 1-1 joins
module.exports = class ExprEvaluator
  evaluate: (expr, row) ->
    if not expr?
      return null

    switch expr.type
      when "field"
        return row.getField(expr.column)
      else
        throw new Error("Unsupported expression type #{expr.type}")



    # TODO distance
    # TODO ops
    # TODO case