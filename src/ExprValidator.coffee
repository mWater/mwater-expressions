_ = require 'lodash'
ExprUtils = require './ExprUtils'

# Validates expressions. If an expression has been cleaned, it will always be valid
module.exports = class ExprValidator
  constructor: (schema) ->
    @schema = schema
    @exprUtils = new ExprUtils(schema)

  # Validates an expression, returning null if it is valid, otherwise return an error string
  # options are:
  #   table: optional current table. expression must be related to this table or will be stripped
  #   types: optional types to limit to
  #   enumValueIds: ids of enum values that are valid if type is enum
  #   idTable: table that type of id must be from
  #   aggrStatuses: statuses of aggregation to allow. list of "individual", "literal", "aggregate". Default: ["individual", "literal"]
  validateExpr: (expr, options={}) ->
    _.defaults(options, {
      aggrStatuses: ["individual", "literal"]
      })

    if not expr
      return null

    # Allow {} placeholder
    if _.isEmpty(expr)
      return null

    # Check table
    if options.table and expr.table != options.table
      return "Wrong table"

    # Literal is ok if right type
    switch expr.type
      when "literal"
        if options.types and expr.valueType not in options.types
          return "Wrong type"

        if options.idTable and expr.valueType == "id" and options.idTable != expr.idTable
          return "Wrong table"

      when "field"
        column = @schema.getColumn(expr.table, expr.column)
        if not column
          return "Missing column"

        # Validate expression
        if column.expr
          error = @validateExpr(column.expr, options)
          if error
            return error

      when "op"
        # Validate exprs
        for subexpr in expr.exprs
          error = @validateExpr(subexpr, _.omit(options, "types"))
          if error
            return error

        # Find op
        opItems = @exprUtils.findMatchingOpItems(op: expr.op, lhsExpr: expr.exprs[0], resultTypes: options.types)
        if opItems.length == 0
          return "No matching op"

      when "case"
        # Validate cases
        for cse in expr.cases
          error = @validateExpr(cse.when, _.extend({}, options, types: ["boolean"]))
          if error
            return error

          error = @validateExpr(cse.then, options)
          if error
            return error

        error = @validateExpr(expr.else, options)
        if error
          return error

      when "score"
        error = @validateExpr(expr.input, _.extend({}, options, types: ["enum", "enumset"]))
        if error
          return error

        if expr.input
          enumValueIds = _.pluck(@exprUtils.getExprEnumValues(expr.input), "id")
        else
          enumValueIds = null

        for key, value of expr.scores
          if enumValueIds and key not in enumValueIds
            return "Invalid score enum"

          error = @validateExpr(value, _.extend({}, options, types: ["number"]))
          if error
            return error

    # Validate table
    if options.idTable and @exprUtils.getExprIdTable(expr) and @exprUtils.getExprIdTable(expr) != options.idTable
      return "Wrong idTable"

    # Validate type if present
    if options.types and @exprUtils.getExprType(expr) and @exprUtils.getExprType(expr) not in options.types
      return "Invalid type"

    # Validate enums
    if options.enumValueIds and @exprUtils.getExprType(expr) in ['enum', 'enumset']
      if _.difference(_.pluck(@exprUtils.getExprEnumValues(expr), "id"), options.enumValueIds).length > 0
        return "Invalid enum"

    return null


