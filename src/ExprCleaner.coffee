_ = require 'lodash'
ExprUtils = require './ExprUtils'

# Cleans expressions. Cleaning means nulling invalid (not just incomplete) expressions if they cannot be auto-fixed.
module.exports = class ExprCleaner
  constructor: (schema) ->
    @schema = schema
    @exprUtils = new ExprUtils(schema)

  # Clean an expression, returning null if completely invalid, otherwise removing
  # invalid parts. Attempts to correct invalid types by wrapping in other expressions.
  # e.g. if an enum is chosen when a boolean is required, it will be wrapped in "= any" op
  # options are:
  #   table: optional current table. expression must be related to this table or will be stripped
  #   types: optional array of types to limit to
  cleanExpr: (expr, options={}) ->
    if not expr
      return null

    # Allow {} placeholder
    if _.isEmpty(expr)
      return expr

    # Strip if wrong table
    if options.table and expr.type != "literal" and expr.table != options.table
      return null

    # Strip if non-existent table
    if expr.table and not @schema.getTable(expr.table)
      return null

    # Get type
    type = @exprUtils.getExprType(expr)

    # If a type is required and expression is not, attempt to wrap with an op
    if options.types and type not in options.types
      for allowedType in options.types
        op = @exprUtils.findOpByResultType(allowedType, type)
        if op
          # Found op that would convert type. Use it.
          expr = { type: "op", op: op, table: expr.table, exprs: [expr] }

    switch expr.type
      when "field"
        return @cleanFieldExpr(expr)
      when "scalar"
        return @cleanScalarExpr(expr)
      when "comparison"
        return @cleanComparisonExpr(expr)
      when "logical"
        return @cleanLogicalExpr(expr)
      when "count"
        return expr
      when "op"
        return @cleanOpExpr(expr, options)
      else
        throw new Error("Unknown expression type #{expr.type}")

  # Removes references to non-existent tables
  cleanFieldExpr: (expr) ->
    # Empty expression
    if not expr.column or not expr.table
      return null

    # Missing table
    if not @schema.getTable(expr.table)
      return null

    # Missing column
    if not @schema.getColumn(expr.table, expr.column)
      return null

    return expr

  cleanOpExpr: (expr, options) ->
    switch expr.op
      when "and", "or"
        return  _.extend({}, expr, exprs: _.map(expr.exprs, (e) => @cleanExpr(e, options)))
      else
        return expr

  # Determines if an set of joins are valid
  areJoinsValid: (table, joins) ->
    t = table
    for j in joins
      joinCol = @schema.getColumn(t, j)
      if not joinCol 
        return false

      t = joinCol.join.toTable

    return true

  # Strips/defaults invalid aggr and where of a scalar expression
  cleanScalarExpr: (expr) ->
    if not @exprUtils.areJoinsValid(expr.table, expr.joins)
      return null

    if expr.aggr and not @exprUtils.isMultipleJoins(expr.table, expr.joins)
      expr = _.omit(expr, "aggr")

    if @exprUtils.isMultipleJoins(expr.table, expr.joins) and expr.aggr not in _.pluck(@exprUtils.getAggrs(expr.expr), "id")
      expr = _.extend({}, expr, { aggr: @exprUtils.getAggrs(expr.expr)[0].id })

    # Clean where
    if expr.where
      expr.where = @cleanExpr(expr.where)

    return expr

  # Removes parts that are invalid, leaving table alone
  cleanComparisonExpr: (expr) =>
    # TODO always creates new
    expr = _.extend({}, expr, lhs: @cleanExpr(expr.lhs, expr.table))

    # Remove op, rhs if no lhs
    if not expr.lhs 
      expr = { type: "comparison", table: expr.table }

    # Remove op if wrong type
    if expr.op and expr.op not in _.pluck(@exprUtils.getComparisonOps(@exprUtils.getExprType(expr.lhs)), "id")
      expr = _.omit(expr, "op")

    # Default op
    if expr.lhs and not expr.op
      expr = _.extend({}, expr, op: @exprUtils.getComparisonOps(@exprUtils.getExprType(expr.lhs))[0].id)

    if expr.op and expr.rhs and expr.lhs
      # Remove rhs if wrong type
      if @exprUtils.getComparisonRhsType(@exprUtils.getExprType(expr.lhs), expr.op) != @exprUtils.getExprType(expr.rhs)
        expr = _.omit(expr, "rhs")        
      # Remove rhs if wrong enum
      else if @exprUtils.getComparisonRhsType(@exprUtils.getExprType(expr.lhs), expr.op) == "enum" 
        if expr.rhs.type == "literal" and expr.rhs.value not in _.pluck(@exprUtils.getExprValues(expr.lhs), "id")
          expr = _.omit(expr, "rhs")
      # Remove rhs if empty enum list
      else if @exprUtils.getComparisonRhsType(@exprUtils.getExprType(expr.lhs), expr.op) == "enum[]" 
        if expr.rhs.type == "literal"
          # Filter invalid values
          expr.rhs.value = _.intersection(_.pluck(@exprUtils.getExprValues(expr.lhs), "id"), expr.rhs.value)

          # Remove if empty
          if expr.rhs.value.length == 0
            expr = _.omit(expr, "rhs")
      else if @exprUtils.getComparisonRhsType(@exprUtils.getExprType(expr.lhs), expr.op) == "text[]" 
        if expr.rhs.type == "literal"
          # Remove if empty
          if expr.rhs.value.length == 0
            expr = _.omit(expr, "rhs")

    return expr

  cleanLogicalExpr: (expr) =>
    # TODO always makes new
    expr = _.extend({}, expr, exprs: _.map(expr.exprs, (e) => @cleanComparisonExpr(e)))

