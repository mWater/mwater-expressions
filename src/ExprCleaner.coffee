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
  #   type: optional types to limit to
  cleanExpr: (expr, options={}) ->
    if not expr
      return null

    # Allow {} placeholder
    if _.isEmpty(expr)
      return expr

    # Handle upgrades from old version
    if expr.type == "comparison"
      return @cleanComparisonExpr(expr, options)
    if expr.type == "logical"
      return @cleanLogicalExpr(expr, options)
      
    # Strip if wrong table 
    if options.table and expr.type != "literal" and expr.table != options.table
      return null

    # Strip if no table
    if not expr.table and expr.type != "literal" 
      return null

    # Strip if non-existent table
    if expr.table and not @schema.getTable(expr.table)
      return null

    # Get type
    type = @exprUtils.getExprType(expr)

    # Strip if wrong type
    if type and options.type and type != options.type
      return null

    switch expr.type
      when "field"
        return @cleanFieldExpr(expr, options)
      when "scalar"
        return @cleanScalarExpr(expr, options)
      when "count"
        return expr
      when "op"
        return @cleanOpExpr(expr, options)
      when "literal"
        return @cleanLiteralExpr(expr, options)
      else
        throw new Error("Unknown expression type #{expr.type}")

  # Removes references to non-existent tables
  cleanFieldExpr: (expr, options) ->
    # Empty expression
    if not expr.column or not expr.table
      return null

    # Missing table
    if not @schema.getTable(expr.table)
      return null

    # Missing column
    column = @schema.getColumn(expr.table, expr.column)
    if not column
      return null

    # Invalid enums
    if options.valueIds and column.type == "enum"
      if _.difference(_.pluck(column.values, "id"), options.valueIds).length > 0
        return null

    return expr

  cleanOpExpr: (expr, options) ->
    switch expr.op
      when "and", "or"
        expr = _.extend({}, expr, exprs: _.map(expr.exprs, (e) => @cleanExpr(e, type: "boolean", table: expr.table)))

        # Simplify
        if expr.exprs.length == 1
          return expr.exprs[0]
        if expr.exprs.length == 0
          return null

        return expr
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
  cleanScalarExpr: (expr, options) ->
    if expr.joins.length == 0
      return @cleanExpr(expr.expr, options)

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

  cleanLiteralExpr: (expr, options) ->
    # TODO strip if no value?

    # Remove if enum type is wrong
    if expr.valueType == "enum" and options.valueIds and expr.value and expr.value not in options.valueIds
      return null

    # Remove invalid enum types
    if expr.valueType == "enum[]" and options.valueIds and expr.value
      expr = _.extend({}, expr, value: _.intersection(options.valueIds, expr.value))

    return expr

  cleanComparisonExpr: (expr, options) =>
    # Upgrade to op
    newExpr = { type: "op", table: expr.table, op: expr.op, exprs: [expr.lhs] }
    if expr.rhs
      newExpr.exprs.push(expr.rhs)

    # If = true
    if expr.op == "= true"
      newExpr = expr.lhs

    if expr.op == "= false"
      newExpr = { type: "op", op: "not", table: expr.table, exprs: [expr.lhs] }

    return @cleanExpr(newExpr, options)

  cleanLogicalExpr: (expr, options) =>
    newExpr = { type: "op", op: expr.op, table: expr.table, exprs: expr.exprs }

    return @cleanExpr(newExpr, options)

