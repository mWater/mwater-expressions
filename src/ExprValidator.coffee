_ = require 'lodash'
ExprUtils = require './ExprUtils'
WeakCache = require('./WeakCache').WeakCache

# Weak cache is global to allow validator to be created and destroyed
weakCache = new WeakCache()

# Validates expressions. If an expression has been cleaned, it will always be valid
module.exports = class ExprValidator
  constructor: (schema, variables = []) ->
    @schema = schema
    @variables = variables
    @exprUtils = new ExprUtils(schema, variables)

  # Validates an expression, returning null if it is valid, otherwise return an error string
  # NOTE: This uses global weak caching and assumes that expressions are never mutated after
  # having been validated!
  # options are:
  #   table: optional current table. expression must be related to this table or will be stripped
  #   types: optional types to limit to
  #   enumValueIds: ids of enum values that are valid if type is enum
  #   idTable: table that type of id must be from
  #   aggrStatuses: statuses of aggregation to allow. list of "individual", "literal", "aggregate". Default: ["individual", "literal"]
  validateExpr: (expr, options={}) ->
    if not expr
      return null

    if not @schema 
      return weakCache.cacheFunction([expr], [@variables, options], () => 
        return @validateExprInternal(expr, options)
      )

    return weakCache.cacheFunction([@schema, expr], [@variables, options], () => 
      return @validateExprInternal(expr, options)
    )

  validateExprInternal: (expr, options) =>
    aggrStatuses = options.aggrStatuses or aggrStatuses: ["individual", "literal"]

    if not expr
      return null

    # Allow {} placeholder
    if _.isEmpty(expr)
      return null

    # Prevent infinite recursion
    if options.depth > 100
      return "Circular reference"

    # Check table if not literal
    if options.table and expr.table and expr.table != options.table 
      return "Wrong table #{expr.table} (expected #{options.table})"

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
          # Use depth to prevent infinite recursion
          error = @validateExprInternal(column.expr, _.extend({}, options, depth: (options.depth or 0) + 1))
          if error
            return error

      when "op"
        # Validate exprs
        for subexpr in expr.exprs
          error = @validateExprInternal(subexpr, _.omit(options, "types", "enumValueIds", "idTable"))
          if error
            return error

        # Find op
        opItems = @exprUtils.findMatchingOpItems(op: expr.op, lhsExpr: expr.exprs[0], resultTypes: options.types)
        if opItems.length == 0
          return "No matching op"

      when "scalar"
        # Validate joins
        if not @exprUtils.areJoinsValid(expr.table, expr.joins)
          return "Invalid joins"

        exprTable = @exprUtils.followJoins(expr.table, expr.joins)
        error = @validateExprInternal(expr.expr, _.extend({}, options, table: exprTable))
        if error
          return error

      when "case"
        # Validate cases
        for cse in expr.cases
          error = @validateExprInternal(cse.when, _.extend({}, options, types: ["boolean"]))
          if error
            return error

          error = @validateExprInternal(cse.then, options)
          if error
            return error

        error = @validateExprInternal(expr.else, options)
        if error
          return error

      when "score"
        error = @validateExprInternal(expr.input, _.extend({}, options, types: ["enum", "enumset"]))
        if error
          return error

        if expr.input
          enumValueIds = _.pluck(@exprUtils.getExprEnumValues(expr.input), "id")
        else
          enumValueIds = null

        for key, value of expr.scores
          if enumValueIds and key not in enumValueIds
            return "Invalid score enum"

          error = @validateExprInternal(value, _.extend({}, options, types: ["number"]))
          if error
            return error

      when "build enumset"
        for key, value of expr.values
          if options.enumValueIds and key not in options.enumValueIds
            return "Invalid score enum"

          error = @validateExprInternal(value, _.extend({}, options, types: ["boolean"]))
          if error
            return error

      when "variable"
        # Get variable
        variable = _.findWhere(@variables, id: expr.variableId)
        if not variable
          return "Missing variable #{expr.variableId}"

      when "spatial join"
        if not expr.toTable
          return "Missing to table"
        if not expr.fromGeometryExpr
          return "Missing from geometry"
        if not expr.toGeometryExpr
          return "Missing to geometry"
        if not expr.radius
          return "Radius required"

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


