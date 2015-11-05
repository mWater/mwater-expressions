_ = require 'lodash'

module.exports = class ExprUtils
  constructor: (schema) ->
    @schema = schema

    @opItems = []

    addOpItem = (op, name, resultType, exprTypes) =>
      @opItems.push(op: op, name: name, resultType: resultType, exprTypes: exprTypes)

    # TODO n?
    addOpItem("= any", "is", "boolean", ["text", "text[]"])
    addOpItem("= any", "is", "boolean", ["enum", "enum[]"])

    addOpItem("=", "is", "boolean", ["integer", "integer"])
    addOpItem("=", "is", "boolean", [["decimal", "integer"], ["decimal", "integer"]])
    addOpItem("=", "is", "boolean", ["text", "text"])
    addOpItem("=", "is", "boolean", ["enum", "enum"])
    addOpItem("=", "is", "boolean", ["date", "date"])
    addOpItem("=", "is", "boolean", ["datetime", "datetime"])
    addOpItem("=", "is", "boolean", ["boolean", "boolean"])

    addOpItem("and", "and", "boolean", ["boolean", "boolean"])
    addOpItem("or", "or", "boolean", ["boolean", "boolean"])

  # # Match an op optionally by op, 
  # doesOpMatch(

  findOpByResultType: (resultType, exprTypes...) ->
    op = _.find @opItems, (op) =>
      if op.resultType != resultType
        return false

      for exprType, i in exprTypes
        if exprType
          if _.isArray(op.exprTypes[i])
            if exprType not in op.exprTypes[i]
              return false
          else if exprType != op.exprTypes[i]
            return false
      return true

    if op
      return op.op

  # Determines if an set of joins contains a multiple
  isMultipleJoins: (table, joins) ->
    t = table
    for j in joins
      joinCol = @schema.getColumn(t, j)
      if joinCol.join.multiple
        return true

      t = joinCol.join.toTable

    return false

  # Get all comparison ops (id and name) for a given left hand side type
  getComparisonOps: (lhsType) ->
    ops = []
    switch lhsType
      when "integer", "decimal"
        ops.push({ id: "=", name: "equals" })
        ops.push({ id: ">", name: "is greater than" })
        ops.push({ id: ">=", name: "is greater or equal to" })
        ops.push({ id: "<", name: "is less than" })
        ops.push({ id: "<=", name: "is less than or equal to" })
      when "text"
        ops.push({ id: "= any", name: "is one of" })
        ops.push({ id: "=", name: "is" })
        ops.push({ id: "~*", name: "matches" })
      when "date", "datetime"
        ops.push({ id: "between", name: "between" })
        ops.push({ id: ">", name: "after" })
        ops.push({ id: "<", name: "before" })
      when "enum"
        ops.push({ id: "= any", name: "is one of" })
        ops.push({ id: "=", name: "is" })
      when "boolean"
        ops.push({ id: "= true", name: "is true"})
        ops.push({ id: "= false", name: "is false"})

    ops.push({ id: "is null", name: "has no value"})
    ops.push({ id: "is not null", name: "has a value"})

    return ops

  # Get the right hand side type for a comparison
  getComparisonRhsType: (lhsType, op) ->
    if op in ['= true', '= false', 'is null', 'is not null']
      return null

    if op in ['= any']
      if lhsType == "enum"
        return 'enum[]'
      else if lhsType == "text"
        return "text[]"
      else
        throw new Error("Invalid lhs type for op = any")

    if op == "between"
      if lhsType == "date"
        return 'daterange'
      if lhsType == "datetime"
        return 'datetimerange'
      else
        throw new Error("Invalid lhs type for op between")

    return lhsType

  # Return array of { id: <enum value>, name: <localized label of enum value> }
  getExprValues: (expr) ->
    if expr.type == "field"
      column = @schema.getColumn(expr.table, expr.column)
      return column.values
    if expr.type == "scalar"
      if expr.expr
        return @getExprValues(expr.expr)  

  # Gets the type of an expression
  getExprType: (expr) ->
    if not expr? or not expr.type
      return null

    switch expr.type
      when "field"
        column = @schema.getColumn(expr.table, expr.column)
        if column
          return column.type
        return null
      when "scalar"
        if expr.aggr
          aggr = _.findWhere(@getAggrs(expr.expr), id: expr.aggr)
          if not aggr
            throw new Error("Aggregation #{expr.aggr} not found for scalar")
          return aggr.type
        return @getExprType(expr.expr)
      when "op"
        # Get types of operand expressions
        exprTypes = _.map(expr.exprs, (e) => @getExprType(e))

        # Get possible ops
        opItems = _.filter @opItems, (opItem) =>
          # Must be correct op
          if opItem.op != expr.op
            return false

          # Must match all known expression types
          for exprType, i in exprTypes
            if exprType
              if _.isArray(opItem.exprTypes[i])
                if exprType not in opItem.exprTypes[i]
                  return false
              else if exprType != opItem.exprTypes[i]
                return false
          return true

        # Get unique resultTypes
        resultTypes = _.uniq(_.compact(_.pluck(opItems, "resultType")))
        if resultTypes.length == 1
          return resultTypes[0]

      when "literal"
        return expr.valueType
      when "count"
        return "count"
      else
        throw new Error("Not implemented for #{expr.type}")

  # Determines if an set of joins are valid
  areJoinsValid: (table, joins) ->
    t = table
    for j in joins
      joinCol = @schema.getColumn(t, j)
      if not joinCol 
        return false

      t = joinCol.join.toTable

    return true

  getAggrTypes: (expr) ->
    # Get available aggregations
    aggrs = @getAggrs(expr)

    # Keep unique types
    return _.uniq(_.pluck(aggrs, "type"))

  # Gets available aggregations [{id, name, type}]
  getAggrs: (expr) ->
    aggrs = []

    type = @getExprType(expr)

    # If null type, retun none
    if not type
      return []
    
    table = @schema.getTable(expr.table)
    if table.ordering and type != "count"
      aggrs.push({ id: "last", name: "Latest", type: type })

    switch type
      when "date", "datetime"
        aggrs.push({ id: "max", name: "Maximum", type: type })
        aggrs.push({ id: "min", name: "Minimum", type: type })

      when "integer", "decimal"
        aggrs.push({ id: "sum", name: "Total", type: type })
        aggrs.push({ id: "avg", name: "Average", type: "decimal" })
        aggrs.push({ id: "max", name: "Maximum", type: type })
        aggrs.push({ id: "min", name: "Minimum", type: type })

    # Count is always last option
    aggrs.push({ id: "count", name: "Number of", type: "integer" })

    return aggrs


  # Summarizes expression as text
  summarizeExpr: (expr) ->
    if not expr
      return "None"

    # # Check named expresions
    # namedExpr = _.find(@schema.getNamedExprs(expr.table), (ne) =>
    #   return @areExprsEqual(@simplifyExpr(ne.expr), @simplifyExpr(expr))
    # )

    # if namedExpr
    #   return namedExpr.name

    switch expr.type
      when "scalar"
        return @summarizeScalarExpr(expr)
      when "field"
        return @schema.getColumn(expr.table, expr.column).name
      when "count"
        return "Number of " + @schema.getTable(expr.table).name
      else
        throw new Error("Unsupported type #{expr.type}")

  summarizeScalarExpr: (expr) ->
    exprType = @getExprType(expr.expr)

    # Add aggr
    if expr.aggr 
      str = _.findWhere(@getAggrs(expr.expr), { id: expr.aggr }).name + " "
    else
      str = ""

    # Add joins
    t = expr.table
    for join in expr.joins
      joinCol = @schema.getColumn(t, join)
      str += joinCol.name + " > "
      t = joinCol.join.toTable

    # Special case for count of count type to be rendered Number of {last join name}
    if expr.aggr and exprType == "count"
      str = str.substring(0, str.length - 3)
    else
      str += @summarizeExpr(expr.expr)

    return str

  # Summarize an expression with optional aggregation
  # TODO Remove to AxisBuilder
  summarizeAggrExpr: (expr, aggr) ->
    exprType = @getExprType(expr)

    # Add aggr if not a count type
    if aggr and exprType != "count"
      aggrName = _.findWhere(@getAggrs(expr), { id: aggr }).name
      return aggrName + " " + @summarizeExpr(expr)
    else
      return @summarizeExpr(expr)
