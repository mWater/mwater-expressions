_ = require 'lodash'

module.exports = class ExprUtils
  constructor: (schema) ->
    @schema = schema

    @opItems = []

    # Adds an op item (particular combination of operands types with an operator)
    # exprTypes is a list of types for expressions. moreExprType is the type of further N expressions, if allowed
    addOpItem = (op, name, resultType, exprTypes, moreExprType) =>
      @opItems.push(op: op, name: name, resultType: resultType, exprTypes: exprTypes, moreExprType: moreExprType)

    # TODO n?
    addOpItem("= any", "is any of", "boolean", ["text", "text[]"])
    addOpItem("= any", "is any of", "boolean", ["enum", "enumset"])

    addOpItem("contains", "includes all of", "boolean", ["enumset", "enumset"])
    # addOpItem("intersects", "includes any of", "boolean", ["enumset", "enumset"]) Painful to implement...

    # Add relative dates
    relativeDateOps = [
      ['thisyear', 'is this year']
      ['lastyear', 'is last year']
      ['thismonth', 'is this month']
      ['lastmonth', 'is last month']
      ['today', 'is today']
      ['yesterday', 'is yesterday']
      ['last7days', 'is in last 7 days']
      ['last30days', 'is in last 30 days']
      ['last365days', 'is in last 365 days']
    ]
    for relativeDateOp in relativeDateOps
      addOpItem(relativeDateOp[0], relativeDateOp[1], "boolean", ['date'])
      addOpItem(relativeDateOp[0], relativeDateOp[1], "boolean", ['datetime'])

    # Add in ranges
    addOpItem("between", "is between", "boolean", ["date", "date", "date"])
    addOpItem("between", "is between", "boolean", ["datetime", "datetime", "datetime"])

    addOpItem("=", "is", "boolean", ["number", "number"])
    addOpItem("=", "is", "boolean", ["text", "text"])
    addOpItem("=", "is", "boolean", ["enum", "enum"])
    addOpItem("=", "is", "boolean", ["date", "date"])
    addOpItem("=", "is", "boolean", ["datetime", "datetime"])
    addOpItem("=", "is", "boolean", ["boolean", "boolean"])

    addOpItem("<>", "is not", "boolean", ["text", "text"])
    addOpItem("<>", "is not", "boolean", ["enum", "enum"])
    addOpItem("<>", "is not", "boolean", ["date", "date"])
    addOpItem("<>", "is not", "boolean", ["datetime", "datetime"])
    addOpItem("<>", "is not", "boolean", ["boolean", "boolean"])

    addOpItem("<>", "is not", "boolean", ["number", "number"])
    addOpItem(">", "is greater than", "boolean", ["number", "number"])
    addOpItem("<", "is less than", "boolean", ["number", "number"])
    addOpItem(">=", "is greater or equal to", "boolean", ["number", "number"])
    addOpItem("<=", "is less or equal to", "boolean", ["number", "number"])

    addOpItem("between", "is between", "boolean", ["number", "number", "number"])

    # And/or is a list of booleans
    addOpItem("and", "and", "boolean", [], "boolean")
    addOpItem("or", "or", "boolean", [], "boolean")

    for op in ['+', '*']
      addOpItem(op, op, "number", [], "number")

    addOpItem("-", "-", "number", ["number", "number"])
    addOpItem("/", "/", "number", ["number", "number"])

    addOpItem("~*", "matches", "boolean", ["text", "text"])
    addOpItem("not", "is false", "boolean", ["boolean"])
    addOpItem("is null", "is blank", "boolean", [null])
    addOpItem("is not null", "is not blank", "boolean", [null])

  # Search can contain resultType, exprTypes and op. resultType can be an array of options
  # Results are array of { name:, op:, resultType:, exprTypes: [array of exprTypes], moreExprType: }
  findMatchingOpItems: (search) ->
    return _.filter @opItems, (opItem) =>
      if search.resultType 
        if _.isArray(search.resultType)
          if opItem.resultType not in search.resultType
            return false
        else if opItem.resultType != search.resultType
          return false

      if search.op and opItem.op != search.op
        return false

      # Handle list of specified types
      if search.exprTypes
        for exprType, i in search.exprTypes
          if i < opItem.exprTypes.length
            if exprType and opItem.exprTypes[i] and exprType != opItem.exprTypes[i]
              return false
          else if opItem.moreExprType
            if exprType and exprType != opItem.moreExprType
              return false

      return true

  # Follows a list of joins to determine final table
  followJoins: (startTable, joins) ->
    t = startTable
    for j in joins
      joinCol = @schema.getColumn(t, j)
      t = joinCol.join.toTable

    return t

  # Determines if an set of joins contains a multiple
  isMultipleJoins: (table, joins) ->
    t = table
    for j in joins
      joinCol = @schema.getColumn(t, j)
      if joinCol.join.type in ['1-n', 'n-n']
        return true

      t = joinCol.join.toTable

    return false

  # Return array of { id: <enum value>, name: <localized label of enum value> }
  getExprEnumValues: (expr) ->
    if not expr 
      return
    if expr.type == "field"
      column = @schema.getColumn(expr.table, expr.column)
      return column.enumValues
    if expr.type == "scalar"
      if expr.expr
        return @getExprEnumValues(expr.expr)  

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
      when "id"
        return "id"
      when "scalar"
        if expr.aggr
          aggr = _.findWhere(@getAggrs(expr.expr), id: expr.aggr)
          if not aggr
            # Type is unknown as a result
            return null
          return aggr.type
        return @getExprType(expr.expr)
      when "op"
        # Check for single-type ops
        opItems = @findMatchingOpItems(op: expr.op)
        resultTypes = _.uniq(_.compact(_.pluck(opItems, "resultType")))
        if resultTypes.length == 1
          return resultTypes[0]

        # Get types of operand expressions
        exprTypes = _.map(expr.exprs, (e) => @getExprType(e))

        # Get possible ops
        opItems = @findMatchingOpItems(op: expr.op, exprTypes: exprTypes)

        # Get unique resultTypes
        resultTypes = _.uniq(_.compact(_.pluck(opItems, "resultType")))
        if resultTypes.length == 1
          return resultTypes[0]
        return null

      when "literal"
        return expr.valueType
      when "case"
        # Use type of first then
        if expr.cases[0]
          return @getExprType(expr.cases[0].then)
        return @getExprType(expr.else)

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

  # Gets the expression table
  getExprTable: (expr) ->
    if not expr
      return null

    return expr.table

  getAggrTypes: (expr) ->
    # Get available aggregations
    aggrs = @getAggrs(expr)

    # Keep unique types
    return _.uniq(_.pluck(aggrs, "type"))

  # Gets available aggregations [{id, name, type}]
  getAggrs: (expr) ->
    aggrs = []

    type = @getExprType(expr)

    # If null type, return none
    if not type
      return []
    
    table = @schema.getTable(expr.table)
    if table.ordering and type not in ["id", "count"] # count is legacy. TODO remove
      aggrs.push({ id: "last", name: "Latest", type: type })

    switch type
      when "date", "datetime"
        aggrs.push({ id: "max", name: "Maximum", type: type })
        aggrs.push({ id: "min", name: "Minimum", type: type })

      when "number", "integer", "decimal" # integer and decimal are legacy. TODO remove
        aggrs.push({ id: "sum", name: "Total", type: type })
        aggrs.push({ id: "avg", name: "Average", type: type })
        aggrs.push({ id: "max", name: "Maximum", type: type })
        aggrs.push({ id: "min", name: "Minimum", type: type })

      when "id", "count" # count is legacy. TODO remove
        aggrs.push({ id: "count", name: "Number of", type: "number" })

    return aggrs

  localizeString: (name, locale) ->
    return ExprUtils.localizeString(name, locale)

  # Localize a string that is { en: "english word", etc. }. Works with null and plain strings too.
  @localizeString: (name, locale) ->
    if not name
      return name

    # Simple string
    if typeof(name) == "string"
      return name

    if name[locale or "en"]
      return name[locale or "en"]

    if name._base and name[name._base]
      return name[name._base]

    return null

  # Summarizes expression as text
  summarizeExpr: (expr, locale) ->
    if not expr
      return "None" # TODO localize

    # # Check named expresions
    # namedExpr = _.find(@schema.getNamedExprs(expr.table), (ne) =>
    #   return @areExprsEqual(@simplifyExpr(ne.expr), @simplifyExpr(expr))
    # )

    # if namedExpr
    #   return namedExpr.name

    switch expr.type
      when "scalar"
        return @summarizeScalarExpr(expr, locale)
      when "field"
        return @localizeString(@schema.getColumn(expr.table, expr.column).name, locale)
      when "id"
        return @localizeString(@schema.getTable(expr.table).name, locale)
      when "op"
        return _.map(expr.exprs, (e) => @summarizeExpr(e, locale)).join(" " + expr.op + " ")
      when "literal"
        return expr.value + ""
      else
        throw new Error("Unsupported type #{expr.type}")

  summarizeScalarExpr: (expr, locale) ->
    exprType = @getExprType(expr.expr)

    # Add aggr
    if expr.aggr 
      str = _.findWhere(@getAggrs(expr.expr), { id: expr.aggr }).name + " " # TODO localize
    else
      str = ""

    # Add joins
    t = expr.table
    for join in expr.joins
      joinCol = @schema.getColumn(t, join)
      str += @localizeString(joinCol.name, locale) + " > "
      t = joinCol.join.toTable

    # Special case for id type to be rendered as {last join name}
    if exprType == "id"
      str = str.substring(0, str.length - 3)
    else
      str += @summarizeExpr(expr.expr, locale)

    return str

  # Summarize an expression with optional aggregation
  # TODO Remove to AxisBuilder
  summarizeAggrExpr: (expr, aggr, locale) ->
    exprType = @getExprType(expr)

    # Add aggr
    if aggr 
      aggrName = _.findWhere(@getAggrs(expr), { id: aggr }).name
      return aggrName + " " + @summarizeExpr(expr, locale)
    else
      return @summarizeExpr(expr, locale)

  # Converts all literals to string, using name of enums
  stringifyExprLiteral: (expr, literal, locale) ->
    if not literal?
      return "None" # TODO localize

    type = @getExprType(expr)
    if type == 'enum'
      enumValues = @getExprEnumValues(expr)
      if enumValues
        item = _.findWhere(enumValues, id: literal)
        if item
          return @localizeString(item.name, locale)
        return "???"

    # Map enumset to A,B...
    if type == "enumset" and _.isArray(literal)
      enumValues = @getExprEnumValues(expr)
      if enumValues
        return _.map(literal, (val) =>
          item = _.findWhere(enumValues, id: val)
          if item
            return @localizeString(item.name, locale)
          return "???"
        ).join(',')

    if literal == true
      return "True"
    if literal == false
      return "False"

    return "#{literal}"

  # Get all comparison ops (id and name) for a given left hand side type DEPRECATED
  getComparisonOps: (lhsType) ->
    ops = []
    switch lhsType
      when "number"
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

  # Get the right hand side type for a comparison DEPRECATED
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
