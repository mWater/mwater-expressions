_ = require 'lodash'

module.exports = class ExprUtils
  constructor: (schema) ->
    @schema = schema

    # opItems are a list of ops for various types:
    # op: e.g. "="
    # name: e.g. "is"
    # resultType: resulting type from op. e.g. "boolean"
    # exprTypes: array of types of expressions required for arguments
    # moreExprType: type of n more expressions (like "and" that takes n arguments)
    # prefix: true if name goes before LHS value
    # lhsCond: optional condition function on LHS expr that tests if applicable (for "within" which only applies to hierarchical tables)
    # rhsLiteral: prefer rhs literal
    # joiner: string to put between exprs when prefix type
    @opItems = []

    # Adds an op item (particular combination of operands types with an operator)
    # exprTypes is a list of types for expressions. moreExprType is the type of further N expressions, if allowed
    addOpItem = (item) =>
      @opItems.push(_.defaults(item, { prefix: false, rhsLiteral: true }))

    # TODO n?
    addOpItem(op: "= any", name: "is any of", resultType: "boolean", exprTypes: ["text", "text[]"])
    addOpItem(op: "= any", name: "is any of", resultType: "boolean", exprTypes: ["enum", "enumset"])

    addOpItem(op: "contains", name: "includes all of", resultType: "boolean", exprTypes: ["enumset", "enumset"])
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
      addOpItem(op: relativeDateOp[0], name: relativeDateOp[1], resultType: "boolean", exprTypes: ['date'])
      addOpItem(op: relativeDateOp[0], name: relativeDateOp[1], resultType: "boolean", exprTypes: ['datetime'])

    # Add in ranges
    addOpItem(op: "between", name: "is between", resultType: "boolean", exprTypes: ["date", "date", "date"])
    addOpItem(op: "between", name: "is between", resultType: "boolean", exprTypes: ["datetime", "datetime", "datetime"])

    addOpItem(op: "=", name: "is", resultType: "boolean", exprTypes: ["number", "number"])
    addOpItem(op: "=", name: "is", resultType: "boolean", exprTypes: ["text", "text"])
    addOpItem(op: "=", name: "is", resultType: "boolean", exprTypes: ["enum", "enum"])
    addOpItem(op: "=", name: "is", resultType: "boolean", exprTypes: ["date", "date"])
    addOpItem(op: "=", name: "is", resultType: "boolean", exprTypes: ["datetime", "datetime"])
    addOpItem(op: "=", name: "is", resultType: "boolean", exprTypes: ["boolean", "boolean"])

    addOpItem(op: "<>", name: "is not", resultType: "boolean", exprTypes: ["text", "text"])
    addOpItem(op: "<>", name: "is not", resultType: "boolean", exprTypes: ["enum", "enum"])
    addOpItem(op: "<>", name: "is not", resultType: "boolean", exprTypes: ["date", "date"])
    addOpItem(op: "<>", name: "is not", resultType: "boolean", exprTypes: ["datetime", "datetime"])
    addOpItem(op: "<>", name: "is not", resultType: "boolean", exprTypes: ["boolean", "boolean"])

    addOpItem(op: "<>", name: "is not", resultType: "boolean", exprTypes: ["number", "number"])
    addOpItem(op: ">", name: "is greater than", resultType: "boolean", exprTypes: ["number", "number"])
    addOpItem(op: "<", name: "is less than", resultType: "boolean", exprTypes: ["number", "number"])
    addOpItem(op: ">=", name: "is greater or equal to", resultType: "boolean", exprTypes: ["number", "number"])
    addOpItem(op: "<=", name: "is less or equal to", resultType: "boolean", exprTypes: ["number", "number"])

    addOpItem(op: "between", name: "is between", resultType: "boolean", exprTypes: ["number", "number", "number"])

    addOpItem(op: "round", name: "round", resultType: "number", exprTypes: ["number"], prefix: true)
    addOpItem(op: "floor", name: "floor", resultType: "number", exprTypes: ["number"], prefix: true)
    addOpItem(op: "ceiling", name: "ceiling", resultType: "number", exprTypes: ["number"], prefix: true)
    addOpItem(op: "latitude", name: "latitude of", resultType: "number", exprTypes: ["geometry"], prefix: true)
    addOpItem(op: "longitude", name: "longitude of", resultType: "number", exprTypes: ["geometry"], prefix: true)
    addOpItem(op: "distance", name: "distance between", resultType: "number", exprTypes: ["geometry", "geometry"], prefix: true, rhsLiteral: false, joiner: "and")

    # And/or is a list of booleans
    addOpItem(op: "and", name: "and", resultType: "boolean", exprTypes: [], moreExprType: "boolean")
    addOpItem(op: "or", name: "or", resultType: "boolean", exprTypes: [], moreExprType: "boolean")

    for op in ['+', '*']
      addOpItem(op: op, name: op, resultType: "number", exprTypes: [], moreExprType: "number")

    addOpItem(op: "-", name: "-", resultType: "number", exprTypes: ["number", "number"])
    addOpItem(op: "/", name: "/", resultType: "number", exprTypes: ["number", "number"])

    addOpItem(op: "within", name: "in", resultType: "boolean", exprTypes: ["id", "id"], lhsCond: (lhsExpr) => 
      lhsIdTable = @getExprIdTable(lhsExpr)
      if lhsIdTable
        return @schema.getTable(lhsIdTable).ancestry?
      return false
    )
    addOpItem(op: "=", name: "is", resultType: "boolean", exprTypes: ["id", "id"])

    addOpItem(op: "~*", name: "matches", resultType: "boolean", exprTypes: ["text", "text"])
    addOpItem(op: "not", name: "is false", resultType: "boolean", exprTypes: ["boolean"])
    addOpItem(op: "is null", name: "is blank", resultType: "boolean", exprTypes: [null])
    addOpItem(op: "is not null", name: "is not blank", resultType: "boolean", exprTypes: [null])


  # Search can contain resultTypes, lhsExpr, op. lhsExpr is actual expression of lhs. resultTypes is optional array of result types
  # Results are array of opItems.
  findMatchingOpItems: (search) ->
    return _.filter @opItems, (opItem) =>
      if search.resultTypes
        if opItem.resultType not in search.resultTypes
          return false

      if search.op and opItem.op != search.op
        return false

      # Handle list of specified types
      if search.lhsExpr
        lhsType = @getExprType(search.lhsExpr)
        if opItem.exprTypes[0] != null and opItem.exprTypes[0] != lhsType 
          return false

      # Check lhsCond
      if search.lhsExpr and opItem.lhsCond and not opItem.lhsCond(search.lhsExpr)
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

  # gets the id table of an expression of type id
  getExprIdTable: (expr) ->
    if not expr
      return null

    if expr.type == "literal" and expr.valueType == "id"
      return expr.idTable

    if expr.type == "id"
      return expr.table

    if expr.type == "scalar"
      return @getExprIdTable(expr.expr)

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
      when "score"
        return "number"
      when "count" # Deprecated
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
      when "case"
        return @summarizeCaseExpr(expr, locale)
      when "literal"
        return expr.value + ""
      when "score"
        return "Score of " + @summarizeExpr(expr.input, locale)
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

  summarizeCaseExpr: (expr, locale) ->
    str = "If"
    for c in expr.cases
      str += " " + @summarizeExpr(c.when)
      str += " Then " + @summarizeExpr(c.then)

    if expr.else
      str += " Else " + @summarizeExpr(expr.else)

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
        ).join(', ')

    # Text array
    if type == "text[]" and _.isArray(literal)
      return literal.join(', ')

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

  # Get a list of column ids of expression table that are referenced in a an expression
  # Useful to know which fields and joins are used. Does not follow joins, beyond including 
  # the first join (which is a column in the start table).
  # Function does not require a schema, so schema can be null/undefined in constructor
  getImmediateReferencedColumns: (expr) ->
    cols = []

    if not expr
      return cols

    switch expr.type
      when "field"
        cols.push(expr.column)
      when "op"
        for subexpr in expr.exprs
          cols = cols.concat(@getImmediateReferencedColumns(subexpr))
      when "case"
        for subcase in expr.cases
          cols = cols.concat(@getImmediateReferencedColumns(subcase.when))
          cols = cols.concat(@getImmediateReferencedColumns(subcase.then))
        cols = cols.concat(@getImmediateReferencedColumns(expr.else))

    return _.uniq(cols)
