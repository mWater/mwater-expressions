_ = require 'lodash'
moment = require 'moment'

module.exports = class ExprUtils
  constructor: (schema, variables = []) ->
    @schema = schema
    @variables = variables

  # Search can contain resultTypes, lhsExpr, op, aggr. lhsExpr is actual expression of lhs. resultTypes is optional array of result types
  # If search ordered is not true, excludes ordered ones
  # If prefix, only prefix
  # Results are array of opItems.
  findMatchingOpItems: (search) ->
    # Narrow list if op specified
    if search.op
      items = groupedOpItems[search.op] or []
    else
      items = opItems

    return _.filter items, (opItem) =>
      if search.resultTypes
        if opItem.resultType not in search.resultTypes
          return false

      if search.aggr? and opItem.aggr != search.aggr
        return false

      if search.ordered == false and opItem.ordered
        return false

      if search.prefix? and opItem.prefix != search.prefix
        return false

      # Handle list of specified types
      if search.lhsExpr
        lhsType = @getExprType(search.lhsExpr)
        if lhsType and opItem.exprTypes[0] != null and opItem.exprTypes[0] != lhsType and opItem.moreExprType != lhsType
          return false

      # Check lhsCond
      if search.lhsExpr and opItem.lhsCond and not opItem.lhsCond(search.lhsExpr, this)
        return false

      return true

  # Determine if op is aggregate
  @isOpAggr: (op) ->
    return aggrOpItems[op] or false

  # Determine if op is prefix
  @isOpPrefix: (op) ->
    return _.findWhere(opItems, op: op, prefix: true)?

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
      if not column
        return null

      # Prefer returning specified enumValues as expr might not cover all possibilities if it's an if/then, etc.        
      if column.enumValues
        return column.enumValues

      # DEPRECATED. Remove July 2017
      if column.type == "expr"
        return @getExprEnumValues(column.expr)

      return null

    if expr.type == "scalar"
      if expr.expr
        return @getExprEnumValues(expr.expr)  

    # "last", "last where" and "previous" are only ops to pass through enum values
    if expr.type == "op" and expr.op in ["last", "last where", "previous"] and expr.exprs[0]
      return @getExprEnumValues(expr.exprs[0])  

    # Weeks of month has predefined values (1-5 as text)
    if expr.type == "op" and expr.op == "weekofmonth"
      return [
        { id: "1", name: { en: "1" }}
        { id: "2", name: { en: "2" }}
        { id: "3", name: { en: "3" }}
        { id: "4", name: { en: "4" }}
        { id: "5", name: { en: "5" }}
      ]

    # Days of month has predefined values (01-31 as text)
    if expr.type == "op" and expr.op == "dayofmonth"
      return [
        { id: "01", name: { en: "01" }}
        { id: "02", name: { en: "02" }}
        { id: "03", name: { en: "03" }}
        { id: "04", name: { en: "04" }}
        { id: "05", name: { en: "05" }}
        { id: "06", name: { en: "06" }}
        { id: "07", name: { en: "07" }}
        { id: "08", name: { en: "08" }}
        { id: "09", name: { en: "09" }}
        { id: "10", name: { en: "10" }}
        { id: "11", name: { en: "11" }}
        { id: "12", name: { en: "12" }}
        { id: "13", name: { en: "13" }}
        { id: "14", name: { en: "14" }}
        { id: "15", name: { en: "15" }}
        { id: "16", name: { en: "16" }}
        { id: "17", name: { en: "17" }}
        { id: "18", name: { en: "18" }}
        { id: "19", name: { en: "19" }}
        { id: "20", name: { en: "20" }}
        { id: "21", name: { en: "21" }}
        { id: "22", name: { en: "22" }}
        { id: "23", name: { en: "23" }}
        { id: "24", name: { en: "24" }}
        { id: "25", name: { en: "25" }}
        { id: "26", name: { en: "26" }}
        { id: "27", name: { en: "27" }}
        { id: "28", name: { en: "28" }}
        { id: "29", name: { en: "29" }}
        { id: "30", name: { en: "30" }}
        { id: "31", name: { en: "31" }}
      ]

    # Month has predefined values
    if expr.type == "op" and expr.op == "month"
      return [
        { id: "01", name: { en: "January" } }
        { id: "02", name: { en: "February" } }
        { id: "03", name: { en: "March" } }
        { id: "04", name: { en: "April" } }
        { id: "05", name: { en: "May" } }
        { id: "06", name: { en: "June" } }
        { id: "07", name: { en: "July" } }
        { id: "08", name: { en: "August" } }
        { id: "09", name: { en: "September" } }
        { id: "10", name: { en: "October" } }
        { id: "11", name: { en: "November" } }
        { id: "12", name: { en: "December" } }
      ]

    # Case statements search for possible values
    if expr.type == "case"
      for cse in expr.cases
        enumValues = @getExprEnumValues(cse.then)
        if enumValues
          return enumValues
      return @getExprEnumValues(expr.else)

    if expr.type == "variable"
      return _.findWhere(@variables, id: expr.variableId)?.enumValues

  # gets the id table of an expression of type id
  getExprIdTable: (expr) ->
    if not expr
      return null

    if expr.type == "literal" and expr.valueType in ["id", "id[]"]
      return expr.idTable

    if expr.type == "id"
      return expr.table

    if expr.type == "scalar"
      return @getExprIdTable(expr.expr)

    # Handle fields
    if expr.type == "field"
      column = @schema.getColumn(expr.table, expr.column)

      if column?.type == "join"
        return column.join.toTable

      # DEPRECATED. Remove July 2017
      if column?.type == "expr"
        return @getExprIdTable(column.expr)

      if column?.type in ["id", "id[]"]
        return column.idTable

      return null
    
    if expr.type == "variable"
      return _.findWhere(@variables, id: expr.variableId)?.idTable

  # Gets the type of an expression
  getExprType: (expr) ->
    if not expr? or not expr.type
      return null

    switch expr.type
      when "field"
        column = @schema.getColumn(expr.table, expr.column)
        if column
          if column.type == "join"
            if column.join.type in ['1-1', 'n-1']
              return "id"
            else
              return "id[]"
          # DEPRECATED. Remove July 2017
          else if column.type == "expr"
            return @getExprType(column.expr)
          return column.type
        return null
      when "id"
        return "id"
      when "scalar"
        # Legacy support:
        if expr.aggr
          return @getExprType({ type: "op", op: expr.aggr, table: expr.table, exprs: [expr.expr] })

        return @getExprType(expr.expr)
      when "op"
        # Check for single-type ops
        matchingOpItems = @findMatchingOpItems(op: expr.op)
        resultTypes = _.uniq(_.compact(_.pluck(matchingOpItems, "resultType")))
        if resultTypes.length == 1
          return resultTypes[0]

        # Get possible ops
        matchingOpItems = @findMatchingOpItems(op: expr.op, lhsExpr: expr.exprs[0])

        # Get unique resultTypes
        resultTypes = _.uniq(_.compact(_.pluck(matchingOpItems, "resultType")))
        if resultTypes.length == 1
          return resultTypes[0]
        return null

      when "literal"
        return expr.valueType
      when "case"
        # Use type of first then that has value
        for cse in expr.cases
          type = @getExprType(cse.then)
          if type
            return type
        return @getExprType(expr.else)
      when "build enumset"
        return "enumset"
      when "score"
        return "number"
      when "count" # Deprecated
        return "count"
      when "variable"
        variable = _.findWhere(@variables, id: expr.variableId)
        if not variable
          return null
        return variable.type
      else
        throw new Error("Not implemented for #{expr.type}")

  # Determines the aggregation status of an expression. This is whether the expression is
  # aggregate (like sum, avg, etc) or individual (a regular field-containing expression) or 
  # literal (which is neither, just a number or text). 
  # Invisible second parameter is depth to prevent infinite recursion
  getExprAggrStatus: (expr) ->
    if not expr? or not expr.type
      return null

    depth = (arguments[1] or 0)
    if depth > 100
      throw new Error("Infinite recursion")

    # Gets the aggregation status of a series of expressions (takes highest always)
    getListAggrStatus = (exprs) =>
      # Get highest type
      aggrStatuses = _.map(exprs, (subExpr) => @getExprAggrStatus(subExpr, depth + 1))
      if "aggregate" in aggrStatuses
        return "aggregate"
      if "individual" in aggrStatuses
        return "individual"
      if "literal" in aggrStatuses
        return "literal"
      return null

    switch expr.type
      when "id", "scalar"
        return "individual"
      when "field"
        column = @schema.getColumn(expr.table, expr.column)
        if column?.expr
          return @getExprAggrStatus(column.expr, depth + 1)
        return "individual"
      when "op"
        # If aggregate op
        if ExprUtils.isOpAggr(expr.op)
          return "aggregate"

        return getListAggrStatus(expr.exprs)
      when "literal"
        return "literal"
      when "case"
        # Gather all exprs
        exprs = [expr.input, expr.else]
        exprs = exprs.concat(_.map(expr.cases, (cs) -> cs.when))
        exprs = exprs.concat(_.map(expr.cases, (cs) -> cs.then))
        return getListAggrStatus(exprs)
      when "score"
        return @getExprAggrStatus(expr.input, depth + 1)
      when "build enumset"
        # Gather all exprs
        exprs = _.values(expr.values)
        return getListAggrStatus(exprs)
      when "count", "comparison", "logical" # Deprecated
        return "individual"
      when "variable"
        variable = _.findWhere(@variables, id: expr.variableId)
        if not variable
          return "literal" # To prevent crash in cleaning, return something
        if variable.table
          return "individual"
        return "literal"
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

  # Gets the types that can be formed by aggregating an expression
  getAggrTypes: (expr) ->
    aggrOpItems = @findMatchingOpItems(lhsExpr: expr, aggr: true, ordered: @schema.getTable(expr.table)?.ordering?)
    return _.uniq(_.pluck(aggrOpItems, "resultType"))

  localizeString: (name, locale) ->
    return ExprUtils.localizeString(name, locale)

  # Localize a string that is { en: "english word", etc. }. Works with null and plain strings too.
  @localizeString: (name, locale) ->
    if not name
      return name

    # Simple string
    if typeof(name) == "string"
      return name

    if locale and name[locale]?
      return name[locale]

    if name._base and name[name._base]?
      return name[name._base]

    # Fall back to English
    if name.en?
      return name.en

    return null

  # Combine n expressions together by and
  @andExprs: (table, exprs...) ->
    exprs = _.map(exprs, (expr) -> if expr?.type == "op" and expr.op == "and" then expr.exprs else expr)
    exprs = _.compact(_.flatten(exprs))
    if exprs.length == 0
      return null
    if exprs.length == 1
      return exprs[0]

    return { type: "op", op: "and", table: table, exprs: exprs }

  # Summarizes expression as text
  summarizeExpr: (expr, locale) ->
    if not expr
      return "None" # TODO localize

    switch expr.type
      when "scalar"
        return @summarizeScalarExpr(expr, locale)
      when "field"
        return @localizeString(@schema.getColumn(expr.table, expr.column)?.name, locale)
      when "id"
        return @localizeString(@schema.getTable(expr.table)?.name, locale)
      when "op"
        # Special case for contains/intersects with literal RHS
        if expr.op == "contains" and expr.exprs[1]?.type == "literal" and expr.exprs[1]?.valueType == "enumset" 
          return @summarizeExpr(expr.exprs[0], locale) + " includes all of " + @stringifyLiteralValue("enumset", expr.exprs[1].value, locale, @getExprEnumValues(expr.exprs[0]))

        if expr.op == "intersects" and expr.exprs[1]?.type == "literal" and expr.exprs[1]?.valueType == "enumset" 
          return @summarizeExpr(expr.exprs[0], locale) + " includes any of " + @stringifyLiteralValue("enumset", expr.exprs[1].value, locale, @getExprEnumValues(expr.exprs[0]))

        # Special case for = any with literal RHS
        if expr.op == "= any" and expr.exprs[1]?.type == "literal" and expr.exprs[1]?.valueType == "enumset" 
          return @summarizeExpr(expr.exprs[0], locale) + " is any of " + @stringifyLiteralValue("enumset", expr.exprs[1].value, locale, @getExprEnumValues(expr.exprs[0]))

        # Special case for = with literal RHS
        if expr.op == "=" and expr.exprs[1]?.type == "literal" and expr.exprs[1]?.valueType == "enum" 
          return @summarizeExpr(expr.exprs[0], locale) + " is " + @stringifyLiteralValue("enum", expr.exprs[1].value, locale, @getExprEnumValues(expr.exprs[0]))

        # Special case for <> with literal RHS
        if expr.op == "<>" and expr.exprs[1]?.type == "literal" and expr.exprs[1]?.valueType == "enum" 
          return @summarizeExpr(expr.exprs[0], locale) + " is not " + @stringifyLiteralValue("enum", expr.exprs[1].value, locale, @getExprEnumValues(expr.exprs[0]))

        # Special case for count
        if expr.op == "count"
          return "Number of " + @localizeString(@schema.getTable(expr.table).name, locale)

        opItem = @findMatchingOpItems(op: expr.op)[0]
        if opItem
          if opItem.prefix
            return (opItem.prefixLabel or opItem.name) + " " + 
              _.map(expr.exprs, (e, index) => 
                # Only use rhs placeholder if > 0
                if index == 0 
                  return if e then @summarizeExpr(e, locale) else opItem.lhsPlaceholder or "None" 
                else 
                  return if e then @summarizeExpr(e, locale) else opItem.rhsPlaceholder or "None"
              ).join(if opItem.joiner then " #{opItem.joiner} " else ", ")

          if expr.exprs.length == 1
            return @summarizeExpr(expr.exprs[0], locale) + " " + opItem.name 

          return _.map(expr.exprs, (e) => @summarizeExpr(e, locale)).join(" " + opItem.name + " ")
        else
          return ""

      when "case"
        return @summarizeCaseExpr(expr, locale)
      when "literal"
        return expr.value + ""
      when "score"
        return "Score of " + @summarizeExpr(expr.input, locale)
      when "build enumset"
        return "Build Enumset"
      when "count"
        return "Count" # Deprecated
      when "variable"
        variable = _.findWhere(@variables, id: expr.variableId)
        return @localizeString(variable?.name, locale)
      else
        throw new Error("Unsupported type #{expr.type}")

  summarizeScalarExpr: (expr, locale) ->
    exprType = @getExprType(expr.expr)

    str = ""

    # Add joins
    t = expr.table
    for join in expr.joins
      joinCol = @schema.getColumn(t, join)
      if joinCol
        str += @localizeString(joinCol.name, locale) + " > "
      else
        str += "NOT FOUND > "
        break
      t = joinCol.join.toTable

    # Special case for id type to be rendered as {last join name}
    if expr.expr?.type == "id" and not expr.aggr
      str = str.substring(0, str.length - 3)
    else
      innerExpr = expr.expr

      # Handle legacy
      if expr.aggr    
        innerExpr = { type: "op", op: expr.aggr, table: expr.expr?.table, exprs: [expr.expr] }
    
      str += @summarizeExpr(innerExpr, locale)

    return str

  summarizeCaseExpr: (expr, locale) ->
    str = "If"
    for c in expr.cases
      str += " " + @summarizeExpr(c.when)
      str += " Then " + @summarizeExpr(c.then)

    if expr.else
      str += " Else " + @summarizeExpr(expr.else)

    return str

  # Converts a literal value related to an expression to a string, using name of enums. preferEnumCodes tries to use code over name
  stringifyExprLiteral: (expr, literal, locale, preferEnumCodes = false) ->
    return @stringifyLiteralValue(@getExprType(expr), literal, locale, @getExprEnumValues(expr), preferEnumCodes)

  # Stringify a literal value of a certain type
  # type is "text", "number", etc.
  # Does not have intelligence to properly handle type id and id[], so just puts in raw id
  stringifyLiteralValue: (type, value, locale, enumValues, preferEnumCodes = false) ->
    if not value?
      return "None" # TODO localize

    switch type
      when "text"
        return value
      when "number"
        return "" + value
      when "enum"
        # Get enumValues
        item = _.findWhere(enumValues, id: value)
        if item
          if preferEnumCodes and item.code
            return item.code
          return ExprUtils.localizeString(item.name, locale)
        return "???"
      when "enumset"
        return _.map(value, (val) =>
          item = _.findWhere(enumValues, id: val)
          if item
            if preferEnumCodes and item.code
              return item.code
            return ExprUtils.localizeString(item.name, locale)
          return "???"
        ).join(', ')

      when "text[]"
        # Parse if string
        if _.isString(value)
          value = JSON.parse(value or "[]")

        return value.join(', ')

      when "date"
        return moment(value, moment.ISO_8601).format("ll")

      when "datetime"
        return moment(value, moment.ISO_8601).format("lll")

    if value == true
      return "True"

    if value == false
      return "False"

    return "#{value}"

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

  # Get a list of fields that are referenced in a an expression
  # Useful to know which fields and joins are used. Includes joins as fields
  getReferencedFields: (expr) ->
    cols = []

    if not expr
      return cols

    switch expr.type
      when "field"
        cols.push(expr)
        column = @schema.getColumn(expr.table, expr.column)
        if column?.expr
          cols = cols.concat(@getReferencedFields(column.expr))
      when "op"
        for subexpr in expr.exprs
          cols = cols.concat(@getReferencedFields(subexpr))
      when "case"
        for subcase in expr.cases
          cols = cols.concat(@getReferencedFields(subcase.when))
          cols = cols.concat(@getReferencedFields(subcase.then))
        cols = cols.concat(@getReferencedFields(expr.else))
      when "scalar"
        table = expr.table
        for join in expr.joins
          cols.push({ type: "field", table: table, column: join })
          column = @schema.getColumn(table, join)
          # Handle gracefully
          if not column
            break

          table = column.join.toTable

        cols = cols.concat(@getReferencedFields(expr.expr))

      when "score"
        cols = cols.concat(@getReferencedFields(expr.input))
        for value in _.values(expr.scores)
          cols = cols.concat(@getReferencedFields(value))

      when "build enumset"
        for value in _.values(expr.values)
          cols = cols.concat(@getReferencedFields(value))

    return _.uniq(cols, (col) -> col.table + "/" + col.column)


  # Replace variables with literal values.
  inlineVariableValues: (expr, variableValues) =>
    # Replace every part of an object, including array members
    mapObject = (obj, replacer) ->
      if not obj  
        return obj
      if _.isArray(obj)
        return _.map(obj, replacer)
      if _.isObject(obj)
        return _.mapValues(obj, replacer)
      return obj

    replacer = (part) => 
      part = mapObject(part, replacer)
      if part and part.type == "variable"
        # Find variable
        variable = _.findWhere(@variables, id: part.variableId)
        if not variable
          throw new Error("Variable #{part.variableId} not found")
        if variable.table
          return variableValues[variable.id] or null
        if variableValues[variable.id]?
          return { type: "literal", valueType: variable.type, value: variableValues[variable.id] }
        else  
          return null
      return part

    return mapObject(expr, replacer)

  # # Get a list of column ids of expression table that are referenced in a an expression
  # # Useful to know which fields and joins are used. Does not follow joins, beyond including 
  # # the first join (which is a column in the start table).
  # # Function does not require a schema, so schema can be null/undefined in constructor
  # getImmediateReferencedColumns: (expr) ->
  #   cols = []

  #   if not expr
  #     return cols

  #   switch expr.type
  #     when "field"
  #       cols.push(expr.column)
  #     when "op"
  #       for subexpr in expr.exprs
  #         cols = cols.concat(@getImmediateReferencedColumns(subexpr))
  #     when "case"
  #       for subcase in expr.cases
  #         cols = cols.concat(@getImmediateReferencedColumns(subcase.when))
  #         cols = cols.concat(@getImmediateReferencedColumns(subcase.then))
  #       cols = cols.concat(@getImmediateReferencedColumns(expr.else))

  #   return _.uniq(cols)


# Setup op items
# opItems are a list of ops for various types:
# op: e.g. "="
# name: e.g. "is"
# resultType: resulting type from op. e.g. "boolean"
# exprTypes: array of types of expressions required for arguments
# moreExprType: type of n more expressions (like "and" that takes n arguments)
# prefix: true if name goes before LHS value
# prefixLabel: overrides name when displayed as prefix
# lhsCond: optional condition function on LHS expr that tests if applicable (for "within" which only applies to hierarchical tables)
# rhsLiteral: prefer rhs literal
# joiner: string to put between exprs when prefix type
# aggr: true if aggregating (e.g. sum)
# ordered: for aggr = true if table must be have ordering
# lhsPlaceholder: placeholder for lhs expression
# rhsPlaceholder: placeholder for rhs expression
opItems = []

# Which op items are aggregate (key = op, value = true)
aggrOpItems = {}

# opItems grouped by op
groupedOpItems = {}

# Adds an op item (particular combination of operands types with an operator)
# exprTypes is a list of types for expressions. moreExprType is the type of further N expressions, if allowed
addOpItem = (item) =>
  opItems.push(_.defaults(item, { prefix: false, rhsLiteral: true, aggr: false, ordered: false }))
  if item.aggr
    aggrOpItems[item.op] = true

  list = groupedOpItems[item.op] or []
  list.push(item)
  groupedOpItems[item.op] = list


# TODO n?
addOpItem(op: "= any", name: "is any of", resultType: "boolean", exprTypes: ["text", "text[]"])
addOpItem(op: "= any", name: "is any of", resultType: "boolean", exprTypes: ["enum", "enumset"])

addOpItem(op: "contains", name: "includes all of", resultType: "boolean", exprTypes: ["enumset", "enumset"])
addOpItem(op: "intersects", name: "includes any of", resultType: "boolean", exprTypes: ["enumset", "enumset"])

# Add relative dates
relativeDateOps = [
  ['thisyear', 'is this year']
  ['lastyear', 'is last year']
  ['thismonth', 'is this month']
  ['lastmonth', 'is last month']
  ['today', 'is today']
  ['yesterday', 'is yesterday']
  ['last24hours', 'is in last 24 hours']
  ['last7days', 'is in last 7 days']
  ['last30days', 'is in last 30 days']
  ['last365days', 'is in last 365 days']
  ['last3months', 'is in last 3 months']
  ['last6months', 'is in last 6 months']
  ['last12months', 'is in last 12 months']
  ['future', 'is in the future']
  ['notfuture', 'is not in the future']
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
addOpItem(op: ">=", name: "is greater than or equal to", resultType: "boolean", exprTypes: ["number", "number"])
addOpItem(op: "<=", name: "is less than or equal to", resultType: "boolean", exprTypes: ["number", "number"])

for type1 in ['date', 'datetime']
  for type2 in ['date', 'datetime']
    addOpItem(op: ">", name: "is after", resultType: "boolean", exprTypes: [type1, type2])
    addOpItem(op: "<", name: "is before", resultType: "boolean", exprTypes: [type1, type2])
    addOpItem(op: ">=", name: "is after or same as", resultType: "boolean", exprTypes: [type1, type2])
    addOpItem(op: "<=", name: "is before or same as", resultType: "boolean", exprTypes: [type1, type2])

addOpItem(op: "between", name: "is between", resultType: "boolean", exprTypes: ["number", "number", "number"])

addOpItem(op: "round", name: "Round", desc: "Round a number to closest whole number", resultType: "number", exprTypes: ["number"], prefix: true)
addOpItem(op: "floor", name: "Floor", desc: "Round a number down", resultType: "number", exprTypes: ["number"], prefix: true)
addOpItem(op: "ceiling", name: "Ceiling", desc: "Round a number up", resultType: "number", exprTypes: ["number"], prefix: true)
addOpItem(op: "latitude", name: "Latitude of", desc: "Get latitude in degrees of a location", resultType: "number", exprTypes: ["geometry"], prefix: true)
addOpItem(op: "longitude", name: "Longitude of", desc: "Get longitude in degrees of a location", resultType: "number", exprTypes: ["geometry"], prefix: true)
addOpItem(op: "distance", name: "Distance between", desc: "Get distance in meters between two locations", resultType: "number", exprTypes: ["geometry", "geometry"], prefix: true, rhsLiteral: false, joiner: "and")

# And/or is a list of booleans
addOpItem(op: "and", name: "and", resultType: "boolean", exprTypes: [], moreExprType: "boolean")
addOpItem(op: "or", name: "or", resultType: "boolean", exprTypes: [], moreExprType: "boolean")

for op in ['+', '*']
  addOpItem(op: op, name: op, resultType: "number", exprTypes: [], moreExprType: "number")

addOpItem(op: "-", name: "-", resultType: "number", exprTypes: ["number", "number"])
addOpItem(op: "/", name: "/", resultType: "number", exprTypes: ["number", "number"])

# Date subtraction
addOpItem(op: "days difference", name: "Days between", desc: "Get the number of days between two dates", resultType: "number", exprTypes: ["date", "date"], prefix: true, rhsLiteral: false, joiner: "and")
addOpItem(op: "days difference", name: "Days between", desc: "Get the number of days between two dates", resultType: "number", exprTypes: ["datetime", "datetime"], prefix: true, rhsLiteral: false, joiner: "and")
addOpItem(op: "days difference", name: "Days between", desc: "Get the number of days between two dates", resultType: "number", exprTypes: ["date", "datetime"], prefix: true, rhsLiteral: false, joiner: "and")
addOpItem(op: "days difference", name: "Days between", desc: "Get the number of days between two dates", resultType: "number", exprTypes: ["datetime", "date"], prefix: true, rhsLiteral: false, joiner: "and")

addOpItem(op: "months difference", name: "Months between", desc: "Get the number of months between two dates", resultType: "number", exprTypes: ["date", "date"], prefix: true, rhsLiteral: false, joiner: "and")
addOpItem(op: "months difference", name: "Months between", desc: "Get the number of months between two dates", resultType: "number", exprTypes: ["datetime", "datetime"], prefix: true, rhsLiteral: false, joiner: "and")
addOpItem(op: "months difference", name: "Months between", desc: "Get the number of months between two dates", resultType: "number", exprTypes: ["date", "datetime"], prefix: true, rhsLiteral: false, joiner: "and")
addOpItem(op: "months difference", name: "Months between", desc: "Get the number of months between two dates", resultType: "number", exprTypes: ["datetime", "date"], prefix: true, rhsLiteral: false, joiner: "and")

addOpItem(op: "years difference", name: "Years between", desc: "Get the number of years between two dates", resultType: "number", exprTypes: ["date", "date"], prefix: true, rhsLiteral: false, joiner: "and")
addOpItem(op: "years difference", name: "Years between", desc: "Get the number of years between two dates", resultType: "number", exprTypes: ["datetime", "datetime"], prefix: true, rhsLiteral: false, joiner: "and")
addOpItem(op: "years difference", name: "Years between", desc: "Get the number of years between two dates", resultType: "number", exprTypes: ["date", "datetime"], prefix: true, rhsLiteral: false, joiner: "and")
addOpItem(op: "years difference", name: "Years between", desc: "Get the number of years between two dates", resultType: "number", exprTypes: ["datetime", "date"], prefix: true, rhsLiteral: false, joiner: "and")

addOpItem(op: "days since", name: "Days since", desc: "Get number of days from a date to the present", resultType: "number", exprTypes: ["date"], prefix: true, rhsLiteral: false)
addOpItem(op: "days since", name: "Days since", desc: "Get number of days from a date to the present", resultType: "number", exprTypes: ["datetime"], prefix: true, rhsLiteral: false)

addOpItem(op: "month", name: "Month", desc: "Month of year", resultType: "enum", exprTypes: ["date"], prefix: true, rhsLiteral: false)
addOpItem(op: "month", name: "Month", desc: "Month of year", resultType: "enum", exprTypes: ["datetime"], prefix: true, rhsLiteral: false)

addOpItem(op: "yearmonth", name: "Year and Month", desc: "Date of start of month", resultType: "date", exprTypes: ["date"], prefix: true, rhsLiteral: false)
addOpItem(op: "yearmonth", name: "Year and Month", desc: "Date of start of month", resultType: "date", exprTypes: ["datetime"], prefix: true, rhsLiteral: false)

addOpItem(op: "year", name: "Year", desc: "Date of start of year", resultType: "date", exprTypes: ["date"], prefix: true, rhsLiteral: false)
addOpItem(op: "year", name: "Year", desc: "Date of start of year", resultType: "date", exprTypes: ["datetime"], prefix: true, rhsLiteral: false)

addOpItem(op: "weekofmonth", name: "Week of month", desc: "Week within the month", resultType: "enum", exprTypes: ["date"], prefix: true, rhsLiteral: false)
addOpItem(op: "weekofmonth", name: "Week of month", desc: "Week within the month", resultType: "enum", exprTypes: ["datetime"], prefix: true, rhsLiteral: false)

addOpItem(op: "dayofmonth", name: "Day of month", desc: "Day within the month (1-31)", resultType: "enum", exprTypes: ["date"], prefix: true, rhsLiteral: false)
addOpItem(op: "dayofmonth", name: "Day of month", desc: "Day within the month (1-31)", resultType: "enum", exprTypes: ["datetime"], prefix: true, rhsLiteral: false)

addOpItem(op: "within", name: "is within", resultType: "boolean", exprTypes: ["id", "id"], lhsCond: (lhsExpr, exprUtils) => 
  lhsIdTable = exprUtils.getExprIdTable(lhsExpr)
  if lhsIdTable
    return exprUtils.schema.getTable(lhsIdTable).ancestry? or exprUtils.schema.getTable(lhsIdTable).ancestryTable?
  return false
)

addOpItem(op: "=", name: "is", resultType: "boolean", exprTypes: ["id", "id"])
addOpItem(op: "<>", name: "is not", resultType: "boolean", exprTypes: ["id", "id"])
addOpItem(op: "= any", name: "is any of", resultType: "boolean", exprTypes: ["id", "id[]"])

for type in ['text', 'number', 'enum', 'enumset', 'boolean', 'date', 'datetime', 'geometry', 'id']
  addOpItem(op: "last", name: "Latest", desc: "Get latest value when there are multiple", resultType: type, exprTypes: [type], prefix: true, aggr: true, ordered: true)
  addOpItem(op: "last where", name: "Latest where", desc: "Get latest value that matches a condition", resultType: type, exprTypes: [type, "boolean"], prefix: true, prefixLabel: "Latest", aggr: true, ordered: true, rhsLiteral: false, joiner: "where", rhsPlaceholder: "All")
  addOpItem(op: "previous", name: "Previous", desc: "Get 2nd latest value when there are multiple", resultType: type, exprTypes: [type], prefix: true, aggr: true, ordered: true)

addOpItem(op: "sum", name: "Total", desc: "Add all values together", resultType: "number", exprTypes: ["number"], prefix: true, aggr: true)
addOpItem(op: "avg", name: "Average", desc: "Average all values together", resultType: "number", exprTypes: ["number"], prefix: true, aggr: true)

for type in ['number', 'date', 'datetime']
  addOpItem(op: "min", name: "Minimum", desc: "Get smallest value", resultType: type, exprTypes: [type], prefix: true, aggr: true)
  addOpItem(op: "min where", name: "Minimum where", desc: "Get smallest value that matches a condition", resultType: type, exprTypes: [type, "boolean"], prefix: true, prefixLabel: "Minimum", aggr: true, rhsLiteral: false, joiner: "of", rhsPlaceholder: "All")

  addOpItem(op: "max", name: "Maximum", desc: "Get largest value", resultType: type, exprTypes: [type], prefix: true, aggr: true)
  addOpItem(op: "max where", name: "Maximum where", desc: "Get largest value that matches a condition", resultType: type, exprTypes: [type, "boolean"], prefix: true, prefixLabel: "Maximum", aggr: true, rhsLiteral: false, joiner: "of", rhsPlaceholder: "All")

addOpItem(op: "percent where", name: "Percent where", desc: "Get percent of items that match a condition", resultType: "number", exprTypes: ["boolean", "boolean"], prefix: true, aggr: true, rhsLiteral: false, joiner: "of", rhsPlaceholder: "All")
addOpItem(op: "count where", name: "Number where", desc: "Get number of items that match a condition", resultType: "number", exprTypes: ["boolean"], prefix: true, aggr: true)
addOpItem(op: "sum where", name: "Total where", desc: "Add together only values that match a condition", resultType: "number", exprTypes: ["number", "boolean"], prefix: true, prefixLabel: "Total", aggr: true, rhsLiteral: false, joiner: "where", rhsPlaceholder: "All")

addOpItem(op: "within any", name: "is within any of", resultType: "boolean", exprTypes: ["id", "id[]"], lhsCond: (lhsExpr, exprUtils) => 
  lhsIdTable = exprUtils.getExprIdTable(lhsExpr)
  if lhsIdTable
    return exprUtils.schema.getTable(lhsIdTable).ancestry? or exprUtils.schema.getTable(lhsIdTable).ancestryTable?
  return false
)

addOpItem(op: "array_agg", name: "Make list of", desc: "Aggregates results into a list", resultType: "text[]", exprTypes: ["text"], prefix: true, aggr: true)

addOpItem(op: "contains", name: "includes all of", resultType: "boolean", exprTypes: ["id[]", "id[]"])

addOpItem(op: "count", name: "Total Number", desc: "Get total number of items", resultType: "number", exprTypes: [], prefix: true, aggr: true)
addOpItem(op: "percent", name: "Percent of Total", desc: "Percent of all items", resultType: "number", exprTypes: [], prefix: true, aggr: true)

addOpItem(op: "~*", name: "matches", resultType: "boolean", exprTypes: ["text", "text"])
addOpItem(op: "not", name: "Not", desc: "Opposite of a value", resultType: "boolean", exprTypes: ["boolean"], prefix: true)
for type in ['text', 'number', 'enum', 'enumset', 'boolean', 'date', 'datetime', 'geometry', 'image', 'imagelist', 'id', 'json']
  addOpItem(op: "is null", name: "is blank", resultType: "boolean", exprTypes: [type])
  addOpItem(op: "is not null", name: "is not blank", resultType: "boolean", exprTypes: [type])

for type in ['id', 'text', 'date']
  addOpItem(op: "count distinct", name: "Number of unique", desc: "Count number of unique values", resultType: "number", exprTypes: [type], prefix: true, aggr: true)

addOpItem(op: "length", name: "Number of values in", desc: "Advanced: number of values selected in a multi-choice field", resultType: "number", exprTypes: ["enumset"], prefix: true)
addOpItem(op: "length", name: "Number of values in", desc: "Advanced: number of images present", resultType: "number", exprTypes: ["imagelist"], prefix: true)
addOpItem(op: "length", name: "Number of values in", desc: "Advanced: number of items present in a text list", resultType: "number", exprTypes: ["text[]"], prefix: true)

addOpItem(op: "line length", name: "Length of line", desc: "Length of a line shape in meters", resultType: "number", exprTypes: ["geometry"], prefix: true)

for type in ['id']
  addOpItem(op: "is latest", name: "Is latest for each", desc: "Only include latest item for each of something", resultType: "boolean", exprTypes: [type, "boolean"], prefix: true, ordered: true, aggr: false, rhsLiteral: false, joiner: "where", rhsPlaceholder: "All")

addOpItem(op: "current date", name: "Today", desc: "Advanced: current date. Do not use in comparisons", resultType: "date", exprTypes: [], prefix: true)
addOpItem(op: "current datetime", name: "Now", desc: "Advanced: current datetime. Do not use in comparisons", resultType: "datetime", exprTypes: [], prefix: true)

addOpItem(op: "to text", name: "Convert to text", desc: "Advanced: convert a choice or number type to a text value", resultType: "text", exprTypes: ["enum"], prefix: true)
addOpItem(op: "to text", name: "Convert to text", desc: "Advanced: convert a choice or number type to a text value", resultType: "text", exprTypes: ["number"], prefix: true)

