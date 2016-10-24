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
  #   types: optional types to limit to
  #   enumValueIds: ids of enum values that are valid if type is enum
  #   idTable: table that type of id must be from
  #   aggrStatuses: statuses of aggregation to allow. list of "individual", "literal", "aggregate". Default: ["individual", "literal"]
  cleanExpr: (expr, options={}) ->
    _.defaults(options, {
      aggrStatuses: ["individual", "literal"]
      })

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
    if expr.type == "count"
      return @cleanCountExpr(expr, options)
    if expr.type == "literal" and expr.valueType == "enum[]"
      expr = { type: "literal", valueType: "enumset", value: expr.value }

    # Strip if wrong table 
    if options.table and expr.type != "literal" and expr.table != options.table
      return null

    # Strip if no table
    if not expr.table and expr.type != "literal" 
      return null

    # Strip if non-existent table
    if expr.table and not @schema.getTable(expr.table)
      return null

    # Default aggregation if needed and not aggregated
    if @exprUtils.getExprAggrStatus(expr) == "individual" and "individual" not in options.aggrStatuses and "aggregate" in options.aggrStatuses
      aggrOpItems = @exprUtils.findMatchingOpItems(resultTypes: options.types, lhsExpr: expr, aggr: true, ordered: @schema.getTable(expr.table)?.ordering?)

      # If aggr is required and there is at least one possible, use it
      if aggrOpItems.length > 0
        expr = { type: "op", op: aggrOpItems[0].op, table: expr.table, exprs: [expr] }

    # Default percent where + booleanization 
    if @exprUtils.getExprAggrStatus(expr) == "individual" and "individual" not in options.aggrStatuses and "aggregate" in options.aggrStatuses
      # Only if result types include number
      if not options.types or "number" in options.types
        # Find op item that matches
        opItem = @exprUtils.findMatchingOpItems(resultTypes: ["boolean"], lhsExpr: expr)[0]

        if opItem
          # Wrap in op to make it boolean
          expr = { type: "op", table: expr.table, op: opItem.op, exprs: [expr] }

          # Determine number of arguments to append
          args = opItem.exprTypes.length - 1

          # Add extra nulls for other arguments
          for i in [1..args]
            expr.exprs.push(null)

          expr = { type: "op", op: "percent where", table: expr.table, exprs: [expr] }

    # Strip if wrong aggregation status
    if @exprUtils.getExprAggrStatus(expr) and @exprUtils.getExprAggrStatus(expr) not in options.aggrStatuses
      return null

    # Get type
    type = @exprUtils.getExprType(expr)

    # Boolean-ize for easy building of filters
    # True if a boolean expression is required
    booleanOnly = options.types and options.types.length == 1 and options.types[0] == "boolean" 

    # If boolean and expr is not boolean, wrap with appropriate expression
    if booleanOnly and type and type != "boolean"
      # Find op item that matches
      opItem = @exprUtils.findMatchingOpItems(resultTypes: ["boolean"], lhsExpr: expr)[0]

      if opItem
        # Wrap in op to make it boolean
        expr = { type: "op", table: expr.table, op: opItem.op, exprs: [expr] }

        # Determine number of arguments to append
        args = opItem.exprTypes.length - 1

        # Add extra nulls for other arguments
        for i in [1..args]
          expr.exprs.push(null)

    # Get type again in case changed  
    type = @exprUtils.getExprType(expr)

    # Strip if wrong type
    if type and options.types and type not in options.types
      # case statements should be preserved as they are a variable type and they will have their then clauses cleaned
      if expr.type != "case"
        return null

    switch expr.type
      when "field"
        return @cleanFieldExpr(expr, options)
      when "scalar"
        return @cleanScalarExpr(expr, options)
      when "op"
        return @cleanOpExpr(expr, options)
      when "literal"
        return @cleanLiteralExpr(expr, options)
      when "case"
        return @cleanCaseExpr(expr, options)
      when "id"
        return @cleanIdExpr(expr, options)
      when "score"
        return @cleanScoreExpr(expr, options)
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
    if options.enumValueIds and column.type == "enum"
      if _.difference(_.pluck(column.enumValues, "id"), options.enumValueIds).length > 0
        return null

    if options.enumValueIds and column.type == "expr"
      if @exprUtils.getExprType(column.expr) == "enum"
        if _.difference(_.pluck(@exprUtils.getExprEnumValues(column.expr), "id"), options.enumValueIds).length > 0
          return null

    return expr

  cleanOpExpr: (expr, options) ->
    switch expr.op
      when "and", "or"
        expr = _.extend({}, expr, exprs: _.map(expr.exprs, (e) => @cleanExpr(e, types: ["boolean"], table: expr.table)))

        # Simplify
        if expr.exprs.length == 1
          return expr.exprs[0]
        if expr.exprs.length == 0
          return null

        return expr
      when "+", "*"
        expr = _.extend({}, expr, exprs: _.map(expr.exprs, (e) => @cleanExpr(e, types: ["number"], table: expr.table, aggrStatuses: options.aggrStatuses)))

        # Simplify
        if expr.exprs.length == 1
          return expr.exprs[0]
        if expr.exprs.length == 0
          return null

        return expr
      else 
        # Count always takes zero parameters and is valid if number type is valid
        if expr.op == "count" and (not options.types or "number" in options.types) and "aggregate" in options.aggrStatuses
          return { type: "op", op: "count", table: expr.table, exprs: [] }

        # Determine aggr setting. Prevent non-aggr for aggr and vice-versa
        aggr = null
        if "individual" not in options.aggrStatuses and "aggregate" in options.aggrStatuses
          aggr = true
        if "aggregate" not in options.aggrStatuses and "individual" in options.aggrStatuses
          aggr = false

        # Determine innerAggrStatuses (same as outer, unless aggregate expression, in which case always aggregate)
        if @exprUtils.findMatchingOpItems(op: expr.op)[0]?.aggr
          innerAggrStatuses = ["literal", "individual"]
        else
          innerAggrStatuses = options.aggrStatuses

        # First do a loose cleaning of LHS to remove obviously invalid values
        lhsExpr = @cleanExpr(expr.exprs[0], table: expr.table, aggrStatuses: innerAggrStatuses)

        # Now attempt to clean it restricting to the types the op allows as lhs
        if lhsExpr
          lhsTypes = _.uniq(_.compact(_.map(@exprUtils.findMatchingOpItems(op: expr.op), (opItem) -> opItem.exprTypes[0])))
          lhsExpr = @cleanExpr(expr.exprs[0], table: expr.table, aggrStatuses: innerAggrStatuses, types: lhsTypes)

          # If this nulls it, don't keep as we can switch ops to preseve it
          if not lhsExpr?
            lhsExpr = @cleanExpr(expr.exprs[0], table: expr.table, aggrStatuses: innerAggrStatuses)

        # Get opItem
        opItems = @exprUtils.findMatchingOpItems(op: expr.op, lhsExpr: lhsExpr, resultTypes: options.types, aggr: aggr, ordered: @schema.getTable(expr.table)?.ordering?)

        # Need LHS for a normal op that is not a prefix. If it is a prefix op, allow the op to stand alone without params
        if not lhsExpr and not opItems[0]?.prefix
          return null

        # If ambiguous, just clean subexprs and return
        if opItems.length > 1
          return _.extend({}, expr, { exprs: _.map(expr.exprs, (e, i) =>
            @cleanExpr(e, table: expr.table, aggrStatuses: innerAggrStatuses)
          )})

        # If not found, default opItem
        if not opItems[0]
          opItem = @exprUtils.findMatchingOpItems(lhsExpr: lhsExpr, resultTypes: options.types, aggr: aggr)[0]
          if not opItem
            return null

          expr = { type: "op", table: expr.table, op: opItem.op, exprs: [lhsExpr or null] }  
        else
          opItem = opItems[0]

        # Pad or trim number of expressions
        while expr.exprs.length < opItem.exprTypes.length
          exprs = expr.exprs.slice()
          exprs.push(null)
          expr = _.extend({}, expr, { exprs: exprs })

        if expr.exprs.length > opItem.exprTypes.length
          expr = _.extend({}, expr, { exprs: _.take(expr.exprs, opItem.exprTypes.length) })          

        # Clean all sub expressions
        if lhsExpr
          enumValues = @exprUtils.getExprEnumValues(lhsExpr)
          if enumValues
            enumValueIds = _.pluck(enumValues, "id")

        expr = _.extend({}, expr, { exprs: _.map(expr.exprs, (e, i) =>
          @cleanExpr(e, table: expr.table, types: (if opItem.exprTypes[i] then [opItem.exprTypes[i]]), enumValueIds: enumValueIds, idTable: @exprUtils.getExprIdTable(expr.exprs[0]), aggrStatuses: innerAggrStatuses)
          )})

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

    # Fix legacy entity joins (accidentally had entities.<tablename>. prepended)
    joins = _.map(expr.joins, (j) =>
      if j.match(/^entities\.[a-z_0-9]+\./)
        return j.split(".")[2]
      return j
      )
    expr = _.extend({}, expr, joins: joins)

    if not @exprUtils.areJoinsValid(expr.table, expr.joins)
      return null

    innerTable = @exprUtils.followJoins(expr.table, expr.joins)

    # Move aggr to inner expression
    if expr.aggr
      expr = _.extend({}, _.omit(expr, "aggr"), expr: { type: "op", table: innerTable, op: expr.aggr, exprs: [expr.expr] })

    # Clean where
    if expr.where
      expr.where = @cleanExpr(expr.where, table: innerTable)

    # Simplify to join column
    if not expr.where and expr.joins.length == 1 and expr.expr?.type == "id"
      return { type: "field", table: expr.table, column: expr.joins[0] }

    # Get inner expression type (must match unless is count which can count anything)
    if expr.expr
      isMultiple = @exprUtils.isMultipleJoins(expr.table, expr.joins)
      aggrStatuses = if isMultiple then ["literal", "aggregate"] else ["literal", "individual"]

      expr = _.extend({}, expr, { 
        expr: @cleanExpr(expr.expr, _.extend({}, options, { table: innerTable, aggrStatuses: aggrStatuses }))
      })    

    return expr

  cleanLiteralExpr: (expr, options) ->
    # Convert old types
    if expr.valueType in ['decimal', 'integer']
      expr = _.extend({}, expr, { valueType: "number"})

    # TODO strip if no value?

    # Remove if enum type is wrong
    if expr.valueType == "enum" and options.enumValueIds and expr.value and expr.value not in options.enumValueIds
      return null

    # Remove invalid enum types
    if expr.valueType == "enumset" and options.enumValueIds and expr.value
      expr = _.extend({}, expr, value: _.intersection(options.enumValueIds, expr.value))

    # Null if wrong table
    if expr.valueType == "id" and options.idTable and expr.idTable != options.idTable
      return null

    return expr

  cleanCaseExpr: (expr, options) ->
    # Simplify if no cases
    if expr.cases.length == 0
      return expr.else or null

    # Clean whens as boolean
    expr = _.extend({}, expr, 
      cases: _.map(expr.cases, (c) => 
        _.extend({}, c, {
          when: @cleanExpr(c.when, types: ["boolean"], table: expr.table)
          then: @cleanExpr(c.then, options)
        })
      )
      else: @cleanExpr(expr.else, options)
    )

    return expr

  cleanIdExpr: (expr, options) ->
    # Null if wrong table
    if options.idTable and expr.table != options.idTable
      return null
    return expr

  cleanScoreExpr: (expr, options) ->
    # Clean input
    expr = _.extend({}, expr, input: @cleanExpr(expr.input, { types: ['enum', 'enumset' ] }))

    # Remove scores if no input
    if not expr.input
      expr = _.extend({}, expr, scores: {})

    # Clean score values
    expr = _.extend({}, expr, scores: _.mapValues(expr.scores, (scoreExpr) => @cleanExpr(scoreExpr, { table: expr.table, types: ['number']})))

    # Remove unknown enum values 
    if expr.input
      enumValues = @exprUtils.getExprEnumValues(expr.input)
      expr = _.extend({}, expr, scores: _.pick(expr.scores, (value, key) =>
        return _.findWhere(enumValues, id: key) and value?
      ))

    return expr

  cleanComparisonExpr: (expr, options) =>
    # Upgrade to op
    newExpr = { type: "op", table: expr.table, op: expr.op, exprs: [expr.lhs] }
    if expr.rhs
      newExpr.exprs.push(expr.rhs)

    # Clean sub-expressions to handle legacy literals
    newExpr.exprs = _.map(newExpr.exprs, (e) => @cleanExpr(e))

    # If = true
    if expr.op == "= true"
      newExpr = expr.lhs

    if expr.op == "= false"
      newExpr = { type: "op", op: "not", table: expr.table, exprs: [expr.lhs] }

    if expr.op == "between" and expr.rhs and expr.rhs.type == "literal" and expr.rhs.valueType == "daterange"
      newExpr.exprs = [expr.lhs, { type: "literal", valueType: "date", value: expr.rhs.value[0] }, { type: "literal", valueType: "date", value: expr.rhs.value[1] }]

    if expr.op == "between" and expr.rhs and expr.rhs.type == "literal" and expr.rhs.valueType == "datetimerange"
      # If date, convert datetime to date
      if @exprUtils.getExprType(expr.lhs) == "date"
        newExpr.exprs = [expr.lhs, { type: "literal", valueType: "date", value: expr.rhs.value[0].substr(0, 10) }, { type: "literal", valueType: "date", value: expr.rhs.value[1].substr(0, 10) }]
      else        
        newExpr.exprs = [expr.lhs, { type: "literal", valueType: "datetime", value: expr.rhs.value[0] }, { type: "literal", valueType: "datetime", value: expr.rhs.value[1] }]

    return @cleanExpr(newExpr, options)

  cleanLogicalExpr: (expr, options) =>
    newExpr = { type: "op", op: expr.op, table: expr.table, exprs: expr.exprs }
    return @cleanExpr(newExpr, options)

  cleanCountExpr: (expr, options) =>
    newExpr = { type: "id", table: expr.table }
    return @cleanExpr(newExpr, options)

