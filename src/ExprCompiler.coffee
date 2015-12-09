_ = require 'lodash'
injectTableAlias = require './injectTableAlias'
injectTableAliases = require './injectTableAliases'
ExprUtils = require './ExprUtils'

# Compiles expressions to JsonQL
module.exports = class ExprCompiler 
  constructor: (schema) ->
    @schema = schema

  # Compile an expression. Pass expr and tableAlias.
  compileExpr: (options) =>
    expr = options.expr

    # Handle null
    if not expr
      return null

    switch expr.type 
      when "id"
        compiledExpr = { type: "field", tableAlias: options.tableAlias, column: @schema.getTable(expr.table).primaryKey }
      when "field"
        compiledExpr = @compileFieldExpr(options)
      when "scalar"
        compiledExpr = @compileScalarExpr(options)
      when "literal" 
        compiledExpr = { type: "literal", value: expr.value }
      when "op"
        compiledExpr = @compileOpExpr(options)
      when "case"
        compiledExpr = @compileCaseExpr(options)
      when "count" # DEPRECATED
        compiledExpr = null
      when "comparison" # DEPRECATED
        compiledExpr = @compileComparisonExpr(options)
      when "logical" # DEPRECATED
        compiledExpr = @compileLogicalExpr(options)
      else
        throw new Error("Expr type #{expr.type} not supported")
    return compiledExpr

  compileFieldExpr: (options) ->
    expr = options.expr

    # Check if column has custom jsonql
    column = @schema.getColumn(expr.table, expr.column)
    if not column
      throw new Error("Column #{expr.table}.#{expr.column} not found")

    # If column has custom jsonql, use that instead of id
    return @compileColumnRef(column.jsonql or column.id, options.tableAlias)

  compileScalarExpr: (options) ->
    expr = options.expr

    where = null
    from = null
    orderBy = null
    limit = null

    # Perform joins
    table = expr.table
    tableAlias = options.tableAlias

    # First join is in where clause
    if expr.joins and expr.joins.length > 0
      join = @schema.getColumn(expr.table, expr.joins[0]).join

      if join.jsonql
        where = injectTableAliases(join.jsonql, { "{from}": tableAlias, "{to}": "j1" })
      else
        # Use manual columns
        where = { 
          type: "op", op: "="
          exprs: [
            @compileColumnRef(join.toColumn, "j1")
            @compileColumnRef(join.fromColumn, tableAlias)
          ]
         }

      from = @compileTable(join.toTable, "j1")

      # We are now at j1, which is the to of the first join
      table = join.toTable
      tableAlias = "j1"

    # Perform remaining joins
    if expr.joins.length > 1
      for i in [1...expr.joins.length]
        join = @schema.getColumn(table, expr.joins[i]).join

        if join.jsonql
          onClause = injectTableAliases(join.jsonql, { "{from}": "j#{i}", "{to}": "j#{i+1}" })
        else
          # Use manual columns
          onClause = { 
            type: "op", op: "="
            exprs: [
              @compileColumnRef(join.fromColumn, "j#{i}")
              @compileColumnRef(join.toColumn, "j#{i+1}")
            ]
           }

        from = {
          type: "join"
          left: from
          right: @compileTable(join.toTable, "j#{i+1}")
          kind: "left"
          on: onClause
        }

        # We are now at jn
        table = join.toTable
        tableAlias = "j#{i+1}"

    # Compile where clause
    if expr.where
      extraWhere = @compileExpr(expr: expr.where, tableAlias: tableAlias)

      # Add to existing 
      if where
        where = { type: "op", op: "and", exprs: [where, extraWhere]}
      else
        where = extraWhere

    scalarExpr = @compileExpr(expr: expr.expr, tableAlias: tableAlias)
    
    # Aggregate
    if expr.aggr
      switch expr.aggr
        when "last"
          # Get ordering
          ordering = @schema.getTable(table).ordering
          if not ordering
            throw new Error("No ordering defined")

          # Limit
          limit = 1

          # order descending
          orderBy = [{ expr: @compileColumnRef(ordering, tableAlias),  direction: "desc" }]
        when "sum", "count", "avg", "max", "min", "stdev", "stdevp"
          # Don't include scalarExpr if null
          if not scalarExpr
            scalarExpr = { type: "op", op: expr.aggr, exprs: [] }
          else
            scalarExpr = { type: "op", op: expr.aggr, exprs: [scalarExpr] }
        else
          throw new Error("Unknown aggregation #{expr.aggr}")

    # If no where, from, orderBy or limit, just return expr for simplicity
    if not from and not where and not orderBy and not limit
      return scalarExpr

    # Create scalar
    scalar = {
      type: "scalar"
      expr: scalarExpr
    }

    if from
      scalar.from = from

    if where
      scalar.where = where

    if orderBy
      scalar.orderBy = orderBy

    if limit
      scalar.limit = limit

    return scalar

  compileOpExpr: (options) ->
    expr = options.expr

    compiledExprs = _.map(expr.exprs, (e) => @compileExpr(expr: e, tableAlias: options.tableAlias))

    # Handle multi
    switch expr.op
      when "and", "or", "+", "*"
        # Strip nulls
        compiledExprs = _.compact(compiledExprs)
        if compiledExprs.length == 0
          return null

        return { 
          type: "op"
          op: expr.op
          exprs: compiledExprs
        }
      when "-", "/", ">", "<", ">=", "<=", "<>", "=", "~*"
        # Null if any not present
        if _.any(compiledExprs, (ce) -> not ce?)
          return null

        return { 
          type: "op"
          op: expr.op
          exprs: compiledExprs
        }
      when '= any'
        # Null if any not present
        if _.any(compiledExprs, (ce) -> not ce?)
          return null

        # Null if empty list on rhs
        if expr.exprs[1].type == "literal"
          if not expr.exprs[1].value or (_.isArray(expr.exprs[1].value) and expr.exprs[1].value.length == 0)
            return null

        return { type: "op", op: "=", modifier: "any", exprs: compiledExprs }

      when "between"
        # Null if first not present
        if not compiledExprs[0]
          return null

        # Null if second and third not present
        if not compiledExprs[1] and not compiledExprs[2]
          return null

        # >= if third missing
        if not compiledExprs[2]
          return {
            type: "op"
            op: ">="
            exprs: [compiledExprs[0], compiledExprs[1]]
          }

        # <= if second missing
        if not compiledExprs[1]
          return {
            type: "op"
            op: "<="
            exprs: [compiledExprs[0], compiledExprs[2]]
          }

        # Between
        return {
          type: "op"
          op: "between"
          exprs: compiledExprs
        }

      when "not", "is null", "is not null"
        if not compiledExprs[0]
          return null

        return {
          type: "op"
          op: expr.op
          exprs: compiledExprs
        }
      when "contains"
        # Null if either not present
        if not compiledExprs[0] or not compiledExprs[1]
          return null

        # Cast both to jsonb and use @>
        return {
          type: "op"
          op: "@>"
          exprs: [
            { type: "op", op: "::jsonb", exprs: [compiledExprs[0]] }
            { type: "op", op: "::jsonb", exprs: [compiledExprs[1]] }
          ]
        }

      else
        throw new Error("Unknown op #{expr.op}")


  compileCaseExpr: (options) ->
    expr = options.expr

    return {
      type: "case"
      cases: _.map(expr.cases, (c) =>
        {
          when: @compileExpr(expr: c.when, tableAlias: options.tableAlias)
          then: @compileExpr(expr: c.then, tableAlias: options.tableAlias)
        })
      else: @compileExpr(expr: expr.else, tableAlias: options.tableAlias)
    }

  compileComparisonExpr: (options) ->
    expr = options.expr

    # Missing right-hand side means null condition
    exprUtils = new ExprUtils(@schema)
    if exprUtils.getComparisonRhsType(exprUtils.getExprType(expr.lhs), expr.op) and not expr.rhs?
      return null

    lhsExpr = @compileExpr(expr: expr.lhs, tableAlias: options.tableAlias) 
    if expr.rhs
      rhsExpr = @compileExpr(expr: expr.rhs, tableAlias: options.tableAlias)
      exprs = [lhsExpr, rhsExpr]
    else
      exprs = [lhsExpr]

    # Handle special cases 
    switch expr.op
      when '= true'
        return { type: "op", op: "=", exprs: [lhsExpr, { type: "literal", value: true }]}
      when '= false'
        return { type: "op", op: "=", exprs: [lhsExpr, { type: "literal", value: false }]}
      when '= any'
        return { type: "op", op: "=", modifier: "any", exprs: exprs }
      when 'between'
        return { type: "op", op: "between", exprs: [lhsExpr, { type: "literal", value: expr.rhs.value[0] }, { type: "literal", value: expr.rhs.value[1] }] }
      else
        return { 
          type: "op"
          op: expr.op
          exprs: exprs
        }

  compileLogicalExpr: (options) ->
    expr = options.expr

    compiledExprs = _.map(expr.exprs, (e) => @compileExpr(expr: e, tableAlias: options.tableAlias))

    # Remove nulls
    compiledExprs = _.compact(compiledExprs)

    # Simplify
    if compiledExprs.length == 1
      return compiledExprs[0]

    if compiledExprs.length == 0
      return null

    return { 
      type: "op"
      op: expr.op
      exprs: compiledExprs
    }

  # Compiles a reference to a column or a JsonQL expression
  # If parameter is a string, create a simple field expression
  # If parameter is an object, inject tableAlias for `{alias}`
  compileColumnRef: (column, tableAlias) ->
    if _.isString(column)
      return { type: "field", tableAlias: tableAlias, column: column }

    return injectTableAlias(column, tableAlias)

  # Compiles a table, substituting with custom jsonql if required
  compileTable: (tableId, alias) ->
    table = @schema.getTable(tableId)
    if not table.jsonql
      return { type: "table", table: tableId, alias: alias }
    else
      return { type: "subquery", query: table.jsonql, alias: alias }
