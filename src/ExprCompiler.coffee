_ = require 'lodash'
injectTableAlias = require './injectTableAlias'
injectTableAliases = require './injectTableAliases'
ExprUtils = require './ExprUtils'
moment = require 'moment'
ColumnNotFoundException = require './ColumnNotFoundException'

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
      when "score"
        compiledExpr = @compileScoreExpr(options)
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

    column = @schema.getColumn(expr.table, expr.column)
    if not column
      throw new ColumnNotFoundException("Column #{expr.table}.#{expr.column} not found")

    # Handle joins specially
    if column.type == "join"
      # If id is result
      if column.join.type in ['1-1', 'n-1']
        # Use scalar to create
        return @compileScalarExpr(expr: { type: "scalar", table: expr.table, joins: [column.id], expr: { type: "id", table: column.join.toTable }}, tableAlias: options.tableAlias)
      else
        return {
          type: "scalar"
          expr: { 
            type: "op"
            op: "to_jsonb"
            exprs: [
              { 
                type: "op"
                op: "array_agg"
                exprs: [
                  { type: "field", tableAlias: "inner", column: @schema.getTable(column.join.toTable).primaryKey }
                ]
              }
            ]
          }
          from: @compileTable(column.join.toTable, "inner")
          where: @compileJoin(column.join, options.tableAlias, "inner")
        }

    # Handle expr type
    if column.type == "expr"
      return @compileExpr({ expr: column.expr, tableAlias: options.tableAlias })

    # If column has custom jsonql, use that instead of id
    return @compileColumnRef(column.jsonql or column.id, options.tableAlias)

  compileScalarExpr: (options) ->
    expr = options.expr

    where = null
    from = null
    orderBy = null
    limit = null

    # Null expr is null
    if not expr.expr
      return null

    # Simplify if a join to an id field where the join uses the primary key of the to table
    if not expr.aggr and not expr.where and expr.joins.length == 1 and expr.expr.type == "id" and @schema.getColumn(expr.table, expr.joins[0]).join.toColumn == @schema.getTable(expr.expr.table).primaryKey
      return @compileColumnRef(@schema.getColumn(expr.table, expr.joins[0]).join.fromColumn, options.tableAlias)

    # Generate a consistent, semi-unique alias
    generateAlias = (expr, joinIndex) ->
      # Make alias-friendly (replace all symbols with _)
      return expr.joins[joinIndex].replace(/[^a-zA-Z0-9]/g, "_").toLowerCase()

    # Perform joins
    table = expr.table
    tableAlias = options.tableAlias

    # First join is in where clause
    if expr.joins and expr.joins.length > 0
      joinColumn = @schema.getColumn(expr.table, expr.joins[0])
      if not joinColumn
        throw new ColumnNotFoundException("Join column #{expr.table}:#{expr.joins[0]} not found")
      join = joinColumn.join

      # Generate a consistent, semi-unique alias
      alias = generateAlias(expr, 0)

      where = @compileJoin(join, tableAlias, alias)

      from = @compileTable(join.toTable, alias)

      # We are now at j1, which is the to of the first join
      table = join.toTable
      tableAlias = alias

    # Perform remaining joins
    if expr.joins.length > 1
      for i in [1...expr.joins.length]
        joinColumn = @schema.getColumn(table, expr.joins[i])
        if not joinColumn
          throw new ColumnNotFoundException("Join column #{expr.table}:#{expr.joins[0]} not found")
        join = joinColumn.join

        # Generate a consistent, semi-unique alias
        nextAlias = generateAlias(expr, i)

        onClause = @compileJoin(join, tableAlias, nextAlias)

        from = {
          type: "join"
          left: from
          right: @compileTable(join.toTable, nextAlias)
          kind: "inner"
          on: onClause
        }

        # We are now at jn
        table = join.toTable
        tableAlias = nextAlias

    # Compile where clause
    if expr.where
      extraWhere = @compileExpr(expr: expr.where, tableAlias: tableAlias)

      # Add to existing 
      if where
        where = { type: "op", op: "and", exprs: [where, extraWhere]}
      else
        where = extraWhere

    scalarExpr = @compileExpr(expr: expr.expr, tableAlias: tableAlias)
    
    # Aggregate DEPRECATED
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

    # If no expr, return null
    if not scalarExpr
      return null

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

  # Compile a join into an on or where clause
  #  join: join part of column definition
  #  fromAlias: alias of from table
  #  toAlias: alias of to table
  compileJoin: (join, fromAlias, toAlias) ->
    if join.jsonql
      return injectTableAliases(join.jsonql, { "{from}": fromAlias, "{to}": toAlias })
    else
      # Use manual columns
      return { 
        type: "op", op: "="
        exprs: [
          @compileColumnRef(join.toColumn, toAlias)
          @compileColumnRef(join.fromColumn, fromAlias)
        ]
      }

  # Compile an expression. Pass expr and tableAlias.
  compileOpExpr: (options) ->
    exprUtils = new ExprUtils(@schema)

    expr = options.expr

    compiledExprs = _.map(expr.exprs, (e) => @compileExpr(expr: e, tableAlias: options.tableAlias))

    # Get type of expr 0
    expr0Type = exprUtils.getExprType(expr.exprs[0])

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
      when "-", "/", ">", "<", ">=", "<=", "<>", "=", "~*", "round", "floor", "ceiling", "sum", "avg", "min", "max", "count", "stdev", "stdevp", "var", "varp"
        # Null if any not present
        if _.any(compiledExprs, (ce) -> not ce?)
          return null

        return { 
          type: "op"
          op: expr.op
          exprs: compiledExprs
        }
      when "last"
        # Null if not present
        if not compiledExprs[0]
          return null

        # Get ordering
        ordering = @schema.getTable(expr.table)?.ordering
        if not ordering
          throw new Error("Table #{expr.table} must be ordered to use last()")

        # (array_agg(xyz order by theordering desc nulls last))[1]
        return { 
          type: "op"
          op: "[]"
          exprs: [
            { type: "op", op: "array_agg", exprs: [compiledExprs[0]], orderBy: [{ expr: { type: "field", tableAlias: options.tableAlias, column: ordering }, direction: "desc", nulls: "last" }] }
            1
          ]
        }

      when "last where"
        # Null if not value present
        if not compiledExprs[0]
          return null

        # Get ordering
        ordering = @schema.getTable(expr.table)?.ordering
        if not ordering
          throw new Error("Table #{expr.table} must be ordered to use last()")

        # Simple last if not condition present
        if not compiledExprs[1]
          # (array_agg(xyz order by theordering desc nulls last))[1]
          return { 
            type: "op"
            op: "[]"
            exprs: [
              { type: "op", op: "array_agg", exprs: [compiledExprs[0]], orderBy: [{ expr: { type: "field", tableAlias: options.tableAlias, column: ordering }, direction: "desc", nulls: "last" }] }
              1
            ]
          }

        # Compiles to:
        # (array_agg((case when <condition> then <value> else null end) order by (case when <condition> then 0 else 1 end), <ordering> desc nulls last))[1]
        # which prevents non-matching from appearing
        return { 
          type: "op"
          op: "[]"
          exprs: [
            { 
              type: "op"
              op: "array_agg"
              exprs: [
                { type: "case", cases: [{ when: compiledExprs[1], then: compiledExprs[0] }], else: null }
              ]
              orderBy: [
                { expr: { type: "case", cases: [{ when: compiledExprs[1], then: 0 }], else: 1 } }
                { expr: { type: "field", tableAlias: options.tableAlias, column: ordering }, direction: "desc", nulls: "last" }
              ] 
            }
            1
          ]
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

        # Null if no expressions in literal list
        if compiledExprs[1].type == "literal" and compiledExprs[1].value.length == 0
          return null

        # Cast both to jsonb and use @>. Also convert both to json first to handle literal arrays
        return {
          type: "op"
          op: "@>"
          exprs: [
            { type: "op", op: "::jsonb", exprs: [{ type: "op", op: "to_json", exprs: [compiledExprs[0]] }] }
            { type: "op", op: "::jsonb", exprs: [{ type: "op", op: "to_json", exprs: [compiledExprs[1]] }] }
          ]
        }

      when "length"
        # Null if not present
        if not compiledExprs[0] 
          return null

        # Cast both to jsonb and use jsonb_array_length. Also convert both to json first to handle literal arrays
        return {
          type: "op"
          op: "jsonb_array_length"
          exprs: [
            { type: "op", op: "::jsonb", exprs: [{ type: "op", op: "to_json", exprs: [compiledExprs[0]] }] }
          ]
        }

      when "to text"
        # Null if not present
        if not compiledExprs[0]
          return null

        # Null if no enum values
        enumValues = exprUtils.getExprEnumValues(expr.exprs[0])
        if not enumValues
          return null

        return {
          type: "case"
          input: compiledExprs[0]
          cases: _.map(enumValues, (ev) =>
            {
              when: { type: "literal", value: ev.id }
              then: { type: "literal", value: exprUtils.localizeString(ev.name, expr.locale) }
            })
        }

      when "count where"
        # Null if not present
        if not compiledExprs[0] 
          return null

        return {
          type: "op"
          op: "sum"
          exprs: [
            { 
              type: "case"
              cases: [
                when: compiledExprs[0]
                then: 1
              ]
              else: 0
            }
          ]
        }

      when "percent where"
        # Null if not present
        if not compiledExprs[0] 
          return null

        # Compiles as sum(case when cond [and basis (if present)] then 100 else 0 end)/sum(1 [or case when basis then 1 else 0 (if present)]) (prevent div by zero)        
        return {
          type: "op"
          op: "/"
          exprs: [
            { 
              type: "op"
              op: "sum"
              exprs: [
                { 
                  type: "case"
                  cases: [
                    when: if compiledExprs[1] then { type: "op", op: "and", exprs: [compiledExprs[0], compiledExprs[1]] } else compiledExprs[0]
                    then: 100
                  ]
                  else: 0
                }
              ]
            }
            if compiledExprs[1]
              {
                type: "op"
                op: "nullif"
                exprs: [
                  { 
                    type: "op"
                    op: "sum"
                    exprs: [
                      { 
                        type: "case"
                        cases: [
                          when: compiledExprs[1]
                          then: 1
                        ]
                        else: 0
                      }
                    ]
                  }
                  0
                ]
              }
            else
              { type: "op", op: "sum", exprs: [1] }
          ]
        }

      when "sum where"
        # Null if not present
        if not compiledExprs[0] 
          return null

        # Simple sum if not specified where
        if not compiledExprs[1]
          return {
            type: "op"
            op: "sum"
            exprs: [compiledExprs[0]]
          }

        return {
          type: "op"
          op: "sum"
          exprs: [
            { 
              type: "case"
              cases: [
                when: compiledExprs[1]
                then: compiledExprs[0]
              ]
              else: 0
            }
          ]
        }

      # Hierarchical test that uses ancestry column
      when "within"
        # Null if either not present
        if not compiledExprs[0] or not compiledExprs[1]
          return null

        # Get table being used
        idTable = exprUtils.getExprIdTable(expr.exprs[0])

        return {
          type: "op"
          op: "in"
          exprs: [
            compiledExprs[0]
            {
              type: "scalar"
              expr: { type: "field", tableAlias: "subwithin", column: @schema.getTable(idTable).primaryKey }
              from: { type: "table", table: idTable, alias: "subwithin" }
              where: {
                type: "op"
                op: "@>"
                exprs: [
                  { type: "field", tableAlias: "subwithin", column: @schema.getTable(idTable).ancestry }
                  { type: "op", op: "::jsonb", exprs: [{ type: "op", op: "json_build_array", exprs: [compiledExprs[1]] }] }
                ]
              }
            }            
          ]
        }

      when "latitude"
        if not compiledExprs[0]
          return null

        return {
          type: "op"
          op: "ST_Y"
          exprs: [
            { type: "op", op: "ST_Transform", exprs: [compiledExprs[0], 4326] }
          ]
        }

      when "longitude"
        if not compiledExprs[0]
          return null

        return {
          type: "op"
          op: "ST_X"
          exprs: [
            { type: "op", op: "ST_Transform", exprs: [compiledExprs[0], 4326] }
          ]
        }

      when 'days difference'
        if not compiledExprs[0] or not compiledExprs[1]
          return null

        if exprUtils.getExprType(expr.exprs[0]) == "date"
          return {
            type: "op"
            op: "-"
            exprs: [
              { type: "op", op: "::date", exprs: [compiledExprs[0]] }
              { type: "op", op: "::date", exprs: [compiledExprs[1]] }
            ]
          }

        if exprUtils.getExprType(expr.exprs[0]) == "datetime"
          return {
            type: "op"
            op: "/"
            exprs: [
              {
                type: "op"
                op: "-"
                exprs: [
                  { type: "op", op: "date_part", exprs: ['epoch', { type: "op", op: "::timestamp", exprs: [compiledExprs[0]] }] }
                  { type: "op", op: "date_part", exprs: ['epoch', { type: "op", op: "::timestamp", exprs: [compiledExprs[1]] }] }
                ]
              }
              86400
            ]
          }

        return null

      when 'days since'
        if not compiledExprs[0]
          return null

        switch expr0Type
          when "date"
            return { 
              type: "op"
              op: "-"
              exprs: [
                { type: "op", op: "::date", exprs: [moment().format("YYYY-MM-DD")] }
                { type: "op", op: "::date", exprs: [compiledExprs[0]] }
              ]
            }
          when "datetime"
            return {
              type: "op"
              op: "/"
              exprs: [
                { 
                  type: "op"
                  op: "-"
                  exprs: [
                    { type: "op", op: "date_part", exprs: ['epoch', { type: "op", op: "::timestamp", exprs: [moment().toISOString()] }] }
                    { type: "op", op: "date_part", exprs: ['epoch', { type: "op", op: "::timestamp", exprs: [compiledExprs[0]] }] }
                  ]
                }
                86400
              ]
            }
          else
            return null


      when 'thisyear'
        if not compiledExprs[0]
          return null

        switch expr0Type
          when "date"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("year").format("YYYY-MM-DD") ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("year").add(1, 'years').format("YYYY-MM-DD") ] }
              ]
            }
          when "datetime"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("year").toISOString() ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("year").add(1, 'years').toISOString() ] }
              ]
            }
          else
            return null

      when 'lastyear'
        if not compiledExprs[0]
          return null

        switch expr0Type
          when "date"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("year").subtract(1, 'years').format("YYYY-MM-DD") ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("year").format("YYYY-MM-DD") ] }
              ]
            }
          when "datetime"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("year").subtract(1, 'years').toISOString() ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("year").toISOString() ] }
              ]
            }
          else
            return null

      when 'thismonth'
        if not compiledExprs[0]
          return null

        switch expr0Type
          when "date"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("month").format("YYYY-MM-DD") ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("month").add(1, 'months').format("YYYY-MM-DD") ] }
              ]
            }
          when "datetime"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("month").toISOString() ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("month").add(1, 'months').toISOString() ] }
              ]
            }
          else
            return null

      when 'lastmonth'
        if not compiledExprs[0]
          return null

        switch expr0Type
          when "date"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("month").subtract(1, 'months').format("YYYY-MM-DD") ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("month").format("YYYY-MM-DD") ] }
              ]
            }
          when "datetime"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("month").subtract(1, 'months').toISOString() ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("month").toISOString() ] }
              ]
            }
          else
            return null

      when 'today'
        if not compiledExprs[0]
          return null

        switch expr0Type
          when "date"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().format("YYYY-MM-DD") ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD") ] }
              ]
            }
          when "datetime"
           return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("day").toISOString() ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString() ] }
              ]
            }
          else
            return null

      when 'yesterday'
        if not compiledExprs[0]
          return null

        switch expr0Type
          when "date"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().subtract(1, 'days').format("YYYY-MM-DD") ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().format("YYYY-MM-DD") ] }
              ]
            }
          when "datetime"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("day").subtract(1, 'days').toISOString() ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("day").toISOString() ] }
              ]
            }
          else
            return null

      when 'last7days'
        if not compiledExprs[0]
          return null

        switch expr0Type
          when "date"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().subtract(7, 'days').format("YYYY-MM-DD") ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD") ] }
              ]
            }
          when "datetime"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("day").subtract(7, 'days').toISOString() ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString() ] }
              ]
            }
          else
            return null

      when 'last30days'
        if not compiledExprs[0]
          return null

        switch expr0Type
          when "date"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().subtract(30, 'days').format("YYYY-MM-DD") ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD") ] }
              ]
            }
          when "datetime"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("day").subtract(30, 'days').toISOString() ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString() ] }
              ]
            }
          else
            return null

      when 'last365days'
        if not compiledExprs[0]
          return null

        switch expr0Type
          when "date"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().subtract(365, 'days').format("YYYY-MM-DD") ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD") ] }
              ]
            }
          when "datetime"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("day").subtract(365, 'days').toISOString() ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString() ] }
              ]
            }
          else
            return null

      when 'distance'
        if not compiledExprs[0] or not compiledExprs[1]
          return null

        return {
          type: "op"
          op: "ST_Distance_Sphere"
          exprs: [
            { type: "op", op: "ST_Transform", exprs: [compiledExprs[0], 4326] }
            { type: "op", op: "ST_Transform", exprs: [compiledExprs[1], 4326] }
          ]
        }

      else
        throw new Error("Unknown op #{expr.op}")


  compileCaseExpr: (options) ->
    expr = options.expr

    compiled = {
      type: "case"
      cases: _.map(expr.cases, (c) =>
        {
          when: @compileExpr(expr: c.when, tableAlias: options.tableAlias)
          then: @compileExpr(expr: c.then, tableAlias: options.tableAlias)
        })
      else: @compileExpr(expr: expr.else, tableAlias: options.tableAlias)
    }

    # Remove null cases
    compiled.cases = _.filter(compiled.cases, (c) -> c.when?)

    # Return null if no cases
    if compiled.cases.length == 0
      return null

    return compiled

  compileScoreExpr: (options) ->
    expr = options.expr
    exprUtils = new ExprUtils(@schema)

    # If empty, literal 0
    if _.isEmpty(expr.scores)
      return { type: "literal", value: 0 }

    # Get type of input
    inputType = exprUtils.getExprType(expr.input)

    switch inputType
      when "enum"
        return {
          type: "case"
          input: @compileExpr(expr: expr.input, tableAlias: options.tableAlias)
          cases: _.map(_.pairs(expr.scores), (pair) =>
            { 
              when: { type: "literal", value: pair[0] }
              then: @compileExpr(expr: pair[1], tableAlias: options.tableAlias) 
            }
          )
          else: { type: "literal", value: 0 }
        }
      when "enumset"
        return {
          type: "op"
          op: "+"
          exprs: _.map(_.pairs(expr.scores), (pair) =>
            {
              type: "case"
              cases: [
                { 
                  when: {
                    type: "op"
                    op: "@>"
                    exprs: [
                      { type: "op", op: "::jsonb", exprs: [{ type: "op", op: "to_json", exprs: [@compileExpr(expr: expr.input, tableAlias: options.tableAlias)] }] }
                      { type: "op", op: "::jsonb", exprs: [{ type: "op", op: "to_json", exprs: [{ type: "literal", value: [pair[0]] }] }]}
                    ]
                  }
                  then: @compileExpr(expr: pair[1], tableAlias: options.tableAlias) 
                }
              ]
              else: { type: "literal", value: 0 }
            }
          )
        }

      # Null if no expression
      else
        return null

  compileComparisonExpr: (options) ->
    expr = options.expr
    exprUtils = new ExprUtils(@schema)

    # Missing left-hand side type means null condition
    exprLhsType = exprUtils.getExprType(expr.lhs)
    if not exprLhsType
      return null

    # Missing right-hand side means null condition
    if exprUtils.getComparisonRhsType(exprLhsType, expr.op) and not expr.rhs?
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
