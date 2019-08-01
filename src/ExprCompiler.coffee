_ = require 'lodash'
injectTableAlias = require './injectTableAlias'
injectTableAliases = require './injectTableAliases'
ExprUtils = require './ExprUtils'
moment = require 'moment'
ColumnNotFoundException = require './ColumnNotFoundException'

# now expression: (to_json(now() at time zone 'UTC')#>>'{}')
nowExpr = {
  type: "op"
  op: "#>>"
  exprs: [
    { type: "op", op: "to_json", exprs: [
      { type: "op", op: "at time zone", exprs: [
        { type: "op", op: "now", exprs: [] }
        "UTC"
      ]}
    ]}
    "{}"
  ]
}

# now 24 hours ago: (to_json((now() - interval '24 hour') at time zone 'UTC')#>>'{}')
nowMinus24HoursExpr = {
  type: "op"
  op: "#>>"
  exprs: [
    { type: "op", op: "to_json", exprs: [
      { type: "op", op: "at time zone", exprs: [
        { type: "op", op: "-", exprs: [{ type: "op", op: "now", exprs: [] }, { type: "literal", value: "24 hour" }] }
        "UTC"
      ]}
    ]}
    "{}"
  ]
}


# Compiles expressions to JsonQL
module.exports = class ExprCompiler 
  # Variable values are lookup of id to variable value
  constructor: (schema, variables = [], variableValues = {}) ->
    @schema = schema
    @variables = variables
    @variableValues = variableValues

  # Compile an expression. Pass expr and tableAlias.
  compileExpr: (options) =>
    expr = options.expr

    # Handle null
    if not expr
      return null

    switch expr.type 
      when "id"
        compiledExpr = @compileColumnRef(@schema.getTable(expr.table).primaryKey, options.tableAlias)
      when "field"
        compiledExpr = @compileFieldExpr(options)
      when "scalar"
        compiledExpr = @compileScalarExpr(options)
      when "literal" 
        if expr.value?
          compiledExpr = { type: "literal", value: expr.value }
        else
          compiledExpr = null
      when "op"
        compiledExpr = @compileOpExpr(options)
      when "case"
        compiledExpr = @compileCaseExpr(options)
      when "score"
        compiledExpr = @compileScoreExpr(options)
      when "build enumset"
        compiledExpr = @compileBuildEnumsetExpr(options)
      when "variable"
        compiledExpr = @compileVariableExpr(options)
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
                  @compileColumnRef(@schema.getTable(column.join.toTable).primaryKey, "inner")
                ]
              }
            ]
          }
          from: @compileTable(column.join.toTable, "inner")
          where: @compileJoin(column.join, options.tableAlias, "inner")
          limit: 1  # Limit 1 to be safe
        }

    # Handle if has expr 
    if column.expr
      return @compileExpr({ expr: column.expr, tableAlias: options.tableAlias })

    # If column has custom jsonql, use that instead of id
    return @compileColumnRef(column.jsonql or column.id, options.tableAlias)

  compileScalarExpr: (options) ->
    expr = options.expr

    where = null
    from = null
    orderBy = null

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
          throw new ColumnNotFoundException("Join column #{table}:#{expr.joins[i]} not found")
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

          # order descending
          orderBy = [{ expr: @compileFieldExpr(expr: { type: "field", table: table, column: ordering}, tableAlias: tableAlias), direction: "desc" }]
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
    if not from and not where and not orderBy
      return scalarExpr

    # Create scalar
    scalar = {
      type: "scalar"
      expr: scalarExpr
      limit: 1
    }

    if from
      scalar.from = from

    if where
      scalar.where = where

    if orderBy
      scalar.orderBy = orderBy

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
      when "and", "or"
        # Strip nulls
        compiledExprs = _.compact(compiledExprs)
        if compiledExprs.length == 0
          return null

        return { 
          type: "op"
          op: expr.op
          exprs: compiledExprs
        }
      when "*"
        # Strip nulls
        compiledExprs = _.compact(compiledExprs)
        if compiledExprs.length == 0
          return null

        # Cast to decimal before multiplying to prevent integer overflow
        return { 
          type: "op"
          op: expr.op
          exprs: _.map(compiledExprs, (e) -> { type: "op", op: "::decimal", exprs: [e] })
        }
      when "+"
        # Strip nulls
        compiledExprs = _.compact(compiledExprs)
        if compiledExprs.length == 0
          return null

        # Cast to decimal before adding to prevent integer overflow. Do cast on internal expr to prevent coalesce mismatch
        return { 
          type: "op"
          op: expr.op
          exprs: _.map(compiledExprs, (e) -> { type: "op", op: "coalesce", exprs: [{ type: "op", op: "::decimal", exprs: [e] }, 0] })
        }
      when "-"
        # Null if any not present
        if _.any(compiledExprs, (ce) -> not ce?)
          return null

        # Cast to decimal before subtracting to prevent integer overflow
        return { 
          type: "op"
          op: expr.op
          exprs: _.map(compiledExprs, (e) -> { type: "op", op: "::decimal", exprs: [e] })
        }
      when ">", "<", ">=", "<=", "<>", "=", "~*", "round", "floor", "ceiling", "sum", "avg", "min", "max", "count", "stdev", "stdevp", "var", "varp", "array_agg"
        # Null if any not present
        if _.any(compiledExprs, (ce) -> not ce?)
          return null

        return { 
          type: "op"
          op: expr.op
          exprs: compiledExprs
        }
      when "/"
        # Null if any not present
        if _.any(compiledExprs, (ce) -> not ce?)
          return null

        # Cast to decimal before dividing to prevent integer math
        return { 
          type: "op"
          op: expr.op
          exprs: [
            compiledExprs[0]
            { type: "op", op: "::decimal", exprs: [{ type: "op", op: "nullif", exprs: [compiledExprs[1], 0] }] }
          ]
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
            { type: "op", op: "array_agg", exprs: [compiledExprs[0]], orderBy: [{ expr: @compileFieldExpr(expr: { type: "field", table: expr.table, column: ordering}, tableAlias: options.tableAlias), direction: "desc", nulls: "last" }] }
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
              { type: "op", op: "array_agg", exprs: [compiledExprs[0]], orderBy: [{ expr: @compileFieldExpr(expr: { type: "field", table: expr.table, column: ordering}, tableAlias: options.tableAlias), direction: "desc", nulls: "last" }] }
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
                { expr: @compileFieldExpr(expr: { type: "field", table: expr.table, column: ordering}, tableAlias: options.tableAlias), direction: "desc", nulls: "last" }
              ] 
            }
            1
          ]
        }

      when "previous"
        # Null if not present
        if not compiledExprs[0]
          return null

        # Get ordering
        ordering = @schema.getTable(expr.table)?.ordering
        if not ordering
          throw new Error("Table #{expr.table} must be ordered to use previous()")

        # (array_agg(xyz order by theordering desc nulls last))[2]
        return { 
          type: "op"
          op: "[]"
          exprs: [
            { type: "op", op: "array_agg", exprs: [compiledExprs[0]], orderBy: [{ expr: @compileFieldExpr(expr: { type: "field", table: expr.table, column: ordering }, tableAlias: options.tableAlias), direction: "desc", nulls: "last" }] }
            2
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

      when "not"
        if not compiledExprs[0]
          return null

        return {
          type: "op"
          op: expr.op
          exprs: [
            { type: "op", op: "coalesce", exprs: [compiledExprs[0], false] }
          ]
        }

      when "is null", "is not null"
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

      when "intersects"
        # Null if either not present
        if not compiledExprs[0] or not compiledExprs[1]
          return null

        # Null if no expressions in literal list
        if compiledExprs[1].type == "literal" and compiledExprs[1].value.length == 0
          return null

        # Cast to jsonb and use ?| Also convert to json first to handle literal arrays
        return {
          type: "op"
          op: "?|"
          exprs: [
            { type: "op", op: "::jsonb", exprs: [{ type: "op", op: "to_json", exprs: [compiledExprs[0]] }] }
            compiledExprs[1]
          ]
        }

      when "length"
        # 0 if null
        if not compiledExprs[0]?
          return 0

        # Cast both to jsonb and use jsonb_array_length. Also convert both to json first to handle literal arrays. Coalesce to 0 so that null is 0
        return {
          type: "op"
          op: "coalesce",
          exprs: [
            {
              type: "op"
              op: "jsonb_array_length"
              exprs: [
                { type: "op", op: "::jsonb", exprs: [{ type: "op", op: "to_json", exprs: [compiledExprs[0]] }] }
              ]
            }
            0
          ]
        }

      when "line length"
        # null if null
        if not compiledExprs[0]?
          return null

        # ST_Length_Spheroid(ST_Transform(location,4326), 'SPHEROID["GRS_1980",6378137,298.257222101]')
        return {
          type: "op"
          op: "ST_Length_Spheroid",
          exprs: [
            {
              type: "op"
              op: "ST_Transform"
              exprs: [compiledExprs[0], 4326]
            }
            'SPHEROID["GRS_1980",6378137,298.257222101]'
          ]
        }

      when "to text"
        # Null if not present
        if not compiledExprs[0]
          return null

        if exprUtils.getExprType(expr.exprs[0]) == "enum"
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

        if exprUtils.getExprType(expr.exprs[0]) == "number"
          return {
            type: "op"
            op: "::text"
            exprs: [compiledExprs[0]]
          }

        return null

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

        # Compiles as sum(case when cond [and basis (if present)] then 100::decimal else 0 end)/sum(1 [or case when basis then 1 else 0 (if present)]) (prevent div by zero)        
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
                    then: { type: "op", op: "::decimal", exprs: [100] }
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

      when "min where"
        # Null if not present
        if not compiledExprs[0] 
          return null

        # Simple min if not specified where
        if not compiledExprs[1]
          return {
            type: "op"
            op: "min"
            exprs: [compiledExprs[0]]
          }

        return {
          type: "op"
          op: "min"
          exprs: [
            { 
              type: "case"
              cases: [
                when: compiledExprs[1]
                then: compiledExprs[0]
              ]
              else: null
            }
          ]
        }

      when "max where"
        # Null if not present
        if not compiledExprs[0] 
          return null

        # Simple max if not specified where
        if not compiledExprs[1]
          return {
            type: "op"
            op: "max"
            exprs: [compiledExprs[0]]
          }

        return {
          type: "op"
          op: "max"
          exprs: [
            { 
              type: "case"
              cases: [
                when: compiledExprs[1]
                then: compiledExprs[0]
              ]
              else: null
            }
          ]
        }

      when "count distinct"
        # Null if not present
        if not compiledExprs[0] 
          return null

        return {
          type: "op"
          op: "count"
          exprs: [compiledExprs[0]]
          modifier: "distinct"
        }

      when "percent"
        # Compiles as count(*) * 100::decimal / sum(count(*)) over()
        return {
          type: "op"
          op: "/"
          exprs: [
            {
              type: "op"
              op: "*"
              exprs: [
                { type: "op", op: "count", exprs: [] }
                { type: "op", op: "::decimal", exprs: [100] }
              ]
            }
            { 
              type: "op"
              op: "sum"
              exprs: [
                { type: "op", op: "count", exprs: [] }
              ]
              over: {}
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

        # Prefer ancestryTable
        if @schema.getTable(idTable).ancestryTable
          # exists (select null from <ancestryTable> as subwithin where ancestor = compiledExprs[1] and descendant = compiledExprs[0])
          return {
            type: "op"
            op: "exists"
            exprs: [
              {
                type: "scalar"
                expr: null
                from: { type: "table", table: @schema.getTable(idTable).ancestryTable, alias: "subwithin" }
                where: {
                  type: "op"
                  op: "and"
                  exprs: [
                    { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "subwithin", column: "ancestor" }, compiledExprs[1]]}
                    { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "subwithin", column: "descendant" }, compiledExprs[0]]}
                  ]
                }
              }
            ]
          }

        return {
          type: "op"
          op: "in"
          exprs: [
            compiledExprs[0]
            {
              type: "scalar"
              expr: @compileColumnRef(@schema.getTable(idTable).primaryKey, "subwithin")
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

      # Hierarchical test that uses ancestry column
      when "within any"
        # Null if either not present
        if not compiledExprs[0] or not compiledExprs[1]
          return null

        # Get table being used
        idTable = exprUtils.getExprIdTable(expr.exprs[0])

        # Prefer ancestryTable
        if @schema.getTable(idTable).ancestryTable
          # exists (select null from <ancestryTable> as subwithin where ancestor = any(compiledExprs[1]) and descendant = compiledExprs[0])
          return {
            type: "op"
            op: "exists"
            exprs: [
              {
                type: "scalar"
                expr: null
                from: { type: "table", table: @schema.getTable(idTable).ancestryTable, alias: "subwithin" }
                where: {
                  type: "op"
                  op: "and"
                  exprs: [
                    { type: "op", op: "=", modifier: "any", exprs: [{ type: "field", tableAlias: "subwithin", column: "ancestor" }, compiledExprs[1]]}
                    { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "subwithin", column: "descendant" }, compiledExprs[0]]}
                  ]
                }
              }
            ]
          }

        # This older code fails now that admin_regions uses integer pk. Replaced with literal-only code
        # return {
        #   type: "op"
        #   op: "in"
        #   exprs: [
        #     compiledExprs[0]
        #     {
        #       type: "scalar"
        #       expr: @compileColumnRef(@schema.getTable(idTable).primaryKey, "subwithin")
        #       from: { type: "table", table: idTable, alias: "subwithin" }
        #       where: {
        #         type: "op"
        #         op: "?|"
        #         exprs: [
        #           { type: "field", tableAlias: "subwithin", column: @schema.getTable(idTable).ancestry }
        #           compiledExprs[1]
        #         ]
        #       }
        #     }            
        #   ]
        # }

        # If not literal, fail
        if compiledExprs[1].type != "literal"
          throw new Error("Non-literal RHS of within any not supported")

        return {
          type: "op"
          op: "in"
          exprs: [
            compiledExprs[0]
            {
              type: "scalar"
              expr: @compileColumnRef(@schema.getTable(idTable).primaryKey, "subwithin")
              from: { type: "table", table: idTable, alias: "subwithin" }
              where: {
                type: "op"
                op: "?|"
                exprs: [
                  { type: "field", tableAlias: "subwithin", column: @schema.getTable(idTable).ancestryText or @schema.getTable(idTable).ancestry }
                  { type: "literal", value: _.map(compiledExprs[1].value, (value) => 
                    if _.isNumber(value)
                      return "" + value
                    else 
                      return value
                    )
                  }
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
            { type: "op", op: "ST_Centroid", exprs: [
              { type: "op", op: "ST_Transform", exprs: [compiledExprs[0], 4326] }
            ] }
          ]
        }

      when "longitude"
        if not compiledExprs[0]
          return null

        return {
          type: "op"
          op: "ST_X"
          exprs: [
            { type: "op", op: "ST_Centroid", exprs: [
              { type: "op", op: "ST_Transform", exprs: [compiledExprs[0], 4326] }
            ] }
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
                    { type: "op", op: "date_part", exprs: ['epoch', { type: "op", op: "::timestamp", exprs: [nowExpr] }] }
                    { type: "op", op: "date_part", exprs: ['epoch', { type: "op", op: "::timestamp", exprs: [compiledExprs[0]] }] }
                  ]
                }
                86400
              ]
            }
          else
            return null

      when 'month'
        if not compiledExprs[0]
          return null

        return {
          type: "op"
          op: "substr"
          exprs: [
            compiledExprs[0]
            6
            2
          ]
        }

      when 'yearmonth'
        if not compiledExprs[0]
          return null

        return {
          type: "op"
          op: "rpad"
          exprs: [
            { type: "op", op: "substr", exprs: [compiledExprs[0], 1, 7] }
            10
            "-01"
          ]
        }

      when 'year'
        if not compiledExprs[0]
          return null

        return {
          type: "op"
          op: "rpad"
          exprs: [
            { type: "op", op: "substr", exprs: [compiledExprs[0], 1, 4] }
            10
            "-01-01"
          ]
        }

      when 'weekofmonth'
        if not compiledExprs[0]
          return null

        return {
          type: "op"
          op: "to_char"
          exprs: [
            { type: "op", op: "::timestamp", exprs: [compiledExprs[0]] }
            "W"
          ]
        }

      when 'dayofmonth'
        if not compiledExprs[0]
          return null

        return {
          type: "op"
          op: "to_char"
          exprs: [
            { type: "op", op: "::timestamp", exprs: [compiledExprs[0]] }
            "DD"
          ]
        }

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

      when 'last24hours'
        if not compiledExprs[0]
          return null

        switch expr0Type
          when "date"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().subtract(1, 'days').format("YYYY-MM-DD") ] }
                { type: "op", op: "<=", exprs: [compiledExprs[0], moment().format("YYYY-MM-DD") ] }
              ]
            }
          when "datetime"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], nowMinus24HoursExpr] }
                { type: "op", op: "<=", exprs: [compiledExprs[0], nowExpr] }
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

      when 'last12months'
        if not compiledExprs[0]
          return null

        switch expr0Type
          when "date"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().subtract(11, "months").startOf('month').format("YYYY-MM-DD") ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD") ] }
              ]
            }
          when "datetime"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().subtract(11, "months").startOf('month').toISOString() ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString() ] }
              ]
            }
          else
            return null
      
      when 'last6months'
        if not compiledExprs[0]
          return null

        switch expr0Type
          when "date"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().subtract(5, "months").startOf('month').format("YYYY-MM-DD") ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD") ] }
              ]
            }
          when "datetime"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().subtract(5, "months").startOf('month').toISOString() ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString() ] }
              ]
            }
          else
            return null

      when 'last3months'
        if not compiledExprs[0]
          return null

        switch expr0Type
          when "date"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().subtract(2, "months").startOf('month').format("YYYY-MM-DD") ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD") ] }
              ]
            }
          when "datetime"
            return { 
              type: "op"
              op: "and"
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().subtract(2, "months").startOf('month').toISOString() ] }
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString() ] }
              ]
            }
          else
            return null

      when 'future'
        if not compiledExprs[0]
          return null

        switch expr0Type
          when "date"
            return { 
              type: "op", 
              op: ">", 
              exprs: [compiledExprs[0], moment().format("YYYY-MM-DD") ] 
            }
          when "datetime"
           return { 
             type: "op", 
             op: ">", 
             exprs: [compiledExprs[0], nowExpr] 
           }
          else
            return null

      when 'notfuture'
        if not compiledExprs[0]
          return null

        switch expr0Type
          when "date"
            return { 
              type: "op", 
              op: "<=", 
              exprs: [compiledExprs[0], moment().format("YYYY-MM-DD") ] 
            }
          when "datetime"
           return { 
             type: "op", 
             op: "<=", 
             exprs: [compiledExprs[0], nowExpr] 
           }
          else
            return null

      when 'distance'
        if not compiledExprs[0] or not compiledExprs[1]
          return null

        return {
          type: "op"
          op: "ST_DistanceSphere"
          exprs: [
            { type: "op", op: "ST_Transform", exprs: [compiledExprs[0], 4326] }
            { type: "op", op: "ST_Transform", exprs: [compiledExprs[1], 4326] }
          ]
        }

      when 'is latest'
        lhsCompiled = @compileExpr(expr: expr.exprs[0], tableAlias: "innerrn")
        if not lhsCompiled
          return null

        filterCompiled = @compileExpr(expr: expr.exprs[1], tableAlias: "innerrn")

        # Get ordering
        ordering = @schema.getTable(expr.table).ordering
        if not ordering
          throw new Error("No ordering defined")

        # order descending
        orderBy = [{ expr: @compileFieldExpr(expr: { type: "field", table: expr.table, column: ordering}, tableAlias: "innerrn"), direction: "desc" }]

        # _id in (select outerrn.id from (select innerrn.id, row_number() over (partition by EXPR1 order by ORDERING desc) as rn from the_table as innerrn where filter) as outerrn where outerrn.rn = 1)

        # Create innerrn query
        innerrnQuery = {
          type: "query"
          selects: [
            { type: "select", expr: @compileExpr(expr: { type: "id", table: expr.table }, tableAlias: "innerrn" ), alias: "id" }
            { 
              type: "select"
              expr: {
                type: "op"
                op: "row_number"
                exprs: []
                over: {
                  partitionBy: [lhsCompiled]
                  orderBy: orderBy
                }
              }
              alias: "rn" 
            }
          ]
          from: { type: "table", table: expr.table, alias: "innerrn" }
        }
        if filterCompiled
          innerrnQuery.where = filterCompiled

        # Wrap in outer query
        outerrnQuery = {
          type: "scalar"
          expr: { type: "field", tableAlias: "outerrn", column: "id" }
          from: {
            type: "subquery"
            query: innerrnQuery
            alias: "outerrn"
          }
          where: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "outerrn", column: "rn" }, 1]}
        }

        return {
          type: "op"
          op: "in"
          exprs: [
            @compileExpr(expr: { type: "id", table: expr.table }, tableAlias: options.tableAlias)
            outerrnQuery
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

  compileBuildEnumsetExpr: (options) ->
    # Create enumset
    # select to_jsonb(array_agg(bes.v)) from (select (case when true then 'x' end) as v union all select (case when true then 'y' end) as v ...) as bes where v is not null

    expr = options.expr

    # Handle empty case
    if _.keys(expr.values).length == 0
      return null

    return {
      type: "scalar"
      expr: {
        type: "op"
        op: "to_jsonb"
        exprs: [
          { 
            type: "op"
            op: "array_agg"
            exprs: [{ type: "field", tableAlias: "bes", column: "v" }]
          }
        ]
      }
      from: {
        type: "subquery"
        alias: "bes"
        query: {
          type: "union all"
          queries: _.map _.pairs(expr.values), (pair) =>
            {
              type: "query"
              selects: [
                { 
                  type: "select"
                  expr: {
                    type: "case"
                    cases: [{ when: @compileExpr(expr: pair[1], tableAlias: options.tableAlias), then: pair[0] }]
                  }
                  alias: "v"
                }
              ]
            }
        }
      }

      where: {
        type: "op"
        op: "is not null"
        exprs: [{ type: "field", tableAlias: "bes", column: "v" }]
      }
    }

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
    if not table
      throw new Error("Table #{tableId} not found")
      
    if not table.jsonql
      return { type: "table", table: tableId, alias: alias }
    else
      return { type: "subquery", query: table.jsonql, alias: alias }

  compileVariableExpr: (options) ->
    # Get variable
    variable = _.findWhere(@variables, id: options.expr.variableId)
    if not variable
      throw new Error("Variable #{options.expr.variableId} not found")

    # Get value
    value = @variableValues[variable.id]

    # If expression, compile
    if variable.table
      return @compileExpr({ expr: value, tableAlias: options.tableAlias })
    else if value?
      return { type: "literal", value: value }
    else
      return null