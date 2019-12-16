_ = require 'lodash'
moment = require 'moment'
async = require 'async'
ExprUtils = require './ExprUtils'

# Evaluates an expression given a context.
#
# context is a plain object that contains:
# row: current row (see below)
# rows: array of rows (for aggregate expressions. See below for row definition)
# 
# a row is a plain object that has the following functions as properties:
#  getPrimaryKey(callback) : gets primary key of row. callback is called with (error, value)
#  getField(columnId, callback) : gets the value of a column. callback is called with (error, value)
#
# For joins, getField will get array of rows for 1-n and n-n joins and a row for n-1 and 1-1 joins
module.exports = class ExprEvaluator
  # Schema is optional and used for ordering, "to text" function and expression columns
  constructor: (schema, locale, variables = [], variableValues = {}) ->
    @schema = schema
    @locale = locale
    @variables = variables
    @variableValues = variableValues

  # Evaluate an expression
  evaluate: (expr, context, callback) ->
    # Handle promise case
    if not callback
      return new Promise((resolve, reject) =>
        @evaluate(expr, context, (error, result) =>
          if error
            reject(error)
          else
            resolve(result)
          )
        )

    if not expr?
      return callback(null, null)

    switch expr.type
      when "field"
        # If schema is present and column is an expression column, use that
        if @schema and @schema.getColumn(expr.table, expr.column)?.expr
          return @evaluate(@schema.getColumn(expr.table, expr.column).expr, context, callback)

        context.row.getField(expr.column, (error, value) =>
          if error
            return callback(error)

          # Handle row case
          if value and value.getPrimaryKey
            return value.getPrimaryKey(callback)

          # Handle rows case
          if value and _.isArray(value) and value.length > 0 and value[0].getPrimaryKey
            # Map to id
            async.map value, ((item, cb) => item.getPrimaryKey(cb)), (error, ids) =>
              callback(error, ids)
            return

          callback(null, value)
          )
      when "literal"
        callback(null, expr.value)
      when "op"
        @evaluateOp(expr.table, expr.op, expr.exprs, context, callback)
      when "id"
        context.row.getPrimaryKey(callback)
      when "case"
        @evaluateCase(expr, context, callback)
      when "scalar"
        @evaluateScalar(expr, context, callback)
      when "score"
        @evaluateScore(expr, context, callback)
      when "build enumset"
        @evaluateBuildEnumset(expr, context, callback)
      when "variable"
        @evaluateVariable(expr, context, callback)
      else
        throw new Error("Unsupported expression type #{expr.type}")

  evaluateOp: (table, op, exprs, context, callback) ->
    # If aggregate op
    if ExprUtils.isOpAggr(op)
      @evaluteAggrOp(table, op, exprs, context, callback)
      return

    # is latest is special case for window-like function
    if op == "is latest"
      @evaluateIsLatest(table, exprs, context, callback)
      return
    
    # Evaluate exprs
    async.map exprs, ((expr, cb) => @evaluate(expr, context, cb)), (error, values) =>
      if error
        return callback(error)

      try
        result = @evaluateOpValues(op, exprs, values)
        callback(null, result)
      catch error
        return callback(error)        

  # Synchronous evaluation
  evaluateOpValues: (op, exprs, values) ->
    # Check if has null argument
    hasNull = _.any(values, (v) -> not v?)

    switch op
      when "+"
        return _.reduce(values, (acc, value) -> acc + (if value? then value else 0))
      when "*"
        if hasNull
          return null
        return _.reduce(values, (acc, value) -> acc * value)
      when "-"
        if hasNull
          return null
        return values[0] - values[1]
      when "/"
        if hasNull
          return null
        if values[1] == 0
          return null
        return values[0] / values[1]
      when "and"
        if values.length == 0
          return null
        return _.reduce(values, (acc, value) -> acc and value)
      when "or"
        if values.length == 0
          return null
        return _.reduce(values, (acc, value) -> acc or value)
      when "not"
        if hasNull
          return true
        return not values[0]
      when "="
        if hasNull
          return null
        return values[0] == values[1]
      when "<>"
        if hasNull
          return null
        return values[0] != values[1]
      when ">"
        if hasNull
          return null
        return values[0] > values[1]
      when ">="
        if hasNull
          return null
        return values[0] >= values[1]
      when "<"
        if hasNull
          return null
        return values[0] < values[1]
      when "<="
        if hasNull
          return null
        return values[0] <= values[1]
      when "= false"
        if hasNull
          return null
        return values[0] == false
      when "is null"
        return not values[0]?
      when "is not null"
        return values[0]?
      when "~*"
        if hasNull
          return null
        return values[0].match(new RegExp(values[1], "i"))?
      when "= any"
        if hasNull
          return null
        return _.contains(values[1], values[0])
      when "contains"
        if hasNull
          return null
        return _.difference(values[1], values[0]).length == 0
      when "intersects"
        if hasNull
          return null
        return _.intersection(values[0], values[1]).length > 0
      when "length"
        if hasNull
          return 0
        return values[0].length
      when "between"
        if hasNull
          return null
        return values[0] >= values[1] and values[0] <= values[2]
      when "round"
        if hasNull
          return null
        return Math.round(values[0])
      when "floor"
        if hasNull
          return null
        return Math.floor(values[0])
      when "ceiling"
        if hasNull
          return null
        return Math.ceil(values[0])

      when "days difference"
        if hasNull
          return null
        return moment(values[0], moment.ISO_8601).diff(moment(values[1], moment.ISO_8601))/24/3600/1000

      when "months difference"
        if hasNull
          return null
        return moment(values[0], moment.ISO_8601).diff(moment(values[1], moment.ISO_8601))/24/3600/1000/30.5

      when "years difference"
        if hasNull
          return null
        return moment(values[0], moment.ISO_8601).diff(moment(values[1], moment.ISO_8601))/24/3600/1000/365

      when "days since"
        if hasNull
          return null
        return moment().diff(moment(values[0], moment.ISO_8601))/24/3600/1000

      when "weekofmonth"
        if hasNull
          return null
        return (Math.floor((moment(values[0], moment.ISO_8601).date() - 1) / 7) + 1) + "" # Make string

      when "dayofmonth"
        if hasNull
          return null
        return moment(values[0], moment.ISO_8601).format("DD")

      when "month"
        if hasNull
          return null
        return values[0].substr(5, 2)

      when "yearmonth"
        if hasNull
          return null
        return values[0].substr(0, 7) + "-01"

      when "year"
        if hasNull
          return null
        return values[0].substr(0, 4) + "-01-01"

      when "today"
        if hasNull
          return null
        date = values[0]
        return moment(date, moment.ISO_8601).format("YYYY-MM-DD") == moment().format("YYYY-MM-DD")

      when "yesterday"
        if hasNull
          return null
        date = values[0]
        return moment(date, moment.ISO_8601).add(1, "days").format("YYYY-MM-DD") == moment().format("YYYY-MM-DD")

      when "thismonth"
        if hasNull
          return null
        date = values[0]
        return moment(date, moment.ISO_8601).format("YYYY-MM") == moment().format("YYYY-MM")

      when "lastmonth"
        if hasNull
          return null
        date = values[0]
        return moment(date, moment.ISO_8601).add(1, "months").format("YYYY-MM") == moment().format("YYYY-MM")

      when "thisyear"
        if hasNull
          return null
        date = values[0]
        return moment(date, moment.ISO_8601).format("YYYY") == moment().format("YYYY")

      when "lastyear"
        if hasNull
          return null
        date = values[0]
        return moment(date, moment.ISO_8601).add(1, "years").format("YYYY") == moment().format("YYYY")

      when "last24hours"
        if hasNull
          return null
        date = values[0]
        return moment(date, moment.ISO_8601).isSameOrBefore(moment()) and moment(date, moment.ISO_8601).isAfter(moment().subtract(24, "hours")) 

      when "last7days"
        if hasNull
          return null
        date = values[0]
        return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) and moment(date, moment.ISO_8601).isAfter(moment().subtract(7, "days")) 

      when "last30days"
        if hasNull
          return null
        date = values[0]
        return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) and moment(date, moment.ISO_8601).isAfter(moment().subtract(30, "days")) 

      when "last365days"
        if hasNull
          return null
        date = values[0]
        return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) and moment(date, moment.ISO_8601).isAfter(moment().subtract(365, "days")) 

      when "last12months"
        if hasNull
          return null
        date = values[0]
        return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) and moment(date, moment.ISO_8601).isAfter(moment().subtract(11, "months").startOf('month')) 

      when "last6months"
        if hasNull
          return null
        date = values[0]
        return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) and moment(date, moment.ISO_8601).isAfter(moment().subtract(5, "months").startOf('month')) 

      when "last3months"
        if hasNull
          return null
        date = values[0]
        return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) and moment(date, moment.ISO_8601).isAfter(moment().subtract(2, "months").startOf('month')) 

      when "future"
        if hasNull
          return null
        date = values[0]

        return moment(date, moment.ISO_8601).isAfter(moment())

      when "notfuture"
        if hasNull
          return null
        date = values[0]
        
        return not moment(date, moment.ISO_8601).isAfter(moment())

      when "current date"
        return moment().format("YYYY-MM-DD")

      when "current datetime"
        return moment().toISOString()

      when "latitude"
        if hasNull
          return null

        point = values[0]
        if point?.type == "Point"
          return point.coordinates[1]

      when "longitude"
        if hasNull
          return null

        point = values[0]
        if point?.type == "Point"
          return point.coordinates[0]

      when "distance"
        if hasNull
          return null

        point1 = values[0]
        point2 = values[1]
        if point1?.type == "Point" and point2?.type == "Point"
          return getDistanceFromLatLngInM(point1.coordinates[1], point1.coordinates[0], point2.coordinates[1], point2.coordinates[0])

      when "line length"
        if hasNull
          return null

        if values[0].type != "LineString"
          return 0

        total = 0
        coords = values[0].coordinates
        for i in [0...coords.length - 1]
          total += getDistanceFromLatLngInM(coords[i][1], coords[i][0], coords[i + 1][1], coords[i + 1][0])

        return total

      when "to text"
        if hasNull
          return null

        if @schema
          exprUtils = new ExprUtils(@schema)
          return exprUtils.stringifyExprLiteral(exprs[0], values[0], @locale)
        else
          return values[0] + ""

      else
        throw new Error("Unknown op #{op}")

  evaluteAggrOp: (table, op, exprs, context, callback) ->
    switch op
      when "count"
        callback(null, context.rows.length)

      when "sum"
        # Evaluate all rows
        async.map context.rows, ((row, cb) => @evaluate(exprs[0], { row: row }, cb)), (error, values) =>
          if error
            return callback(error)

          callback(null, _.sum(values))

      when "avg"
        # Evaluate all rows
        async.map context.rows, ((row, cb) => @evaluate(exprs[0], { row: row }, cb)), (error, values) =>
          if error
            return callback(error)

          callback(null, _.sum(values)/values.length)

      # TODO. Uses window functions, so returning 100 for now
      when "percent"
        callback(null, 100)

      when "min"
        # Evaluate all rows
        async.map context.rows, ((row, cb) => @evaluate(exprs[0], { row: row }, cb)), (error, values) =>
          if error
            return callback(error)

          callback(null, _.min(values))

      when "max"
        # Evaluate all rows
        async.map context.rows, ((row, cb) => @evaluate(exprs[0], { row: row }, cb)), (error, values) =>
          if error
            return callback(error)

          callback(null, _.max(values))

      when "last"
        # Fail quietly if no ordering or no schema
        if not @schema or not @schema.getTable(table).ordering
          console.warn("last does not work without schema and ordering")
          return callback(null, null)

        # Evaluate all rows by ordering
        async.map context.rows, ((row, cb) => row.getField(@schema.getTable(table).ordering, cb)), (error, orderValues) =>
          if error
            return callback(error)

          # Evaluate all rows
          async.map context.rows, ((row, cb) => @evaluate(exprs[0], { row: row }, cb)), (error, values) =>
            if error
              return callback(error)

            zipped = _.zip(values, orderValues)

            # Sort by ordering reverse
            zipped = _.sortByOrder(zipped, [(entry) => entry[1]], ["desc"])
            values = _.map(zipped, (entry) -> entry[0])

            # Take first non-null
            for value in values
              if value?
                callback(null, value)
                return

            callback(null, null)

      when "last where"
        # Fail quietly if no ordering or no schema
        if not @schema or not @schema.getTable(table).ordering
          console.warn("last where does not work without schema and ordering")
          return callback(null, null)

        # Evaluate all rows by ordering
        async.map context.rows, ((row, cb) => row.getField(@schema.getTable(table).ordering, cb)), (error, orderValues) =>
          if error
            return callback(error)
  
          # Evaluate all rows by where
          async.map context.rows, ((row, cb) => @evaluate(exprs[1], { row: row }, cb)), (error, wheres) =>
            if error
              return callback(error)

            # Evaluate all rows
            async.map context.rows, ((row, cb) => @evaluate(exprs[0], { row: row }, cb)), (error, values) =>
              if error
                return callback(error)

              # Find largest
              if orderValues.length == 0
                return callback(null, null)

              index = -1
              largest = null
              for row, i in context.rows
                if (wheres[i] or not exprs[1]) and (index == -1 or orderValues[i] > largest) and values[i]?
                  index = i
                  largest = orderValues[i]
    
              if index >= 0
                callback(null, values[index])
              else
                callback(null, null)

      when "previous"
        # Fail quietly if no ordering or no schema
        if not @schema or not @schema.getTable(table).ordering
          console.warn("last does not work without schema and ordering")
          return callback(null, null)

        # Evaluate all rows by ordering
        async.map context.rows, ((row, cb) => row.getField(@schema.getTable(table).ordering, cb)), (error, orderValues) =>
          if error
            return callback(error)

          # Evaluate all rows
          async.map context.rows, ((row, cb) => @evaluate(exprs[0], { row: row }, cb)), (error, values) =>
            if error
              return callback(error)

            zipped = _.zip(values, orderValues)

            # Sort by ordering reverse
            zipped = _.sortByOrder(zipped, [(entry) => entry[1]], ["desc"])
            values = _.map(zipped, (entry) -> entry[0])

            # Take second non-null
            values = _.filter(values, (v) => v?)
            if values[1]?
              callback(null, values[1])
              return

            callback(null, null)

      when "count where"
        # Evaluate all rows by where
        async.map context.rows, ((row, cb) => @evaluate(exprs[0], { row: row }, cb)), (error, wheres) =>
          if error
            return callback(error)

          count = 0
          for where in wheres
            if where == true
              count += 1

          return callback(null, count)

      when "sum where"
        # Evaluate all rows
        async.map context.rows, ((row, cb) => @evaluate(exprs[0], { row: row }, cb)), (error, values) =>
          if error
            return callback(error)

          # Evaluate all rows by where
          async.map context.rows, ((row, cb) => @evaluate(exprs[1], { row: row }, cb)), (error, wheres) =>
            if error
              return callback(error)

            sum = 0
            for row, i in context.rows
              if wheres[i] == true
                sum += values[i]

            callback(null, sum)

      when "percent where"
        # Evaluate all rows
        async.map context.rows, ((row, cb) => @evaluate(exprs[0], { row: row }, cb)), (error, wheres) =>
          if error
            return callback(error)

          # Evaluate all rows by of
          async.map context.rows, ((row, cb) => @evaluate(exprs[1], { row: row }, cb)), (error, ofs) =>
            if error
              return callback(error)

            sum = 0
            count = 0
            for row, i in context.rows
              if wheres[i] == true and (not exprs[1] or ofs[i] == true)
                sum += 1
              if not exprs[1] or ofs[i] == true
                count += 1

            if count == 0
              callback(null, null)
            else
              callback(null, sum/count * 100)

      when "min where"
        # Evaluate all rows
        async.map context.rows, ((row, cb) => @evaluate(exprs[0], { row: row }, cb)), (error, values) =>
          if error
            return callback(error)

          # Evaluate all rows by where
          async.map context.rows, ((row, cb) => @evaluate(exprs[1], { row: row }, cb)), (error, wheres) =>
            if error
              return callback(error)

            items = []
            for row, i in context.rows
              if wheres[i] == true
                items.push(values[i])
            value = _.min(items)

            callback(null, if value? then value else null)

      when "max where"
        # Evaluate all rows
        async.map context.rows, ((row, cb) => @evaluate(exprs[0], { row: row }, cb)), (error, values) =>
          if error
            return callback(error)

          # Evaluate all rows by where
          async.map context.rows, ((row, cb) => @evaluate(exprs[1], { row: row }, cb)), (error, wheres) =>
            if error
              return callback(error)

            items = []
            for row, i in context.rows
              if wheres[i] == true
                items.push(values[i])
            value = _.max(items)

            callback(null, if value? then value else null)

      when "count distinct"
        # Evaluate all rows 
        async.map context.rows, ((row, cb) => @evaluate(exprs[0], { row: row }, cb)), (error, values) =>
          if error
            return callback(error)

          count = _.uniq(values).length

          return callback(null, count)

      when "array_agg"
        # Evaluate all rows
        async.map context.rows, ((row, cb) => @evaluate(exprs[0], { row: row }, cb)), (error, values) =>
          if error
            return callback(error)

          callback(null, values)


      else      
        callback(new Error("Unknown op #{op}"))

  evaluateCase: (expr, context, callback) ->
    # Evaluate case whens and thens
    async.map expr.cases, ((acase, cb) => @evaluate(acase.when, context, cb)), (error, whens) =>
      if error
        return callback(error)

      async.map expr.cases, ((acase, cb) => @evaluate(acase.then, context, cb)), (error, thens) =>
        if error
          return callback(error)

        @evaluate expr.else, context, (error, aelse) =>
          if error
            return callback(error)

          for acase, i in expr.cases
            if whens[i]
              return callback(null, thens[i])

          return callback(null, aelse)

  evaluateScalar: (expr, context, callback) ->
    # Null expression is null
    if not expr.expr
      return callback(null, null)

    # Follow joins
    async.reduce expr.joins, { row: context.row }, (memo, join, cb) =>
      # Memo is the context to apply the join to 
      # If multiple rows, get join for each and flatten
      if memo.rows
        async.map memo.rows, ((row, cb2) => row.getField(join, cb2)), (error, results) =>
          if error
            return cb(error)

          if results
            cb(null, { rows: _.compact(_.flatten(results)) })
      else if memo.row
        # Single row
        memo.row.getField(join, (error, result) =>
          if error
            return cb(error)

          if _.isArray(result)
            cb(null, { rows: _.compact(result) })
          else
            cb(null, { row: result })
        )
      else
        cb(null, { row: null })
    , (error, exprContext) =>
      if error
        return callback(error)

      # Null row and null/empty rows is null
      if not exprContext.row? and (not exprContext.rows? or exprContext.rows.length == 0)
        return callback(null, null)
      
      @evaluate(expr.expr, exprContext, callback)

  evaluateScore: (expr, context, callback) ->
    # Get input value
    if not expr.input
      return callback(null, null)

    sum = 0
    @evaluate(expr.input, context, (error, input) =>
      if error
        return callback(error)

      scorePairs = _.pairs(expr.scores)
      async.map scorePairs, ((scorePair, cb) => @evaluate(scorePair[1], context, cb)), (error, values) =>
        if error
          return callback(error)

        scoreValues = {}
        for scorePair, i in scorePairs
          scoreValues[scorePair[0]] = values[i]

        if _.isArray(input)
          for val in input
            if scoreValues[val]
              sum += scoreValues[val]
        else if input
          if scoreValues[input]
            sum += scoreValues[input]

        callback(null, sum)
    )

  evaluateBuildEnumset: (expr, context, callback) ->
    # Evaluate each boolean
    valuePairs = _.pairs(expr.values)
    async.map valuePairs, ((valuePair, cb) => @evaluate(valuePair[1], context, cb)), (error, values) =>
      if error
        return callback(error)

      result = []
      for valuePair, i in valuePairs
        if values[i]
          result.push(valuePair[0])

      callback(null, result)

  # NOTE: This is not technically correct. It's not a window function (as window
  # functions can't be used in where clauses) but rather a special query
  evaluateIsLatest: (table, exprs, context, callback) ->
    # Fail quietly if no ordering or no schema
    if not @schema or not @schema.getTable(table).ordering
      console.warn("evaluateIsLatest does not work without schema and ordering")
      return callback(null, false)

    # Fail quietly if no rows
    if not context.rows
      console.warn("evaluateIsLatest does not work without rows context")
      return callback(null, false)

    # Evaluate lhs (value to group by) for all rows
    async.map context.rows, ((row, cb) => @evaluate(exprs[0], { row: row }, cb)), (error, lhss) =>
      if error
        return callback(error)

      # Evaluate pk for all rows
      async.map context.rows, ((row, cb) => row.getPrimaryKey(cb)), (error, pks) =>
        if error
          return callback(error)

        # Evaluate ordering for all rows
        async.map context.rows, ((row, cb) => row.getField(@schema.getTable(table).ordering, cb)), (error, orderValues) =>
          if error
            return callback(error)

          # Evaluate filter value for all rows if present
          async.map context.rows, ((row, cb) => @evaluate(exprs[1], { row: row }, cb)), (error, filters) =>
            if error
              return callback(error)

            items = _.map lhss, (lhs, index) => { lhs: lhs, pk: pks[index], ordering: orderValues[index], filter: filters[index] }

            # Filter
            if exprs[1]
              items = _.filter(items, (item) -> item.filter)

            # Group by lhs
            groups = _.groupBy(items, "lhs")

            # Keep latest of each group
            latests = []
            for lhs, items of groups
              latests.push(_.max(items, "ordering"))

            # Get pk of row
            context.row.getPrimaryKey (error, pk) =>
              if error
                return callback(error)

              # See if match
              callback(null, pk in _.pluck(latests, "pk"))

  evaluateVariable: (expr, context, callback) ->
    # Get variable
    variable = _.findWhere(@variables, id: expr.variableId)
    if not variable
      throw new Error("Variable #{expr.variableId} not found")

    # Get value
    value = @variableValues[variable.id]
    if value == undefined
      throw new Error("Variable #{expr.variableId} has no value")

    # If expression, compile
    if variable.table
      return @evaluate(value, context, callback)
    else 
      callback(null, value)

# From http://www.movable-type.co.uk/scripts/latlong.html
getDistanceFromLatLngInM = (lat1, lng1, lat2, lng2) ->
  R = 6370986 # Radius of the earth in m
  dLat = deg2rad(lat2 - lat1) # deg2rad below
  dLng = deg2rad(lng2 - lng1)
  a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.sin(dLng / 2) * Math.sin(dLng / 2)
  c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
  d = R * c # Distance in m
  return d

deg2rad = (deg) ->
  deg * (Math.PI / 180)
