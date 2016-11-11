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
#  getOrdering(columnId, callback) : gets the ordering of a row if they are ordered. Otherwise, not defined
#
# For joins, getField will get array of rows for 1-n and n-n joins and a row for n-1 and 1-1 joins
module.exports = class ExprEvaluator
  evaluate: (expr, context, callback) ->
    if not expr?
      return callback(null, null)

    switch expr.type
      when "field"
        return context.row.getField(expr.column, callback)
      when "literal"
        return callback(null, expr.value)
      when "op"
        return @evaluateOp(expr.op, expr.exprs, context, callback)
      when "id"
        return row.getPrimaryKey(callback)
      when "case"
        return @evaluateCase(expr, context, callback)
      when "scalar"
        return @evaluateScalar(expr, context, callback)
      when "score"
        return @evaluateScore(expr, context, callback)
      else
        throw new Error("Unsupported expression type #{expr.type}")

  evaluateOp: (op, exprs, context, callback) ->
    # If aggregate op
    if ExprUtils.isOpAggr(op)
      return @evaluteAggrOp(op, exprs, context, callback)
    
    # Evaluate exprs
    async.map exprs, ((expr, cb) => @evaluate(expr, context, cb)), (error, values) =>
      if error
        return callback(error)

      try
        result = @evaluateOpValues(op, values)
        callback(null, result)
      catch error
        return callback(error)        

  # Synchronous evaluation
  evaluateOpValues: (op, values) ->
    # Check if has null argument
    hasNull = _.any(values, (v) -> not v?)

    switch op
      when "+"
        if hasNull
          return null
        return _.reduce(values, (acc, value) -> acc + value)
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
        return values[0] / values[1]
      when "and"
        return _.reduce(values, (acc, value) -> acc and value)
      when "or"
        return _.reduce(values, (acc, value) -> acc or value)
      when "not"
        if hasNull
          return null
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
      when "length"
        if hasNull
          return null
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

      when "days since"
        if hasNull
          return null
        return moment().diff(moment(values[0], moment.ISO_8601))/24/3600/1000

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

      when "to text"
        if hasNull
          return null

        # TODO should localize, but would require schema
        return values[0]

      else
        throw new Error("Unknown op #{op}")

  evaluteAggrOp: (op, exprs, context, callback) ->
    switch op
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

      else      
        callback(new Error("Unknown op #{op}"))

  evaluateCase: (expr, row) ->
    for acase in expr.cases
      if @evaluate(acase.when, row)
        return @evaluate(acase.then, row)

    return @evaluate(expr.else, row)        

  evaluateScalar: (expr, row) ->
    if expr.aggr
      throw new Error("Aggr not supported")

    if expr.joins.length > 1
      throw new Error("Multi-joins not supported")

    # Get inner row
    innerRow = row.getField(expr.joins[0])
    if innerRow
      return @evaluate(expr.expr, innerRow)
    else 
      return null

  evaluateScore: (expr, row) ->
    # Get input value
    if not expr.input
      return null

    sum = 0
    input = @evaluate(expr.input, row)
    if _.isArray(input)
      for val in input
        if expr.scores[val]
          sum += @evaluate(expr.scores[val], row)
    else if input
      if expr.scores[input]
        sum += @evaluate(expr.scores[input], row)

    return sum

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
