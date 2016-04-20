_ = require 'lodash'
moment = require 'moment'

# Evaluates an expression given a row
# row is a plain object that has the following functions as properties:
#  getPrimaryKey() : returns primary key of row
#  getField(columnId) : gets the value of a column
# For joins, getField will return array of rows for 1-n and n-n joins and a row for n-1 and 1-1 joins
module.exports = class ExprEvaluator
  evaluate: (expr, row) ->
    if not expr?
      return null

    switch expr.type
      when "field"
        return row.getField(expr.column)
      when "literal"
        return expr.value
      when "op"
        return @evaluateOp(expr.op, expr.exprs, row)
      when "id"
        return row.getPrimaryKey()
      when "case"
        return @evaluateCase(expr, row)
      when "scalar"
        return @evaluateScalar(expr, row)
      else
        throw new Error("Unsupported expression type #{expr.type}")

  evaluateOp: (op, exprs, row) ->
    switch op
      when "and"
        return @evaluate(exprs[0], row) and @evaluate(exprs[1], row) 
      when "or"
        return @evaluate(exprs[0], row) or @evaluate(exprs[1], row) 
      when "="
        return @evaluate(exprs[0], row) == @evaluate(exprs[1], row) 
      when "<>"
        return @evaluate(exprs[0], row) != @evaluate(exprs[1], row) 
      when ">"
        return @evaluate(exprs[0], row) > @evaluate(exprs[1], row) 
      when ">="
        return @evaluate(exprs[0], row) >= @evaluate(exprs[1], row) 
      when "<"
        return @evaluate(exprs[0], row) < @evaluate(exprs[1], row) 
      when "<="
        return @evaluate(exprs[0], row) <= @evaluate(exprs[1], row) 
      when "= false"
        return @evaluate(exprs[0], row) == false
      when "is null"
        return not @evaluate(exprs[0], row)?
      when "is not null"
        return @evaluate(exprs[0], row)?
      when "~*"
        return @evaluate(exprs[0], row).match(new RegExp(@evaluate(exprs[1], row), "i"))?
      when "= any"
        return _.contains(@evaluate(exprs[1], row), @evaluate(exprs[0], row))
      when "between"
        return @evaluate(exprs[0], row) >= @evaluate(exprs[1], row) and @evaluate(exprs[0], row) <= @evaluate(exprs[2], row)

      when "today"
        date = @evaluate(exprs[0], row)
        if not date
          return false
        return moment(date, moment.ISO_8601).format("YYYY-MM-DD") == moment().format("YYYY-MM-DD")

      when "yesterday"
        date = @evaluate(exprs[0], row)
        if not date
          return false
        return moment(date, moment.ISO_8601).add(1, "days").format("YYYY-MM-DD") == moment().format("YYYY-MM-DD")

      when "thismonth"
        date = @evaluate(exprs[0], row)
        if not date
          return false
        return moment(date, moment.ISO_8601).format("YYYY-MM") == moment().format("YYYY-MM")

      when "lastmonth"
        date = @evaluate(exprs[0], row)
        if not date
          return false
        return moment(date, moment.ISO_8601).add(1, "months").format("YYYY-MM") == moment().format("YYYY-MM")

      when "thisyear"
        date = @evaluate(exprs[0], row)
        if not date
          return false
        return moment(date, moment.ISO_8601).format("YYYY") == moment().format("YYYY")

      when "lastyear"
        date = @evaluate(exprs[0], row)
        if not date
          return false
        return moment(date, moment.ISO_8601).add(1, "years").format("YYYY") == moment().format("YYYY")

      when "last7days"
        date = @evaluate(exprs[0], row)
        if not date
          return false
        return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) and moment(date, moment.ISO_8601).isAfter(moment().subtract(7, "days")) 

      when "last30days"
        date = @evaluate(exprs[0], row)
        if not date
          return false
        return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) and moment(date, moment.ISO_8601).isAfter(moment().subtract(30, "days")) 

      when "last365days"
        date = @evaluate(exprs[0], row)
        if not date
          return false
        return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) and moment(date, moment.ISO_8601).isAfter(moment().subtract(365, "days")) 

      when "latitude"
        point = @evaluate(exprs[0], row)
        if point?.type == "Point"
          return point.coordinates[1]

      when "longitude"
        point = @evaluate(exprs[0], row)
        if point?.type == "Point"
          return point.coordinates[0]

      when "distance"
        point1 = @evaluate(exprs[0], row)
        point2 = @evaluate(exprs[1], row)
        if point1?.type == "Point" and point2?.type == "Point"
          return getDistanceFromLatLngInM(point1.coordinates[1], point1.coordinates[0], point2.coordinates[1], point2.coordinates[0])

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
