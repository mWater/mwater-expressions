_ = require 'lodash'
React = require 'react'
H = React.DOM

# Schema for a database. Immutable.
# Stores tables with columns (possibly in nested sections).
# See wiki for details of format
module.exports = class Schema
  constructor: (json) ->
    @tables = []

    # Map of table.id to table
    @tableMap = {} 

    # Map of "<tableid>::<columnid>" to column
    @columnMap = {}

    if json
      @tables = _.cloneDeep(json.tables)

      # Strip id type
      @tables = _.map(@tables, @stripIdColumns)

      @_reindex()

  # Reloads the table and column map after table added/changed
  _reindex: ->
    @tableMap = {}
    @columnMap = {}

    mapContent = (table, item) =>
      # Recurse for sections
      if item.type == "section"
        for item2 in item.contents
          mapContent(table, item2)
      else
        @columnMap["#{table.id}::#{item.id}"] = item

    for table in @tables
      @tableMap[table.id] = table

      for item in table.contents
        mapContent(table, item)

  getTables: -> @tables

  getTable: (tableId) -> @tableMap[tableId]

  getColumn: (tableId, columnId) ->
    return @columnMap["#{tableId}::#{columnId}"]

  # Add table with id, name, desc, primaryKey, ordering (column with natural order) and contents (array of columns/sections)
  # Will replace table if already exists. S
  addTable: (table) ->
    # Remove existing and add new
    tables = _.filter(@tables, (t) -> t.id != table.id)
    tables.push(table)
    return new Schema(tables: tables)

  # TODO readd someday
  getNamedExprs: (tableId) ->
    return []

  # Strip id columns from a table
  stripIdColumns: (table) ->
    stripIdColumnsFromContents = (contents) ->
      output = []
      for item in contents
        if item.type != "section" and item.type != "id"
          output.push(item)
        else if item.type == "section"
          output.push(_.extend(item, { contents: stripIdColumnsFromContents(item.contents)}))

      return output

    return _.extend(table, contents: stripIdColumnsFromContents(table.contents))

