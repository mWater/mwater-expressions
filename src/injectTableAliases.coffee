_ = require 'lodash'

# Recursively inject table aliases 
# aliases is map of replacement to table aliases. For example, { "{a}": b } will replace "{a}" with "b"
injectTableAliases = (jsonql, aliases) ->
  # Handle empty
  if not jsonql
    return jsonql

  # Handle arrays
  if _.isArray(jsonql)
    return _.map(jsonql, (item) => injectTableAliases(item, aliases))

  # Handle non-objects by leaving alone
  if not _.isObject(jsonql)
    return jsonql

  # Handle field
  if jsonql.type == "field" and aliases[jsonql.tableAlias]
    return _.extend({}, jsonql, tableAlias: aliases[jsonql.tableAlias])

  # Recurse object keys
  return _.mapValues(jsonql, (value) => injectTableAliases(value, aliases))

module.exports = injectTableAliases
