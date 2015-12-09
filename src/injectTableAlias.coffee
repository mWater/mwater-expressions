_ = require 'lodash'
injectTableAliases = require './injectTableAliases'

# Recursively inject table alias tableAlias for `{alias}` 
injectTableAlias = (jsonql, tableAlias) ->
  return injectTableAliases(jsonql, { "{alias}": tableAlias })

module.exports = injectTableAlias
