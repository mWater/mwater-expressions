
# Thrown when a column is not found when compiling an expression
module.exports = class ColumnNotFoundException extends Error
  constructor: ->
    super