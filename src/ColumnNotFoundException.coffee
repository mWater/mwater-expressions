
# Thrown when a column is not found when compiling an expression
module.exports = class ColumnNotFoundException extends Error
  constructor: (message) ->
    super(message)
    @name = @constructor.name
    @message = message
    @stack = (new Error(message)).stack

  @::constructor = @
