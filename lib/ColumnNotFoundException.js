// Thrown when a column is not found when compiling an expression
var ColumnNotFoundException;

module.exports = ColumnNotFoundException = (function() {
  class ColumnNotFoundException extends Error {
    constructor(message) {
      super(message);
      this.name = this.constructor.name;
      this.message = message;
      this.stack = (new Error(message)).stack;
    }

  };

  ColumnNotFoundException.prototype.constructor = ColumnNotFoundException;

  return ColumnNotFoundException;

}).call(this);
