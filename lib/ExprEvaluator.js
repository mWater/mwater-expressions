var ExprEvaluator;

module.exports = ExprEvaluator = (function() {
  function ExprEvaluator() {}

  ExprEvaluator.prototype.evaluate = function(expr, row) {
    if (expr == null) {
      return null;
    }
    switch (expr.type) {
      case "field":
        return row.getField(expr.column);
      default:
        throw new Error("Unsupported expression type " + expr.type);
    }
  };

  return ExprEvaluator;

})();
