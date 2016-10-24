var ExprUtils, ExprValidator, _,
  indexOf = [].indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

_ = require('lodash');

ExprUtils = require('./ExprUtils');

module.exports = ExprValidator = (function() {
  function ExprValidator(schema) {
    this.schema = schema;
    this.exprUtils = new ExprUtils(schema);
  }

  ExprValidator.prototype.validateExpr = function(expr, options) {
    var column, cse, enumValueIds, error, i, j, key, len, len1, opItems, ref, ref1, ref2, ref3, ref4, ref5, subexpr, value;
    if (options == null) {
      options = {};
    }
    _.defaults(options, {
      aggrStatuses: ["individual", "literal"]
    });
    if (!expr) {
      return null;
    }
    if (_.isEmpty(expr)) {
      return null;
    }
    if (options.table && expr.table !== options.table) {
      return "Wrong table";
    }
    switch (expr.type) {
      case "literal":
        if (options.types && (ref = expr.valueType, indexOf.call(options.types, ref) < 0)) {
          return "Wrong type";
        }
        if (options.idTable && expr.valueType === "id" && options.idTable !== expr.idTable) {
          return "Wrong table";
        }
        break;
      case "field":
        column = this.schema.getColumn(expr.table, expr.column);
        if (!column) {
          return "Missing column";
        }
        if (column.expr) {
          error = this.validateExpr(column.expr, options);
          if (error) {
            return error;
          }
        }
        break;
      case "op":
        ref1 = expr.exprs;
        for (i = 0, len = ref1.length; i < len; i++) {
          subexpr = ref1[i];
          error = this.validateExpr(subexpr, _.omit(options, "types"));
          if (error) {
            return error;
          }
        }
        opItems = this.exprUtils.findMatchingOpItems({
          op: expr.op,
          lhsExpr: expr.exprs[0],
          resultTypes: options.types
        });
        if (opItems.length === 0) {
          return "No matching op";
        }
        break;
      case "case":
        ref2 = expr.cases;
        for (j = 0, len1 = ref2.length; j < len1; j++) {
          cse = ref2[j];
          error = this.validateExpr(cse.when, _.extend({}, options, {
            types: ["boolean"]
          }));
          if (error) {
            return error;
          }
          error = this.validateExpr(cse.then, options);
          if (error) {
            return error;
          }
        }
        error = this.validateExpr(expr["else"], options);
        if (error) {
          return error;
        }
        break;
      case "score":
        error = this.validateExpr(expr.input, _.extend({}, options, {
          types: ["enum", "enumset"]
        }));
        if (error) {
          return error;
        }
        if (expr.input) {
          enumValueIds = _.pluck(this.exprUtils.getExprEnumValues(expr.input), "id");
        } else {
          enumValueIds = null;
        }
        ref3 = expr.scores;
        for (key in ref3) {
          value = ref3[key];
          if (enumValueIds && indexOf.call(enumValueIds, key) < 0) {
            return "Invalid score enum";
          }
          error = this.validateExpr(value, _.extend({}, options, {
            types: ["number"]
          }));
          if (error) {
            return error;
          }
        }
    }
    if (options.idTable && this.exprUtils.getExprIdTable(expr) && this.exprUtils.getExprIdTable(expr) !== options.idTable) {
      return "Wrong idTable";
    }
    if (options.types && this.exprUtils.getExprType(expr) && (ref4 = this.exprUtils.getExprType(expr), indexOf.call(options.types, ref4) < 0)) {
      return "Invalid type";
    }
    if (options.enumValueIds && ((ref5 = this.exprUtils.getExprType(expr)) === 'enum' || ref5 === 'enumset')) {
      if (_.difference(_.pluck(this.exprUtils.getExprEnumValues(expr), "id"), options.enumValueIds).length > 0) {
        return "Invalid enum";
      }
    }
    return null;
  };

  return ExprValidator;

})();
