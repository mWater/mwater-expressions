var ExprUtils, _,
  indexOf = [].indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

_ = require('lodash');

module.exports = ExprUtils = (function() {
  function ExprUtils(schema) {
    var addOpItem, k, l, len, len1, op, ref, relativeDateOp, relativeDateOps;
    this.schema = schema;
    this.opItems = [];
    addOpItem = (function(_this) {
      return function(op, name, resultType, exprTypes, moreExprType) {
        return _this.opItems.push({
          op: op,
          name: name,
          resultType: resultType,
          exprTypes: exprTypes,
          moreExprType: moreExprType
        });
      };
    })(this);
    addOpItem("= any", "is any of", "boolean", ["text", "text[]"]);
    addOpItem("= any", "is any of", "boolean", ["enum", "enumset"]);
    addOpItem("contains", "includes all of", "boolean", ["enumset", "enumset"]);
    relativeDateOps = [['thisyear', 'is this year'], ['lastyear', 'is last year'], ['thismonth', 'is this month'], ['lastmonth', 'is last month'], ['today', 'is today'], ['yesterday', 'is yesterday'], ['last7days', 'is in last 7 days'], ['last30days', 'is in last 30 days'], ['last365days', 'is in last 365 days']];
    for (k = 0, len = relativeDateOps.length; k < len; k++) {
      relativeDateOp = relativeDateOps[k];
      addOpItem(relativeDateOp[0], relativeDateOp[1], "boolean", ['date']);
      addOpItem(relativeDateOp[0], relativeDateOp[1], "boolean", ['datetime']);
    }
    addOpItem("between", "is between", "boolean", ["date", "date", "date"]);
    addOpItem("between", "is between", "boolean", ["datetime", "datetime", "datetime"]);
    addOpItem("=", "is", "boolean", ["number", "number"]);
    addOpItem("=", "is", "boolean", ["text", "text"]);
    addOpItem("=", "is", "boolean", ["enum", "enum"]);
    addOpItem("=", "is", "boolean", ["date", "date"]);
    addOpItem("=", "is", "boolean", ["datetime", "datetime"]);
    addOpItem("=", "is", "boolean", ["boolean", "boolean"]);
    addOpItem("<>", "is not", "boolean", ["text", "text"]);
    addOpItem("<>", "is not", "boolean", ["enum", "enum"]);
    addOpItem("<>", "is not", "boolean", ["date", "date"]);
    addOpItem("<>", "is not", "boolean", ["datetime", "datetime"]);
    addOpItem("<>", "is not", "boolean", ["boolean", "boolean"]);
    addOpItem("<>", "is not", "boolean", ["number", "number"]);
    addOpItem(">", "is greater than", "boolean", ["number", "number"]);
    addOpItem("<", "is less than", "boolean", ["number", "number"]);
    addOpItem(">=", "is greater or equal to", "boolean", ["number", "number"]);
    addOpItem("<=", "is less or equal to", "boolean", ["number", "number"]);
    addOpItem("between", "is between", "boolean", ["number", "number", "number"]);
    addOpItem("and", "and", "boolean", [], "boolean");
    addOpItem("or", "or", "boolean", [], "boolean");
    ref = ['+', '*'];
    for (l = 0, len1 = ref.length; l < len1; l++) {
      op = ref[l];
      addOpItem(op, op, "number", [], "number");
    }
    addOpItem("-", "-", "number", ["number", "number"]);
    addOpItem("/", "/", "number", ["number", "number"]);
    addOpItem("~*", "matches", "boolean", ["text", "text"]);
    addOpItem("not", "is false", "boolean", ["boolean"]);
    addOpItem("is null", "is blank", "boolean", [null]);
    addOpItem("is not null", "is not blank", "boolean", [null]);
  }

  ExprUtils.prototype.findMatchingOpItems = function(search) {
    return _.filter(this.opItems, (function(_this) {
      return function(opItem) {
        var exprType, i, k, len, ref, ref1;
        if (search.resultType) {
          if (_.isArray(search.resultType)) {
            if (ref = opItem.resultType, indexOf.call(search.resultType, ref) < 0) {
              return false;
            }
          } else if (opItem.resultType !== search.resultType) {
            return false;
          }
        }
        if (search.op && opItem.op !== search.op) {
          return false;
        }
        if (search.exprTypes) {
          ref1 = search.exprTypes;
          for (i = k = 0, len = ref1.length; k < len; i = ++k) {
            exprType = ref1[i];
            if (i < opItem.exprTypes.length) {
              if (exprType && opItem.exprTypes[i] && exprType !== opItem.exprTypes[i]) {
                return false;
              }
            } else if (opItem.moreExprType) {
              if (exprType && exprType !== opItem.moreExprType) {
                return false;
              }
            }
          }
        }
        return true;
      };
    })(this));
  };

  ExprUtils.prototype.followJoins = function(startTable, joins) {
    var j, joinCol, k, len, t;
    t = startTable;
    for (k = 0, len = joins.length; k < len; k++) {
      j = joins[k];
      joinCol = this.schema.getColumn(t, j);
      t = joinCol.join.toTable;
    }
    return t;
  };

  ExprUtils.prototype.isMultipleJoins = function(table, joins) {
    var j, joinCol, k, len, ref, t;
    t = table;
    for (k = 0, len = joins.length; k < len; k++) {
      j = joins[k];
      joinCol = this.schema.getColumn(t, j);
      if ((ref = joinCol.join.type) === '1-n' || ref === 'n-n') {
        return true;
      }
      t = joinCol.join.toTable;
    }
    return false;
  };

  ExprUtils.prototype.getExprEnumValues = function(expr) {
    var column;
    if (!expr) {
      return;
    }
    if (expr.type === "field") {
      column = this.schema.getColumn(expr.table, expr.column);
      return column.enumValues;
    }
    if (expr.type === "scalar") {
      if (expr.expr) {
        return this.getExprEnumValues(expr.expr);
      }
    }
  };

  ExprUtils.prototype.getExprType = function(expr) {
    var aggr, column, exprTypes, opItems, resultTypes;
    if ((expr == null) || !expr.type) {
      return null;
    }
    switch (expr.type) {
      case "field":
        column = this.schema.getColumn(expr.table, expr.column);
        if (column) {
          return column.type;
        }
        return null;
      case "id":
        return "id";
      case "scalar":
        if (expr.aggr) {
          aggr = _.findWhere(this.getAggrs(expr.expr), {
            id: expr.aggr
          });
          if (!aggr) {
            return null;
          }
          return aggr.type;
        }
        return this.getExprType(expr.expr);
      case "op":
        opItems = this.findMatchingOpItems({
          op: expr.op
        });
        resultTypes = _.uniq(_.compact(_.pluck(opItems, "resultType")));
        if (resultTypes.length === 1) {
          return resultTypes[0];
        }
        exprTypes = _.map(expr.exprs, (function(_this) {
          return function(e) {
            return _this.getExprType(e);
          };
        })(this));
        opItems = this.findMatchingOpItems({
          op: expr.op,
          exprTypes: exprTypes
        });
        resultTypes = _.uniq(_.compact(_.pluck(opItems, "resultType")));
        if (resultTypes.length === 1) {
          return resultTypes[0];
        }
        return null;
      case "literal":
        return expr.valueType;
      case "case":
        if (expr.cases[0]) {
          return this.getExprType(expr.cases[0].then);
        }
        return this.getExprType(expr["else"]);
      case "count":
        return "count";
      default:
        throw new Error("Not implemented for " + expr.type);
    }
  };

  ExprUtils.prototype.areJoinsValid = function(table, joins) {
    var j, joinCol, k, len, t;
    t = table;
    for (k = 0, len = joins.length; k < len; k++) {
      j = joins[k];
      joinCol = this.schema.getColumn(t, j);
      if (!joinCol) {
        return false;
      }
      t = joinCol.join.toTable;
    }
    return true;
  };

  ExprUtils.prototype.getExprTable = function(expr) {
    if (!expr) {
      return null;
    }
    return expr.table;
  };

  ExprUtils.prototype.getAggrTypes = function(expr) {
    var aggrs;
    aggrs = this.getAggrs(expr);
    return _.uniq(_.pluck(aggrs, "type"));
  };

  ExprUtils.prototype.getAggrs = function(expr) {
    var aggrs, table, type;
    aggrs = [];
    type = this.getExprType(expr);
    if (!type) {
      return [];
    }
    table = this.schema.getTable(expr.table);
    if (table.ordering && (type !== "id" && type !== "count")) {
      aggrs.push({
        id: "last",
        name: "Latest",
        type: type
      });
    }
    switch (type) {
      case "date":
      case "datetime":
        aggrs.push({
          id: "max",
          name: "Maximum",
          type: type
        });
        aggrs.push({
          id: "min",
          name: "Minimum",
          type: type
        });
        break;
      case "number":
      case "integer":
      case "decimal":
        aggrs.push({
          id: "sum",
          name: "Total",
          type: type
        });
        aggrs.push({
          id: "avg",
          name: "Average",
          type: type
        });
        aggrs.push({
          id: "max",
          name: "Maximum",
          type: type
        });
        aggrs.push({
          id: "min",
          name: "Minimum",
          type: type
        });
        break;
      case "id":
      case "count":
        aggrs.push({
          id: "count",
          name: "Number of",
          type: "number"
        });
    }
    return aggrs;
  };

  ExprUtils.prototype.localizeString = function(name, locale) {
    return ExprUtils.localizeString(name, locale);
  };

  ExprUtils.localizeString = function(name, locale) {
    if (!name) {
      return name;
    }
    if (typeof name === "string") {
      return name;
    }
    if (name[locale || "en"]) {
      return name[locale || "en"];
    }
    if (name._base && name[name._base]) {
      return name[name._base];
    }
    return null;
  };

  ExprUtils.prototype.summarizeExpr = function(expr, locale) {
    if (!expr) {
      return "None";
    }
    switch (expr.type) {
      case "scalar":
        return this.summarizeScalarExpr(expr, locale);
      case "field":
        return this.localizeString(this.schema.getColumn(expr.table, expr.column).name, locale);
      case "id":
        return this.localizeString(this.schema.getTable(expr.table).name, locale);
      case "op":
        return _.map(expr.exprs, (function(_this) {
          return function(e) {
            return _this.summarizeExpr(e, locale);
          };
        })(this)).join(" " + expr.op + " ");
      case "literal":
        return expr.value + "";
      default:
        throw new Error("Unsupported type " + expr.type);
    }
  };

  ExprUtils.prototype.summarizeScalarExpr = function(expr, locale) {
    var exprType, join, joinCol, k, len, ref, str, t;
    exprType = this.getExprType(expr.expr);
    if (expr.aggr) {
      str = _.findWhere(this.getAggrs(expr.expr), {
        id: expr.aggr
      }).name + " ";
    } else {
      str = "";
    }
    t = expr.table;
    ref = expr.joins;
    for (k = 0, len = ref.length; k < len; k++) {
      join = ref[k];
      joinCol = this.schema.getColumn(t, join);
      str += this.localizeString(joinCol.name, locale) + " > ";
      t = joinCol.join.toTable;
    }
    if (exprType === "id") {
      str = str.substring(0, str.length - 3);
    } else {
      str += this.summarizeExpr(expr.expr, locale);
    }
    return str;
  };

  ExprUtils.prototype.summarizeAggrExpr = function(expr, aggr, locale) {
    var aggrName, exprType;
    exprType = this.getExprType(expr);
    if (aggr) {
      aggrName = _.findWhere(this.getAggrs(expr), {
        id: aggr
      }).name;
      return aggrName + " " + this.summarizeExpr(expr, locale);
    } else {
      return this.summarizeExpr(expr, locale);
    }
  };

  ExprUtils.prototype.stringifyExprLiteral = function(expr, literal, locale) {
    var enumValues, item, type;
    if (literal == null) {
      return "None";
    }
    type = this.getExprType(expr);
    if (type === 'enum') {
      enumValues = this.getExprEnumValues(expr);
      if (enumValues) {
        item = _.findWhere(enumValues, {
          id: literal
        });
        if (item) {
          return this.localizeString(item.name, locale);
        }
        return "???";
      }
    }
    if (type === "enumset" && _.isArray(literal)) {
      enumValues = this.getExprEnumValues(expr);
      if (enumValues) {
        return _.map(literal, (function(_this) {
          return function(val) {
            item = _.findWhere(enumValues, {
              id: val
            });
            if (item) {
              return _this.localizeString(item.name, locale);
            }
            return "???";
          };
        })(this)).join(',');
      }
    }
    if (literal === true) {
      return "True";
    }
    if (literal === false) {
      return "False";
    }
    return "" + literal;
  };

  ExprUtils.prototype.getComparisonOps = function(lhsType) {
    var ops;
    ops = [];
    switch (lhsType) {
      case "number":
        ops.push({
          id: "=",
          name: "equals"
        });
        ops.push({
          id: ">",
          name: "is greater than"
        });
        ops.push({
          id: ">=",
          name: "is greater or equal to"
        });
        ops.push({
          id: "<",
          name: "is less than"
        });
        ops.push({
          id: "<=",
          name: "is less than or equal to"
        });
        break;
      case "text":
        ops.push({
          id: "= any",
          name: "is one of"
        });
        ops.push({
          id: "=",
          name: "is"
        });
        ops.push({
          id: "~*",
          name: "matches"
        });
        break;
      case "date":
      case "datetime":
        ops.push({
          id: "between",
          name: "between"
        });
        ops.push({
          id: ">",
          name: "after"
        });
        ops.push({
          id: "<",
          name: "before"
        });
        break;
      case "enum":
        ops.push({
          id: "= any",
          name: "is one of"
        });
        ops.push({
          id: "=",
          name: "is"
        });
        break;
      case "boolean":
        ops.push({
          id: "= true",
          name: "is true"
        });
        ops.push({
          id: "= false",
          name: "is false"
        });
    }
    ops.push({
      id: "is null",
      name: "has no value"
    });
    ops.push({
      id: "is not null",
      name: "has a value"
    });
    return ops;
  };

  ExprUtils.prototype.getComparisonRhsType = function(lhsType, op) {
    if (op === '= true' || op === '= false' || op === 'is null' || op === 'is not null') {
      return null;
    }
    if (op === '= any') {
      if (lhsType === "enum") {
        return 'enum[]';
      } else if (lhsType === "text") {
        return "text[]";
      } else {
        throw new Error("Invalid lhs type for op = any");
      }
    }
    if (op === "between") {
      if (lhsType === "date") {
        return 'daterange';
      }
      if (lhsType === "datetime") {
        return 'datetimerange';
      } else {
        throw new Error("Invalid lhs type for op between");
      }
    }
    return lhsType;
  };

  return ExprUtils;

})();
