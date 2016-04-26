var ExprUtils, _,
  indexOf = [].indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

_ = require('lodash');

module.exports = ExprUtils = (function() {
  function ExprUtils(schema) {
    var addOpItem, i, k, len, len1, op, ref, relativeDateOp, relativeDateOps;
    this.schema = schema;
    this.opItems = [];
    addOpItem = (function(_this) {
      return function(item) {
        return _this.opItems.push(_.defaults(item, {
          prefix: false,
          rhsLiteral: true
        }));
      };
    })(this);
    addOpItem({
      op: "= any",
      name: "is any of",
      resultType: "boolean",
      exprTypes: ["text", "text[]"]
    });
    addOpItem({
      op: "= any",
      name: "is any of",
      resultType: "boolean",
      exprTypes: ["enum", "enumset"]
    });
    addOpItem({
      op: "contains",
      name: "includes all of",
      resultType: "boolean",
      exprTypes: ["enumset", "enumset"]
    });
    relativeDateOps = [['thisyear', 'is this year'], ['lastyear', 'is last year'], ['thismonth', 'is this month'], ['lastmonth', 'is last month'], ['today', 'is today'], ['yesterday', 'is yesterday'], ['last7days', 'is in last 7 days'], ['last30days', 'is in last 30 days'], ['last365days', 'is in last 365 days']];
    for (i = 0, len = relativeDateOps.length; i < len; i++) {
      relativeDateOp = relativeDateOps[i];
      addOpItem({
        op: relativeDateOp[0],
        name: relativeDateOp[1],
        resultType: "boolean",
        exprTypes: ['date']
      });
      addOpItem({
        op: relativeDateOp[0],
        name: relativeDateOp[1],
        resultType: "boolean",
        exprTypes: ['datetime']
      });
    }
    addOpItem({
      op: "between",
      name: "is between",
      resultType: "boolean",
      exprTypes: ["date", "date", "date"]
    });
    addOpItem({
      op: "between",
      name: "is between",
      resultType: "boolean",
      exprTypes: ["datetime", "datetime", "datetime"]
    });
    addOpItem({
      op: "=",
      name: "is",
      resultType: "boolean",
      exprTypes: ["number", "number"]
    });
    addOpItem({
      op: "=",
      name: "is",
      resultType: "boolean",
      exprTypes: ["text", "text"]
    });
    addOpItem({
      op: "=",
      name: "is",
      resultType: "boolean",
      exprTypes: ["enum", "enum"]
    });
    addOpItem({
      op: "=",
      name: "is",
      resultType: "boolean",
      exprTypes: ["date", "date"]
    });
    addOpItem({
      op: "=",
      name: "is",
      resultType: "boolean",
      exprTypes: ["datetime", "datetime"]
    });
    addOpItem({
      op: "=",
      name: "is",
      resultType: "boolean",
      exprTypes: ["boolean", "boolean"]
    });
    addOpItem({
      op: "<>",
      name: "is not",
      resultType: "boolean",
      exprTypes: ["text", "text"]
    });
    addOpItem({
      op: "<>",
      name: "is not",
      resultType: "boolean",
      exprTypes: ["enum", "enum"]
    });
    addOpItem({
      op: "<>",
      name: "is not",
      resultType: "boolean",
      exprTypes: ["date", "date"]
    });
    addOpItem({
      op: "<>",
      name: "is not",
      resultType: "boolean",
      exprTypes: ["datetime", "datetime"]
    });
    addOpItem({
      op: "<>",
      name: "is not",
      resultType: "boolean",
      exprTypes: ["boolean", "boolean"]
    });
    addOpItem({
      op: "<>",
      name: "is not",
      resultType: "boolean",
      exprTypes: ["number", "number"]
    });
    addOpItem({
      op: ">",
      name: "is greater than",
      resultType: "boolean",
      exprTypes: ["number", "number"]
    });
    addOpItem({
      op: "<",
      name: "is less than",
      resultType: "boolean",
      exprTypes: ["number", "number"]
    });
    addOpItem({
      op: ">=",
      name: "is greater or equal to",
      resultType: "boolean",
      exprTypes: ["number", "number"]
    });
    addOpItem({
      op: "<=",
      name: "is less or equal to",
      resultType: "boolean",
      exprTypes: ["number", "number"]
    });
    addOpItem({
      op: "between",
      name: "is between",
      resultType: "boolean",
      exprTypes: ["number", "number", "number"]
    });
    addOpItem({
      op: "round",
      name: "round",
      resultType: "number",
      exprTypes: ["number"],
      prefix: true
    });
    addOpItem({
      op: "floor",
      name: "floor",
      resultType: "number",
      exprTypes: ["number"],
      prefix: true
    });
    addOpItem({
      op: "ceiling",
      name: "ceiling",
      resultType: "number",
      exprTypes: ["number"],
      prefix: true
    });
    addOpItem({
      op: "latitude",
      name: "latitude of",
      resultType: "number",
      exprTypes: ["geometry"],
      prefix: true
    });
    addOpItem({
      op: "longitude",
      name: "longitude of",
      resultType: "number",
      exprTypes: ["geometry"],
      prefix: true
    });
    addOpItem({
      op: "distance",
      name: "distance between",
      resultType: "number",
      exprTypes: ["geometry", "geometry"],
      prefix: true,
      rhsLiteral: false,
      joiner: "and"
    });
    addOpItem({
      op: "and",
      name: "and",
      resultType: "boolean",
      exprTypes: [],
      moreExprType: "boolean"
    });
    addOpItem({
      op: "or",
      name: "or",
      resultType: "boolean",
      exprTypes: [],
      moreExprType: "boolean"
    });
    ref = ['+', '*'];
    for (k = 0, len1 = ref.length; k < len1; k++) {
      op = ref[k];
      addOpItem({
        op: op,
        name: op,
        resultType: "number",
        exprTypes: [],
        moreExprType: "number"
      });
    }
    addOpItem({
      op: "-",
      name: "-",
      resultType: "number",
      exprTypes: ["number", "number"]
    });
    addOpItem({
      op: "/",
      name: "/",
      resultType: "number",
      exprTypes: ["number", "number"]
    });
    addOpItem({
      op: "within",
      name: "in",
      resultType: "boolean",
      exprTypes: ["id", "id"],
      lhsCond: (function(_this) {
        return function(lhsExpr) {
          var lhsIdTable;
          lhsIdTable = _this.getExprIdTable(lhsExpr);
          if (lhsIdTable) {
            return _this.schema.getTable(lhsIdTable).ancestry != null;
          }
          return false;
        };
      })(this)
    });
    addOpItem({
      op: "=",
      name: "is",
      resultType: "boolean",
      exprTypes: ["id", "id"]
    });
    addOpItem({
      op: "~*",
      name: "matches",
      resultType: "boolean",
      exprTypes: ["text", "text"]
    });
    addOpItem({
      op: "not",
      name: "is false",
      resultType: "boolean",
      exprTypes: ["boolean"]
    });
    addOpItem({
      op: "is null",
      name: "is blank",
      resultType: "boolean",
      exprTypes: [null]
    });
    addOpItem({
      op: "is not null",
      name: "is not blank",
      resultType: "boolean",
      exprTypes: [null]
    });
  }

  ExprUtils.prototype.findMatchingOpItems = function(search) {
    return _.filter(this.opItems, (function(_this) {
      return function(opItem) {
        var lhsType, ref;
        if (search.resultTypes) {
          if (ref = opItem.resultType, indexOf.call(search.resultTypes, ref) < 0) {
            return false;
          }
        }
        if (search.op && opItem.op !== search.op) {
          return false;
        }
        if (search.lhsExpr) {
          lhsType = _this.getExprType(search.lhsExpr);
          if (opItem.exprTypes[0] !== null && opItem.exprTypes[0] !== lhsType) {
            return false;
          }
        }
        if (search.lhsExpr && opItem.lhsCond && !opItem.lhsCond(search.lhsExpr)) {
          return false;
        }
        return true;
      };
    })(this));
  };

  ExprUtils.prototype.followJoins = function(startTable, joins) {
    var i, j, joinCol, len, t;
    t = startTable;
    for (i = 0, len = joins.length; i < len; i++) {
      j = joins[i];
      joinCol = this.schema.getColumn(t, j);
      t = joinCol.join.toTable;
    }
    return t;
  };

  ExprUtils.prototype.isMultipleJoins = function(table, joins) {
    var i, j, joinCol, len, ref, t;
    t = table;
    for (i = 0, len = joins.length; i < len; i++) {
      j = joins[i];
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

  ExprUtils.prototype.getExprIdTable = function(expr) {
    if (!expr) {
      return null;
    }
    if (expr.type === "literal" && expr.valueType === "id") {
      return expr.idTable;
    }
    if (expr.type === "id") {
      return expr.table;
    }
    if (expr.type === "scalar") {
      return this.getExprIdTable(expr.expr);
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
      case "score":
        return "number";
      case "count":
        return "count";
      default:
        throw new Error("Not implemented for " + expr.type);
    }
  };

  ExprUtils.prototype.areJoinsValid = function(table, joins) {
    var i, j, joinCol, len, t;
    t = table;
    for (i = 0, len = joins.length; i < len; i++) {
      j = joins[i];
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
      case "case":
        return this.summarizeCaseExpr(expr, locale);
      case "literal":
        return expr.value + "";
      case "score":
        return "Score of " + this.summarizeExpr(expr.input, locale);
      default:
        throw new Error("Unsupported type " + expr.type);
    }
  };

  ExprUtils.prototype.summarizeScalarExpr = function(expr, locale) {
    var exprType, i, join, joinCol, len, ref, str, t;
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
    for (i = 0, len = ref.length; i < len; i++) {
      join = ref[i];
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

  ExprUtils.prototype.summarizeCaseExpr = function(expr, locale) {
    var c, i, len, ref, str;
    str = "If";
    ref = expr.cases;
    for (i = 0, len = ref.length; i < len; i++) {
      c = ref[i];
      str += " " + this.summarizeExpr(c.when);
      str += " Then " + this.summarizeExpr(c.then);
    }
    if (expr["else"]) {
      str += " Else " + this.summarizeExpr(expr["else"]);
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
        })(this)).join(', ');
      }
    }
    if (type === "text[]" && _.isArray(literal)) {
      return literal.join(', ');
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

  ExprUtils.prototype.getImmediateReferencedColumns = function(expr) {
    var cols, i, k, len, len1, ref, ref1, subcase, subexpr;
    cols = [];
    if (!expr) {
      return cols;
    }
    switch (expr.type) {
      case "field":
        cols.push(expr.column);
        break;
      case "op":
        ref = expr.exprs;
        for (i = 0, len = ref.length; i < len; i++) {
          subexpr = ref[i];
          cols = cols.concat(this.getImmediateReferencedColumns(subexpr));
        }
        break;
      case "case":
        ref1 = expr.cases;
        for (k = 0, len1 = ref1.length; k < len1; k++) {
          subcase = ref1[k];
          cols = cols.concat(this.getImmediateReferencedColumns(subcase.when));
          cols = cols.concat(this.getImmediateReferencedColumns(subcase.then));
        }
        cols = cols.concat(this.getImmediateReferencedColumns(expr["else"]));
    }
    return _.uniq(cols);
  };

  return ExprUtils;

})();
