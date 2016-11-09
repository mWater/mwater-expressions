var ExprUtils, _, addOpItem, i, k, l, len, len1, len2, len3, len4, len5, m, moment, n, o, op, opItems, ref, ref1, ref2, ref3, ref4, relativeDateOp, relativeDateOps, type,
  indexOf = [].indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

_ = require('lodash');

moment = require('moment');

module.exports = ExprUtils = (function() {
  function ExprUtils(schema) {
    this.schema = schema;
  }

  ExprUtils.prototype.findMatchingOpItems = function(search) {
    return _.filter(opItems, (function(_this) {
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
        if ((search.aggr != null) && opItem.aggr !== search.aggr) {
          return false;
        }
        if (search.ordered === false && opItem.ordered) {
          return false;
        }
        if ((search.prefix != null) && opItem.prefix !== search.prefix) {
          return false;
        }
        if (search.lhsExpr) {
          lhsType = _this.getExprType(search.lhsExpr);
          if (opItem.exprTypes[0] !== null && opItem.exprTypes[0] !== lhsType && opItem.moreExprType !== lhsType) {
            return false;
          }
        }
        if (search.lhsExpr && opItem.lhsCond && !opItem.lhsCond(search.lhsExpr, _this)) {
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
    var column, ref;
    if (!expr) {
      return;
    }
    if (expr.type === "field") {
      column = this.schema.getColumn(expr.table, expr.column);
      if (!column) {
        return null;
      }
      if (column.enumValues) {
        return column.enumValues;
      }
      if (column.type === "expr") {
        return this.getExprEnumValues(column.expr);
      }
      return null;
    }
    if (expr.type === "scalar") {
      if (expr.expr) {
        return this.getExprEnumValues(expr.expr);
      }
    }
    if (expr.type === "op" && ((ref = expr.op) === "last" || ref === "last where") && expr.exprs[0]) {
      return this.getExprEnumValues(expr.exprs[0]);
    }
  };

  ExprUtils.prototype.getExprIdTable = function(expr) {
    var column, ref;
    if (!expr) {
      return null;
    }
    if (expr.type === "literal" && ((ref = expr.valueType) === "id" || ref === "id[]")) {
      return expr.idTable;
    }
    if (expr.type === "id") {
      return expr.table;
    }
    if (expr.type === "scalar") {
      return this.getExprIdTable(expr.expr);
    }
    if (expr.type === "field") {
      column = this.schema.getColumn(expr.table, expr.column);
      if ((column != null ? column.type : void 0) === "join") {
        return column.join.toTable;
      }
      if ((column != null ? column.type : void 0) === "expr") {
        return this.getExprIdTable(column.expr);
      }
      if ((column != null ? column.type : void 0) === "id[]") {
        return column.idTable;
      }
      return null;
    }
  };

  ExprUtils.prototype.getExprType = function(expr) {
    var column, cse, i, len, matchingOpItems, ref, ref1, resultTypes, type;
    if ((expr == null) || !expr.type) {
      return null;
    }
    switch (expr.type) {
      case "field":
        column = this.schema.getColumn(expr.table, expr.column);
        if (column) {
          if (column.type === "join") {
            if ((ref = column.join.type) === '1-1' || ref === 'n-1') {
              return "id";
            } else {
              return "id[]";
            }
          } else if (column.type === "expr") {
            return this.getExprType(column.expr);
          }
          return column.type;
        }
        return null;
      case "id":
        return "id";
      case "scalar":
        if (expr.aggr) {
          return this.getExprType({
            type: "op",
            op: expr.aggr,
            table: expr.table,
            exprs: [expr.expr]
          });
        }
        return this.getExprType(expr.expr);
      case "op":
        matchingOpItems = this.findMatchingOpItems({
          op: expr.op
        });
        resultTypes = _.uniq(_.compact(_.pluck(matchingOpItems, "resultType")));
        if (resultTypes.length === 1) {
          return resultTypes[0];
        }
        matchingOpItems = this.findMatchingOpItems({
          op: expr.op,
          lhsExpr: expr.exprs[0]
        });
        resultTypes = _.uniq(_.compact(_.pluck(matchingOpItems, "resultType")));
        if (resultTypes.length === 1) {
          return resultTypes[0];
        }
        return null;
      case "literal":
        return expr.valueType;
      case "case":
        ref1 = expr.cases;
        for (i = 0, len = ref1.length; i < len; i++) {
          cse = ref1[i];
          type = this.getExprType(cse.then);
          if (type) {
            return type;
          }
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

  ExprUtils.prototype.getExprAggrStatus = function(expr) {
    var column, exprs, getListAggrStatus;
    if ((expr == null) || !expr.type) {
      return null;
    }
    getListAggrStatus = (function(_this) {
      return function(exprs) {
        var i, k, l, len, len1, len2, subExpr;
        for (i = 0, len = exprs.length; i < len; i++) {
          subExpr = exprs[i];
          if (_this.getExprAggrStatus(subExpr) === "aggregate") {
            return "aggregate";
          }
        }
        for (k = 0, len1 = exprs.length; k < len1; k++) {
          subExpr = exprs[k];
          if (_this.getExprAggrStatus(subExpr) === "individual") {
            return "individual";
          }
        }
        for (l = 0, len2 = exprs.length; l < len2; l++) {
          subExpr = exprs[l];
          if (_this.getExprAggrStatus(subExpr) === "literal") {
            return "literal";
          }
        }
        return null;
      };
    })(this);
    switch (expr.type) {
      case "id":
      case "scalar":
        return "individual";
      case "field":
        column = this.schema.getColumn(expr.table, expr.column);
        if ((column != null ? column.type : void 0) === "expr") {
          return this.getExprAggrStatus(column.expr);
        }
        return "individual";
      case "op":
        if (this.findMatchingOpItems({
          op: expr.op,
          aggr: true
        })[0]) {
          return "aggregate";
        }
        return getListAggrStatus(expr.exprs);
      case "literal":
        return "literal";
      case "case":
        exprs = [expr.input, expr["else"]];
        exprs = exprs.concat(_.map(expr.cases, function(cs) {
          return cs.when;
        }));
        exprs = exprs.concat(_.map(expr.cases, function(cs) {
          return cs.then;
        }));
        return getListAggrStatus(exprs);
      case "score":
        return this.getExprAggrStatus(expr.input);
      case "count":
      case "comparison":
      case "logical":
        return "individual";
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
    var aggrOpItems, ref;
    aggrOpItems = this.findMatchingOpItems({
      lhsExpr: expr,
      aggr: true,
      ordered: ((ref = this.schema.getTable(expr.table)) != null ? ref.ordering : void 0) != null
    });
    return _.uniq(_.pluck(aggrOpItems, "resultType"));
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
    if (locale && name[locale]) {
      return name[locale];
    }
    if (name._base && name[name._base]) {
      return name[name._base];
    }
    if (name.en) {
      return name.en;
    }
    return null;
  };

  ExprUtils.prototype.summarizeExpr = function(expr, locale) {
    var opItem, ref, ref1, ref2;
    if (!expr) {
      return "None";
    }
    switch (expr.type) {
      case "scalar":
        return this.summarizeScalarExpr(expr, locale);
      case "field":
        return this.localizeString((ref = this.schema.getColumn(expr.table, expr.column)) != null ? ref.name : void 0, locale);
      case "id":
        return this.localizeString((ref1 = this.schema.getTable(expr.table)) != null ? ref1.name : void 0, locale);
      case "op":
        if (expr.op === "contains" && ((ref2 = expr.exprs[1]) != null ? ref2.type : void 0) === "literal") {
          return this.summarizeExpr(expr.exprs[0], locale) + " contains " + this.stringifyExprLiteral(expr.exprs[0], expr.exprs[1].value, locale);
        }
        if (expr.op === "count") {
          return "Number of " + this.localizeString(this.schema.getTable(expr.table).name, locale);
        }
        opItem = this.findMatchingOpItems({
          op: expr.op
        })[0];
        if (opItem) {
          if (opItem.prefix) {
            return opItem.name + " " + _.map(expr.exprs, (function(_this) {
              return function(e) {
                return _this.summarizeExpr(e, locale);
              };
            })(this)).join(", ");
          }
          return _.map(expr.exprs, (function(_this) {
            return function(e) {
              return _this.summarizeExpr(e, locale);
            };
          })(this)).join(" " + opItem.name + " ");
        } else {
          return "";
        }
        break;
      case "case":
        return this.summarizeCaseExpr(expr, locale);
      case "literal":
        return expr.value + "";
      case "score":
        return "Score of " + this.summarizeExpr(expr.input, locale);
      case "count":
        return "Count";
      default:
        throw new Error("Unsupported type " + expr.type);
    }
  };

  ExprUtils.prototype.summarizeScalarExpr = function(expr, locale) {
    var exprType, i, innerExpr, join, joinCol, len, ref, ref1, str, t;
    exprType = this.getExprType(expr.expr);
    str = "";
    t = expr.table;
    ref = expr.joins;
    for (i = 0, len = ref.length; i < len; i++) {
      join = ref[i];
      joinCol = this.schema.getColumn(t, join);
      str += this.localizeString(joinCol.name, locale) + " > ";
      t = joinCol.join.toTable;
    }
    if (exprType === "id" && !expr.aggr) {
      str = str.substring(0, str.length - 3);
    } else {
      innerExpr = expr.expr;
      if (expr.aggr) {
        innerExpr = {
          type: "op",
          op: expr.aggr,
          table: (ref1 = expr.expr) != null ? ref1.table : void 0,
          exprs: [expr.expr]
        };
      }
      str += this.summarizeExpr(innerExpr, locale);
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

  ExprUtils.prototype.stringifyExprLiteral = function(expr, literal, locale, preferEnumCodes) {
    var enumValues, item, type;
    if (preferEnumCodes == null) {
      preferEnumCodes = false;
    }
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
          if (preferEnumCodes && item.code) {
            return item.code;
          }
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
              if (preferEnumCodes && item.code) {
                return item.code;
              }
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
    if (type === "date") {
      return moment(literal, moment.ISO_8601).format("ll");
    }
    if (type === "datetime") {
      return moment(literal, moment.ISO_8601).format("lll");
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

  ExprUtils.prototype.getReferencedFields = function(expr) {
    var cols, column, i, join, k, l, len, len1, len2, len3, m, ref, ref1, ref2, ref3, subcase, subexpr, table, value;
    cols = [];
    if (!expr) {
      return cols;
    }
    switch (expr.type) {
      case "field":
        cols.push(expr);
        column = this.schema.getColumn(expr.table, expr.column);
        if (column != null ? column.expr : void 0) {
          cols = cols.concat(this.getReferencedFields(column.expr));
        }
        break;
      case "op":
        ref = expr.exprs;
        for (i = 0, len = ref.length; i < len; i++) {
          subexpr = ref[i];
          cols = cols.concat(this.getReferencedFields(subexpr));
        }
        break;
      case "case":
        ref1 = expr.cases;
        for (k = 0, len1 = ref1.length; k < len1; k++) {
          subcase = ref1[k];
          cols = cols.concat(this.getReferencedFields(subcase.when));
          cols = cols.concat(this.getReferencedFields(subcase.then));
        }
        cols = cols.concat(this.getReferencedFields(expr["else"]));
        break;
      case "scalar":
        table = expr.table;
        ref2 = expr.joins;
        for (l = 0, len2 = ref2.length; l < len2; l++) {
          join = ref2[l];
          cols.push({
            type: "field",
            table: table,
            column: join
          });
          column = this.schema.getColumn(table, join);
          if (!column) {
            break;
          }
          table = column.join.toTable;
        }
        cols = cols.concat(this.getReferencedFields(expr.expr));
        break;
      case "score":
        cols = cols.concat(this.getReferencedFields(expr.input));
        ref3 = _.values(expr.scores);
        for (m = 0, len3 = ref3.length; m < len3; m++) {
          value = ref3[m];
          cols = cols.concat(this.getReferencedFields(value));
        }
    }
    return _.uniq(cols, function(col) {
      return col.table + "/" + col.column;
    });
  };

  return ExprUtils;

})();

opItems = [];

addOpItem = (function(_this) {
  return function(item) {
    return opItems.push(_.defaults(item, {
      prefix: false,
      rhsLiteral: true,
      aggr: false,
      ordered: false
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

ref = ['date', 'datetime'];
for (k = 0, len1 = ref.length; k < len1; k++) {
  type = ref[k];
  addOpItem({
    op: ">",
    name: "is after",
    resultType: "boolean",
    exprTypes: [type, type]
  });
  addOpItem({
    op: "<",
    name: "is before",
    resultType: "boolean",
    exprTypes: [type, type]
  });
  addOpItem({
    op: ">=",
    name: "is after or same as",
    resultType: "boolean",
    exprTypes: [type, type]
  });
  addOpItem({
    op: "<=",
    name: "is before or same as",
    resultType: "boolean",
    exprTypes: [type, type]
  });
}

addOpItem({
  op: "between",
  name: "is between",
  resultType: "boolean",
  exprTypes: ["number", "number", "number"]
});

addOpItem({
  op: "round",
  name: "Round",
  resultType: "number",
  exprTypes: ["number"],
  prefix: true
});

addOpItem({
  op: "floor",
  name: "Floor",
  resultType: "number",
  exprTypes: ["number"],
  prefix: true
});

addOpItem({
  op: "ceiling",
  name: "Ceiling",
  resultType: "number",
  exprTypes: ["number"],
  prefix: true
});

addOpItem({
  op: "latitude",
  name: "Latitude of",
  resultType: "number",
  exprTypes: ["geometry"],
  prefix: true
});

addOpItem({
  op: "longitude",
  name: "Longitude of",
  resultType: "number",
  exprTypes: ["geometry"],
  prefix: true
});

addOpItem({
  op: "distance",
  name: "Distance between",
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

ref1 = ['+', '*'];
for (l = 0, len2 = ref1.length; l < len2; l++) {
  op = ref1[l];
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
  op: "days difference",
  name: "Days between",
  resultType: "number",
  exprTypes: ["date", "date"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
});

addOpItem({
  op: "days difference",
  name: "Days between",
  resultType: "number",
  exprTypes: ["datetime", "datetime"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
});

addOpItem({
  op: "days since",
  name: "Days since",
  resultType: "number",
  exprTypes: ["date"],
  prefix: true,
  rhsLiteral: false
});

addOpItem({
  op: "days since",
  name: "Days since",
  resultType: "number",
  exprTypes: ["datetime"],
  prefix: true,
  rhsLiteral: false
});

ref2 = ['text', 'number', 'enum', 'enumset', 'boolean', 'date', 'datetime', 'geometry'];
for (m = 0, len3 = ref2.length; m < len3; m++) {
  type = ref2[m];
  addOpItem({
    op: "last",
    name: "Latest",
    resultType: type,
    exprTypes: [type],
    prefix: true,
    aggr: true,
    ordered: true
  });
  addOpItem({
    op: "last where",
    name: "Latest that",
    resultType: type,
    exprTypes: [type, "boolean"],
    prefix: true,
    prefixLabel: "Latest",
    aggr: true,
    ordered: true,
    rhsLiteral: false,
    joiner: "that",
    rhsPlaceholder: "All"
  });
}

addOpItem({
  op: "sum",
  name: "Total",
  resultType: "number",
  exprTypes: ["number"],
  prefix: true,
  aggr: true
});

addOpItem({
  op: "avg",
  name: "Average",
  resultType: "number",
  exprTypes: ["number"],
  prefix: true,
  aggr: true
});

ref3 = ['number', 'date', 'datetime'];
for (n = 0, len4 = ref3.length; n < len4; n++) {
  type = ref3[n];
  addOpItem({
    op: "min",
    name: "Minimum",
    resultType: type,
    exprTypes: [type],
    prefix: true,
    aggr: true
  });
  addOpItem({
    op: "max",
    name: "Maximum",
    resultType: type,
    exprTypes: [type],
    prefix: true,
    aggr: true
  });
}

addOpItem({
  op: "percent where",
  name: "Percent that",
  resultType: "number",
  exprTypes: ["boolean", "boolean"],
  prefix: true,
  aggr: true,
  rhsLiteral: false,
  joiner: "of",
  rhsPlaceholder: "All"
});

addOpItem({
  op: "count where",
  name: "Number that",
  resultType: "number",
  exprTypes: ["boolean"],
  prefix: true,
  aggr: true
});

addOpItem({
  op: "sum where",
  name: "Total that",
  resultType: "number",
  exprTypes: ["number", "boolean"],
  prefix: true,
  prefixLabel: "Total",
  aggr: true,
  rhsLiteral: false,
  joiner: "that",
  rhsPlaceholder: "All"
});

addOpItem({
  op: "within",
  name: "in",
  resultType: "boolean",
  exprTypes: ["id", "id"],
  lhsCond: (function(_this) {
    return function(lhsExpr, exprUtils) {
      var lhsIdTable;
      lhsIdTable = exprUtils.getExprIdTable(lhsExpr);
      if (lhsIdTable) {
        return exprUtils.schema.getTable(lhsIdTable).ancestry != null;
      }
      return false;
    };
  })(this)
});

addOpItem({
  op: "= any",
  name: "is any of",
  resultType: "boolean",
  exprTypes: ["id", "id[]"]
});

addOpItem({
  op: "contains",
  name: "includes all of",
  resultType: "boolean",
  exprTypes: ["id[]", "id[]"]
});

addOpItem({
  op: "=",
  name: "is",
  resultType: "boolean",
  exprTypes: ["id", "id"]
});

addOpItem({
  op: "<>",
  name: "is not",
  resultType: "boolean",
  exprTypes: ["id", "id"]
});

addOpItem({
  op: "count",
  name: "Number of",
  resultType: "number",
  exprTypes: [],
  prefix: true,
  aggr: true
});

addOpItem({
  op: "~*",
  name: "matches",
  resultType: "boolean",
  exprTypes: ["text", "text"]
});

addOpItem({
  op: "not",
  name: "not",
  resultType: "boolean",
  exprTypes: ["boolean"],
  prefix: true
});

ref4 = ['text', 'number', 'enum', 'enumset', 'boolean', 'date', 'datetime', 'geometry', 'image', 'imagelist', 'id'];
for (o = 0, len5 = ref4.length; o < len5; o++) {
  type = ref4[o];
  addOpItem({
    op: "is null",
    name: "is blank",
    resultType: "boolean",
    exprTypes: [type]
  });
  addOpItem({
    op: "is not null",
    name: "is not blank",
    resultType: "boolean",
    exprTypes: [type]
  });
}

addOpItem({
  op: "cardinality",
  name: "Number of values in",
  resultType: "number",
  exprTypes: ["enumset"],
  prefix: true
});

addOpItem({
  op: "cardinality",
  name: "Number of values in",
  resultType: "number",
  exprTypes: ["imagelist"],
  prefix: true
});

addOpItem({
  op: "cardinality",
  name: "Number of values in",
  resultType: "number",
  exprTypes: ["text[]"],
  prefix: true
});

addOpItem({
  op: "to text",
  name: "Convert to text",
  resultType: "text",
  exprTypes: ["enum"],
  prefix: true
});
