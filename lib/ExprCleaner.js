var ExprCleaner, ExprUtils, _,
  bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; },
  indexOf = [].indexOf || function(item) { for (var i = 0, l = this.length; i < l; i++) { if (i in this && this[i] === item) return i; } return -1; };

_ = require('lodash');

ExprUtils = require('./ExprUtils');

module.exports = ExprCleaner = (function() {
  function ExprCleaner(schema) {
    this.cleanCountExpr = bind(this.cleanCountExpr, this);
    this.cleanLogicalExpr = bind(this.cleanLogicalExpr, this);
    this.cleanComparisonExpr = bind(this.cleanComparisonExpr, this);
    this.schema = schema;
    this.exprUtils = new ExprUtils(schema);
  }

  ExprCleaner.prototype.cleanExpr = function(expr, options) {
    var type;
    if (options == null) {
      options = {};
    }
    if (!expr) {
      return null;
    }
    if (_.isEmpty(expr)) {
      return expr;
    }
    if (expr.type === "comparison") {
      return this.cleanComparisonExpr(expr, options);
    }
    if (expr.type === "logical") {
      return this.cleanLogicalExpr(expr, options);
    }
    if (expr.type === "count") {
      return this.cleanCountExpr(expr, options);
    }
    if (expr.type === "literal" && expr.valueType === "enum[]") {
      expr = {
        type: "literal",
        valueType: "enumset",
        value: expr.value
      };
    }
    if (options.table && expr.type !== "literal" && expr.table !== options.table) {
      return null;
    }
    if (!expr.table && expr.type !== "literal") {
      return null;
    }
    if (expr.table && !this.schema.getTable(expr.table)) {
      return null;
    }
    type = this.exprUtils.getExprType(expr);
    if (type && options.types && indexOf.call(options.types, type) < 0) {
      if (expr.type !== "case") {
        return null;
      }
    }
    switch (expr.type) {
      case "field":
        return this.cleanFieldExpr(expr, options);
      case "scalar":
        return this.cleanScalarExpr(expr, options);
      case "op":
        return this.cleanOpExpr(expr, options);
      case "literal":
        return this.cleanLiteralExpr(expr, options);
      case "case":
        return this.cleanCaseExpr(expr, options);
      case "id":
        return this.cleanIdExpr(expr, options);
      default:
        throw new Error("Unknown expression type " + expr.type);
    }
  };

  ExprCleaner.prototype.cleanFieldExpr = function(expr, options) {
    var column;
    if (!expr.column || !expr.table) {
      return null;
    }
    if (!this.schema.getTable(expr.table)) {
      return null;
    }
    column = this.schema.getColumn(expr.table, expr.column);
    if (!column) {
      return null;
    }
    if (options.enumValueIds && column.type === "enum") {
      if (_.difference(_.pluck(column.enumValues, "id"), options.enumValueIds).length > 0) {
        return null;
      }
    }
    return expr;
  };

  ExprCleaner.prototype.cleanOpExpr = function(expr, options) {
    var enumValueIds, enumValues, exprs, lhsType, opItem, opItems;
    switch (expr.op) {
      case "and":
      case "or":
        expr = _.extend({}, expr, {
          exprs: _.map(expr.exprs, (function(_this) {
            return function(e) {
              return _this.cleanExpr(e, {
                types: ["boolean"],
                table: expr.table
              });
            };
          })(this))
        });
        if (expr.exprs.length === 1) {
          return expr.exprs[0];
        }
        if (expr.exprs.length === 0) {
          return null;
        }
        return expr;
      case "+":
      case "*":
        expr = _.extend({}, expr, {
          exprs: _.map(expr.exprs, (function(_this) {
            return function(e) {
              return _this.cleanExpr(e, {
                types: ["number"],
                table: expr.table
              });
            };
          })(this))
        });
        if (expr.exprs.length === 1) {
          return expr.exprs[0];
        }
        if (expr.exprs.length === 0) {
          return null;
        }
        return expr;
      default:
        if (!expr.exprs[0]) {
          return null;
        }
        lhsType = this.exprUtils.getExprType(expr.exprs[0]);
        opItems = this.exprUtils.findMatchingOpItems({
          op: expr.op,
          exprTypes: [lhsType]
        });
        if (opItems.length > 1) {
          return _.extend({}, expr, {
            exprs: _.map(expr.exprs, (function(_this) {
              return function(e, i) {
                return _this.cleanExpr(e, {
                  table: expr.table
                });
              };
            })(this))
          });
        }
        if (!opItems[0]) {
          opItem = this.exprUtils.findMatchingOpItems({
            exprTypes: [lhsType]
          })[0];
          expr = {
            type: "op",
            table: expr.table,
            op: opItem.op,
            exprs: [expr.exprs[0] || null]
          };
        } else {
          opItem = opItems[0];
        }
        while (expr.exprs.length < opItem.exprTypes.length) {
          exprs = expr.exprs.slice();
          exprs.push(null);
          expr = _.extend({}, expr, {
            exprs: exprs
          });
        }
        if (expr.exprs.length > opItem.exprTypes.length) {
          expr = _.extend({}, expr, {
            exprs: _.take(expr.exprs, opItem.exprTypes.length)
          });
        }
        if (expr.exprs[0]) {
          enumValues = this.exprUtils.getExprEnumValues(expr.exprs[0]);
          if (enumValues) {
            enumValueIds = _.pluck(enumValues, "id");
          }
        }
        expr = _.extend({}, expr, {
          exprs: _.map(expr.exprs, (function(_this) {
            return function(e, i) {
              return _this.cleanExpr(e, {
                table: expr.table,
                types: (opItem.exprTypes[i] ? [opItem.exprTypes[i]] : void 0),
                enumValueIds: enumValueIds
              });
            };
          })(this))
        });
        return expr;
    }
  };

  ExprCleaner.prototype.areJoinsValid = function(table, joins) {
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

  ExprCleaner.prototype.cleanScalarExpr = function(expr, options) {
    var innerTable, innerTypes, ref;
    if (expr.joins.length === 0) {
      return this.cleanExpr(expr.expr, options);
    }
    if (!this.exprUtils.areJoinsValid(expr.table, expr.joins)) {
      return null;
    }
    if (expr.aggr && !this.exprUtils.isMultipleJoins(expr.table, expr.joins)) {
      expr = _.omit(expr, "aggr");
    }
    if (this.exprUtils.isMultipleJoins(expr.table, expr.joins) && (ref = expr.aggr, indexOf.call(_.pluck(this.exprUtils.getAggrs(expr.expr), "id"), ref) < 0)) {
      expr = _.extend({}, expr, {
        aggr: this.exprUtils.getAggrs(expr.expr)[0].id
      });
    }
    if (expr.where) {
      expr.where = this.cleanExpr(expr.where);
    }
    innerTable = this.exprUtils.followJoins(expr.table, expr.joins);
    innerTypes = expr.aggr !== "count" ? options.types : void 0;
    expr = _.extend({}, expr, {
      expr: this.cleanExpr(expr.expr, _.extend({}, options, {
        table: innerTable,
        types: innerTypes
      }))
    });
    return expr;
  };

  ExprCleaner.prototype.cleanLiteralExpr = function(expr, options) {
    var ref;
    if (expr.valueType === "enum" && options.enumValueIds && expr.value && (ref = expr.value, indexOf.call(options.enumValueIds, ref) < 0)) {
      return null;
    }
    if (expr.valueType === "enumset" && options.enumValueIds && expr.value) {
      expr = _.extend({}, expr, {
        value: _.intersection(options.enumValueIds, expr.value)
      });
    }
    return expr;
  };

  ExprCleaner.prototype.cleanCaseExpr = function(expr, options) {
    if (expr.cases.length === 0) {
      return expr["else"] || null;
    }
    expr = _.extend({}, expr, {
      cases: _.map(expr.cases, (function(_this) {
        return function(c) {
          return _.extend({}, c, {
            when: _this.cleanExpr(c.when, {
              types: ["boolean"],
              table: expr.table
            }),
            then: _this.cleanExpr(c.then, options)
          });
        };
      })(this)),
      "else": this.cleanExpr(expr["else"], options)
    });
    return expr;
  };

  ExprCleaner.prototype.cleanIdExpr = function(expr, options) {
    if (options.idTable && expr.table !== options.idTable) {
      return null;
    }
    return expr;
  };

  ExprCleaner.prototype.cleanComparisonExpr = function(expr, options) {
    var newExpr;
    newExpr = {
      type: "op",
      table: expr.table,
      op: expr.op,
      exprs: [expr.lhs]
    };
    if (expr.rhs) {
      newExpr.exprs.push(expr.rhs);
    }
    if (expr.op === "= true") {
      newExpr = expr.lhs;
    }
    if (expr.op === "= false") {
      newExpr = {
        type: "op",
        op: "not",
        table: expr.table,
        exprs: [expr.lhs]
      };
    }
    if (expr.op === "between" && expr.rhs && expr.rhs.type === "literal" && expr.rhs.valueType === "daterange") {
      newExpr.exprs = [
        expr.lhs, {
          type: "literal",
          valueType: "date",
          value: expr.rhs.value[0]
        }, {
          type: "literal",
          valueType: "date",
          value: expr.rhs.value[1]
        }
      ];
    }
    if (expr.op === "between" && expr.rhs && expr.rhs.type === "literal" && expr.rhs.valueType === "datetimerange") {
      if (this.exprUtils.getExprType(expr.lhs) === "date") {
        newExpr.exprs = [
          expr.lhs, {
            type: "literal",
            valueType: "date",
            value: expr.rhs.value[0].substr(0, 10)
          }, {
            type: "literal",
            valueType: "date",
            value: expr.rhs.value[1].substr(0, 10)
          }
        ];
      } else {
        newExpr.exprs = [
          expr.lhs, {
            type: "literal",
            valueType: "datetime",
            value: expr.rhs.value[0]
          }, {
            type: "literal",
            valueType: "datetime",
            value: expr.rhs.value[1]
          }
        ];
      }
    }
    return this.cleanExpr(newExpr, options);
  };

  ExprCleaner.prototype.cleanLogicalExpr = function(expr, options) {
    var newExpr;
    newExpr = {
      type: "op",
      op: expr.op,
      table: expr.table,
      exprs: expr.exprs
    };
    return this.cleanExpr(newExpr, options);
  };

  ExprCleaner.prototype.cleanCountExpr = function(expr, options) {
    var newExpr;
    newExpr = {
      type: "id",
      table: expr.table
    };
    return this.cleanExpr(newExpr, options);
  };

  return ExprCleaner;

})();
