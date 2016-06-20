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
    var aggrOpItems, args, booleanOnly, i, k, l, opItem, ref, ref1, ref2, ref3, type;
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
    if (this.exprUtils.getExprAggrStatus(expr) === "individual" && indexOf.call(options.aggrStatuses, "individual") < 0 && indexOf.call(options.aggrStatuses, "aggregate") >= 0) {
      aggrOpItems = this.exprUtils.findMatchingOpItems({
        resultTypes: options.types,
        lhsExpr: expr,
        aggr: true,
        ordered: ((ref = this.schema.getTable(expr.table)) != null ? ref.ordering : void 0) != null
      });
      if (aggrOpItems.length > 0) {
        expr = {
          type: "op",
          op: aggrOpItems[0].op,
          table: expr.table,
          exprs: [expr]
        };
      }
    }
    if (this.exprUtils.getExprAggrStatus(expr) === "individual" && indexOf.call(options.aggrStatuses, "individual") < 0 && indexOf.call(options.aggrStatuses, "aggregate") >= 0) {
      if (!options.types || indexOf.call(options.types, "number") >= 0) {
        opItem = this.exprUtils.findMatchingOpItems({
          resultTypes: ["boolean"],
          lhsExpr: expr
        })[0];
        if (opItem) {
          expr = {
            type: "op",
            table: expr.table,
            op: opItem.op,
            exprs: [expr]
          };
          args = opItem.exprTypes.length - 1;
          for (i = k = 1, ref1 = args; 1 <= ref1 ? k <= ref1 : k >= ref1; i = 1 <= ref1 ? ++k : --k) {
            expr.exprs.push(null);
          }
          expr = {
            type: "op",
            op: "count where",
            table: expr.table,
            exprs: [expr]
          };
        }
      }
    }
    if (this.exprUtils.getExprAggrStatus(expr) && (ref2 = this.exprUtils.getExprAggrStatus(expr), indexOf.call(options.aggrStatuses, ref2) < 0)) {
      return null;
    }
    type = this.exprUtils.getExprType(expr);
    booleanOnly = options.types && options.types.length === 1 && options.types[0] === "boolean";
    if (booleanOnly && type && type !== "boolean") {
      opItem = this.exprUtils.findMatchingOpItems({
        resultTypes: ["boolean"],
        lhsExpr: expr
      })[0];
      if (opItem) {
        expr = {
          type: "op",
          table: expr.table,
          op: opItem.op,
          exprs: [expr]
        };
        args = opItem.exprTypes.length - 1;
        for (i = l = 1, ref3 = args; 1 <= ref3 ? l <= ref3 : l >= ref3; i = 1 <= ref3 ? ++l : --l) {
          expr.exprs.push(null);
        }
      }
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
      case "score":
        return this.cleanScoreExpr(expr, options);
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
    var aggr, enumValueIds, enumValues, exprs, innerAggrStatuses, lhsExpr, lhsTypes, opItem, opItems, ref, ref1, ref2;
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
        if (expr.op === "count" && (!options.types || indexOf.call(options.types, "number") >= 0) && indexOf.call(options.aggrStatuses, "aggregate") >= 0) {
          return {
            type: "op",
            op: "count",
            table: expr.table,
            exprs: []
          };
        }
        aggr = null;
        if (indexOf.call(options.aggrStatuses, "individual") < 0 && indexOf.call(options.aggrStatuses, "aggregate") >= 0) {
          aggr = true;
        }
        if (indexOf.call(options.aggrStatuses, "aggregate") < 0 && indexOf.call(options.aggrStatuses, "individual") >= 0) {
          aggr = false;
        }
        if ((ref = this.exprUtils.findMatchingOpItems({
          op: expr.op
        })[0]) != null ? ref.aggr : void 0) {
          innerAggrStatuses = ["literal", "individual"];
        } else {
          innerAggrStatuses = options.aggrStatuses;
        }
        lhsExpr = this.cleanExpr(expr.exprs[0], {
          table: expr.table,
          aggrStatuses: innerAggrStatuses
        });
        if (lhsExpr) {
          lhsTypes = _.uniq(_.compact(_.map(this.exprUtils.findMatchingOpItems({
            op: expr.op
          }), function(opItem) {
            return opItem.exprTypes[0];
          })));
          lhsExpr = this.cleanExpr(expr.exprs[0], {
            table: expr.table,
            aggrStatuses: innerAggrStatuses,
            types: lhsTypes
          });
          if (lhsExpr == null) {
            lhsExpr = this.cleanExpr(expr.exprs[0], {
              table: expr.table,
              aggrStatuses: innerAggrStatuses
            });
          }
        }
        opItems = this.exprUtils.findMatchingOpItems({
          op: expr.op,
          lhsExpr: lhsExpr,
          resultTypes: options.types,
          aggr: aggr,
          ordered: ((ref1 = this.schema.getTable(expr.table)) != null ? ref1.ordering : void 0) != null
        });
        if (!lhsExpr && !((ref2 = opItems[0]) != null ? ref2.prefix : void 0)) {
          return null;
        }
        if (opItems.length > 1) {
          return _.extend({}, expr, {
            exprs: _.map(expr.exprs, (function(_this) {
              return function(e, i) {
                return _this.cleanExpr(e, {
                  table: expr.table,
                  aggrStatuses: innerAggrStatuses
                });
              };
            })(this))
          });
        }
        if (!opItems[0]) {
          opItem = this.exprUtils.findMatchingOpItems({
            lhsExpr: lhsExpr,
            resultTypes: options.types,
            aggr: aggr
          })[0];
          if (!opItem) {
            return null;
          }
          expr = {
            type: "op",
            table: expr.table,
            op: opItem.op,
            exprs: [lhsExpr || null]
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
        if (lhsExpr) {
          enumValues = this.exprUtils.getExprEnumValues(lhsExpr);
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
                enumValueIds: enumValueIds,
                idTable: _this.exprUtils.getExprIdTable(expr.exprs[0]),
                aggrStatuses: innerAggrStatuses
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
    var aggrStatuses, innerTable, isMultiple, joins;
    if (expr.joins.length === 0) {
      return this.cleanExpr(expr.expr, options);
    }
    joins = _.map(expr.joins, (function(_this) {
      return function(j) {
        if (j.match(/^entities\.[a-z_0-9]+\./)) {
          return j.split(".")[2];
        }
        return j;
      };
    })(this));
    expr = _.extend({}, expr, {
      joins: joins
    });
    if (!this.exprUtils.areJoinsValid(expr.table, expr.joins)) {
      return null;
    }
    innerTable = this.exprUtils.followJoins(expr.table, expr.joins);
    if (expr.aggr) {
      expr = _.extend({}, _.omit(expr, "aggr"), {
        expr: {
          type: "op",
          table: innerTable,
          op: expr.aggr,
          exprs: [expr.expr]
        }
      });
    }
    if (expr.where) {
      expr.where = this.cleanExpr(expr.where, {
        table: innerTable
      });
    }
    if (expr.expr) {
      isMultiple = this.exprUtils.isMultipleJoins(expr.table, expr.joins);
      aggrStatuses = isMultiple ? ["literal", "aggregate"] : ["literal", "individual"];
      expr = _.extend({}, expr, {
        expr: this.cleanExpr(expr.expr, _.extend({}, options, {
          table: innerTable,
          aggrStatuses: aggrStatuses
        }))
      });
    }
    return expr;
  };

  ExprCleaner.prototype.cleanLiteralExpr = function(expr, options) {
    var ref, ref1;
    if ((ref = expr.valueType) === 'decimal' || ref === 'integer') {
      expr = _.extend({}, expr, {
        valueType: "number"
      });
    }
    if (expr.valueType === "enum" && options.enumValueIds && expr.value && (ref1 = expr.value, indexOf.call(options.enumValueIds, ref1) < 0)) {
      return null;
    }
    if (expr.valueType === "enumset" && options.enumValueIds && expr.value) {
      expr = _.extend({}, expr, {
        value: _.intersection(options.enumValueIds, expr.value)
      });
    }
    if (expr.valueType === "id" && options.idTable && expr.idTable !== options.idTable) {
      return null;
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

  ExprCleaner.prototype.cleanScoreExpr = function(expr, options) {
    var enumValues;
    expr = _.extend({}, expr, {
      input: this.cleanExpr(expr.input, {
        types: ['enum', 'enumset']
      })
    });
    if (!expr.input) {
      expr = _.extend({}, expr, {
        scores: {}
      });
    }
    expr = _.extend({}, expr, {
      scores: _.mapValues(expr.scores, (function(_this) {
        return function(scoreExpr) {
          return _this.cleanExpr(scoreExpr, {
            table: expr.table,
            types: ['number']
          });
        };
      })(this))
    });
    if (expr.input) {
      enumValues = this.exprUtils.getExprEnumValues(expr.input);
      expr = _.extend({}, expr, {
        scores: _.pick(expr.scores, (function(_this) {
          return function(value, key) {
            return _.findWhere(enumValues, {
              id: key
            }) && (value != null);
          };
        })(this))
      });
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
    newExpr.exprs = _.map(newExpr.exprs, (function(_this) {
      return function(e) {
        return _this.cleanExpr(e);
      };
    })(this));
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
