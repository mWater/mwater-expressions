var ColumnNotFoundException, ExprCompiler, ExprUtils, _, injectTableAlias, injectTableAliases, moment,
  bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };

_ = require('lodash');

injectTableAlias = require('./injectTableAlias');

injectTableAliases = require('./injectTableAliases');

ExprUtils = require('./ExprUtils');

moment = require('moment');

ColumnNotFoundException = require('./ColumnNotFoundException');

module.exports = ExprCompiler = (function() {
  function ExprCompiler(schema) {
    this.compileExpr = bind(this.compileExpr, this);
    this.schema = schema;
  }

  ExprCompiler.prototype.compileExpr = function(options) {
    var compiledExpr, expr;
    expr = options.expr;
    if (!expr) {
      return null;
    }
    switch (expr.type) {
      case "id":
        compiledExpr = {
          type: "field",
          tableAlias: options.tableAlias,
          column: this.schema.getTable(expr.table).primaryKey
        };
        break;
      case "field":
        compiledExpr = this.compileFieldExpr(options);
        break;
      case "scalar":
        compiledExpr = this.compileScalarExpr(options);
        break;
      case "literal":
        compiledExpr = {
          type: "literal",
          value: expr.value
        };
        break;
      case "op":
        compiledExpr = this.compileOpExpr(options);
        break;
      case "case":
        compiledExpr = this.compileCaseExpr(options);
        break;
      case "count":
        compiledExpr = null;
        break;
      case "comparison":
        compiledExpr = this.compileComparisonExpr(options);
        break;
      case "logical":
        compiledExpr = this.compileLogicalExpr(options);
        break;
      default:
        throw new Error("Expr type " + expr.type + " not supported");
    }
    return compiledExpr;
  };

  ExprCompiler.prototype.compileFieldExpr = function(options) {
    var column, expr;
    expr = options.expr;
    column = this.schema.getColumn(expr.table, expr.column);
    if (!column) {
      throw new ColumnNotFoundException("Column " + expr.table + "." + expr.column + " not found");
    }
    return this.compileColumnRef(column.jsonql || column.id, options.tableAlias);
  };

  ExprCompiler.prototype.compileScalarExpr = function(options) {
    var expr, extraWhere, from, i, j, join, joinColumn, limit, onClause, orderBy, ordering, ref, scalar, scalarExpr, table, tableAlias, where;
    expr = options.expr;
    where = null;
    from = null;
    orderBy = null;
    limit = null;
    table = expr.table;
    tableAlias = options.tableAlias;
    if (expr.joins && expr.joins.length > 0) {
      joinColumn = this.schema.getColumn(expr.table, expr.joins[0]);
      if (!joinColumn) {
        throw new ColumnNotFoundException("Join column " + expr.table + ":" + expr.joins[0] + " not found");
      }
      join = joinColumn.join;
      if (join.jsonql) {
        where = injectTableAliases(join.jsonql, {
          "{from}": tableAlias,
          "{to}": "j1"
        });
      } else {
        where = {
          type: "op",
          op: "=",
          exprs: [this.compileColumnRef(join.toColumn, "j1"), this.compileColumnRef(join.fromColumn, tableAlias)]
        };
      }
      from = this.compileTable(join.toTable, "j1");
      table = join.toTable;
      tableAlias = "j1";
    }
    if (expr.joins.length > 1) {
      for (i = j = 1, ref = expr.joins.length; 1 <= ref ? j < ref : j > ref; i = 1 <= ref ? ++j : --j) {
        joinColumn = this.schema.getColumn(table, expr.joins[i]);
        if (!joinColumn) {
          throw new ColumnNotFoundException("Join column " + expr.table + ":" + expr.joins[0] + " not found");
        }
        join = joinColumn.join;
        if (join.jsonql) {
          onClause = injectTableAliases(join.jsonql, {
            "{from}": "j" + i,
            "{to}": "j" + (i + 1)
          });
        } else {
          onClause = {
            type: "op",
            op: "=",
            exprs: [this.compileColumnRef(join.fromColumn, "j" + i), this.compileColumnRef(join.toColumn, "j" + (i + 1))]
          };
        }
        from = {
          type: "join",
          left: from,
          right: this.compileTable(join.toTable, "j" + (i + 1)),
          kind: "left",
          on: onClause
        };
        table = join.toTable;
        tableAlias = "j" + (i + 1);
      }
    }
    if (expr.where) {
      extraWhere = this.compileExpr({
        expr: expr.where,
        tableAlias: tableAlias
      });
      if (where) {
        where = {
          type: "op",
          op: "and",
          exprs: [where, extraWhere]
        };
      } else {
        where = extraWhere;
      }
    }
    scalarExpr = this.compileExpr({
      expr: expr.expr,
      tableAlias: tableAlias
    });
    if (expr.aggr) {
      switch (expr.aggr) {
        case "last":
          ordering = this.schema.getTable(table).ordering;
          if (!ordering) {
            throw new Error("No ordering defined");
          }
          limit = 1;
          orderBy = [
            {
              expr: this.compileColumnRef(ordering, tableAlias),
              direction: "desc"
            }
          ];
          break;
        case "sum":
        case "count":
        case "avg":
        case "max":
        case "min":
        case "stdev":
        case "stdevp":
          if (!scalarExpr) {
            scalarExpr = {
              type: "op",
              op: expr.aggr,
              exprs: []
            };
          } else {
            scalarExpr = {
              type: "op",
              op: expr.aggr,
              exprs: [scalarExpr]
            };
          }
          break;
        default:
          throw new Error("Unknown aggregation " + expr.aggr);
      }
    }
    if (!from && !where && !orderBy && !limit) {
      return scalarExpr;
    }
    scalar = {
      type: "scalar",
      expr: scalarExpr
    };
    if (from) {
      scalar.from = from;
    }
    if (where) {
      scalar.where = where;
    }
    if (orderBy) {
      scalar.orderBy = orderBy;
    }
    if (limit) {
      scalar.limit = limit;
    }
    return scalar;
  };

  ExprCompiler.prototype.compileOpExpr = function(options) {
    var compiledExprs, expr;
    expr = options.expr;
    compiledExprs = _.map(expr.exprs, (function(_this) {
      return function(e) {
        return _this.compileExpr({
          expr: e,
          tableAlias: options.tableAlias
        });
      };
    })(this));
    switch (expr.op) {
      case "and":
      case "or":
      case "+":
      case "*":
        compiledExprs = _.compact(compiledExprs);
        if (compiledExprs.length === 0) {
          return null;
        }
        return {
          type: "op",
          op: expr.op,
          exprs: compiledExprs
        };
      case "-":
      case "/":
      case ">":
      case "<":
      case ">=":
      case "<=":
      case "<>":
      case "=":
      case "~*":
        if (_.any(compiledExprs, function(ce) {
          return ce == null;
        })) {
          return null;
        }
        return {
          type: "op",
          op: expr.op,
          exprs: compiledExprs
        };
      case '= any':
        if (_.any(compiledExprs, function(ce) {
          return ce == null;
        })) {
          return null;
        }
        if (expr.exprs[1].type === "literal") {
          if (!expr.exprs[1].value || (_.isArray(expr.exprs[1].value) && expr.exprs[1].value.length === 0)) {
            return null;
          }
        }
        return {
          type: "op",
          op: "=",
          modifier: "any",
          exprs: compiledExprs
        };
      case "between":
        if (!compiledExprs[0]) {
          return null;
        }
        if (!compiledExprs[1] && !compiledExprs[2]) {
          return null;
        }
        if (!compiledExprs[2]) {
          return {
            type: "op",
            op: ">=",
            exprs: [compiledExprs[0], compiledExprs[1]]
          };
        }
        if (!compiledExprs[1]) {
          return {
            type: "op",
            op: "<=",
            exprs: [compiledExprs[0], compiledExprs[2]]
          };
        }
        return {
          type: "op",
          op: "between",
          exprs: compiledExprs
        };
      case "not":
      case "is null":
      case "is not null":
        if (!compiledExprs[0]) {
          return null;
        }
        return {
          type: "op",
          op: expr.op,
          exprs: compiledExprs
        };
      case "contains":
        if (!compiledExprs[0] || !compiledExprs[1]) {
          return null;
        }
        return {
          type: "op",
          op: "@>",
          exprs: [
            {
              type: "op",
              op: "::jsonb",
              exprs: [compiledExprs[0]]
            }, {
              type: "op",
              op: "::jsonb",
              exprs: [compiledExprs[1]]
            }
          ]
        };
      case 'thisyear':
        if (!compiledExprs[0]) {
          return null;
        }
        return {
          type: "op",
          op: "and",
          exprs: [
            {
              type: "op",
              op: ">=",
              exprs: [compiledExprs[0], moment().startOf("year").format("YYYY-MM-DD")]
            }, {
              type: "op",
              op: "<",
              exprs: [compiledExprs[0], moment().startOf("year").add(1, 'years').format("YYYY-MM-DD")]
            }
          ]
        };
      case 'lastyear':
        if (!compiledExprs[0]) {
          return null;
        }
        return {
          type: "op",
          op: "and",
          exprs: [
            {
              type: "op",
              op: ">=",
              exprs: [compiledExprs[0], moment().startOf("year").subtract(1, 'years').format("YYYY-MM-DD")]
            }, {
              type: "op",
              op: "<",
              exprs: [compiledExprs[0], moment().startOf("year").format("YYYY-MM-DD")]
            }
          ]
        };
      case 'thismonth':
        if (!compiledExprs[0]) {
          return null;
        }
        return {
          type: "op",
          op: "and",
          exprs: [
            {
              type: "op",
              op: ">=",
              exprs: [compiledExprs[0], moment().startOf("month").format("YYYY-MM-DD")]
            }, {
              type: "op",
              op: "<",
              exprs: [compiledExprs[0], moment().startOf("month").add(1, 'months').format("YYYY-MM-DD")]
            }
          ]
        };
      case 'lastmonth':
        if (!compiledExprs[0]) {
          return null;
        }
        return {
          type: "op",
          op: "and",
          exprs: [
            {
              type: "op",
              op: ">=",
              exprs: [compiledExprs[0], moment().startOf("month").subtract(1, 'months').format("YYYY-MM-DD")]
            }, {
              type: "op",
              op: "<",
              exprs: [compiledExprs[0], moment().startOf("month").format("YYYY-MM-DD")]
            }
          ]
        };
      case 'today':
        if (!compiledExprs[0]) {
          return null;
        }
        return {
          type: "op",
          op: "and",
          exprs: [
            {
              type: "op",
              op: ">=",
              exprs: [compiledExprs[0], moment().format("YYYY-MM-DD")]
            }, {
              type: "op",
              op: "<",
              exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD")]
            }
          ]
        };
      case 'yesterday':
        if (!compiledExprs[0]) {
          return null;
        }
        return {
          type: "op",
          op: "and",
          exprs: [
            {
              type: "op",
              op: ">=",
              exprs: [compiledExprs[0], moment().subtract(1, 'days').format("YYYY-MM-DD")]
            }, {
              type: "op",
              op: "<",
              exprs: [compiledExprs[0], moment().format("YYYY-MM-DD")]
            }
          ]
        };
      case 'last7days':
        if (!compiledExprs[0]) {
          return null;
        }
        return {
          type: "op",
          op: "and",
          exprs: [
            {
              type: "op",
              op: ">=",
              exprs: [compiledExprs[0], moment().subtract(7, 'days').format("YYYY-MM-DD")]
            }, {
              type: "op",
              op: "<",
              exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD")]
            }
          ]
        };
      case 'last30days':
        if (!compiledExprs[0]) {
          return null;
        }
        return {
          type: "op",
          op: "and",
          exprs: [
            {
              type: "op",
              op: ">=",
              exprs: [compiledExprs[0], moment().subtract(30, 'days').format("YYYY-MM-DD")]
            }, {
              type: "op",
              op: "<",
              exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD")]
            }
          ]
        };
      case 'last365days':
        if (!compiledExprs[0]) {
          return null;
        }
        return {
          type: "op",
          op: "and",
          exprs: [
            {
              type: "op",
              op: ">=",
              exprs: [compiledExprs[0], moment().subtract(365, 'days').format("YYYY-MM-DD")]
            }, {
              type: "op",
              op: "<",
              exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD")]
            }
          ]
        };
      default:
        throw new Error("Unknown op " + expr.op);
    }
  };

  ExprCompiler.prototype.compileCaseExpr = function(options) {
    var expr;
    expr = options.expr;
    return {
      type: "case",
      cases: _.map(expr.cases, (function(_this) {
        return function(c) {
          return {
            when: _this.compileExpr({
              expr: c.when,
              tableAlias: options.tableAlias
            }),
            then: _this.compileExpr({
              expr: c.then,
              tableAlias: options.tableAlias
            })
          };
        };
      })(this)),
      "else": this.compileExpr({
        expr: expr["else"],
        tableAlias: options.tableAlias
      })
    };
  };

  ExprCompiler.prototype.compileComparisonExpr = function(options) {
    var expr, exprLhsType, exprUtils, exprs, lhsExpr, rhsExpr;
    expr = options.expr;
    exprLhsType = exprUtils.getExprType(expr.lhs);
    if (!exprLhsType) {
      return null;
    }
    exprUtils = new ExprUtils(this.schema);
    if (exprUtils.getComparisonRhsType(exprLhsType, expr.op) && (expr.rhs == null)) {
      return null;
    }
    lhsExpr = this.compileExpr({
      expr: expr.lhs,
      tableAlias: options.tableAlias
    });
    if (expr.rhs) {
      rhsExpr = this.compileExpr({
        expr: expr.rhs,
        tableAlias: options.tableAlias
      });
      exprs = [lhsExpr, rhsExpr];
    } else {
      exprs = [lhsExpr];
    }
    switch (expr.op) {
      case '= true':
        return {
          type: "op",
          op: "=",
          exprs: [
            lhsExpr, {
              type: "literal",
              value: true
            }
          ]
        };
      case '= false':
        return {
          type: "op",
          op: "=",
          exprs: [
            lhsExpr, {
              type: "literal",
              value: false
            }
          ]
        };
      case '= any':
        return {
          type: "op",
          op: "=",
          modifier: "any",
          exprs: exprs
        };
      case 'between':
        return {
          type: "op",
          op: "between",
          exprs: [
            lhsExpr, {
              type: "literal",
              value: expr.rhs.value[0]
            }, {
              type: "literal",
              value: expr.rhs.value[1]
            }
          ]
        };
      default:
        return {
          type: "op",
          op: expr.op,
          exprs: exprs
        };
    }
  };

  ExprCompiler.prototype.compileLogicalExpr = function(options) {
    var compiledExprs, expr;
    expr = options.expr;
    compiledExprs = _.map(expr.exprs, (function(_this) {
      return function(e) {
        return _this.compileExpr({
          expr: e,
          tableAlias: options.tableAlias
        });
      };
    })(this));
    compiledExprs = _.compact(compiledExprs);
    if (compiledExprs.length === 1) {
      return compiledExprs[0];
    }
    if (compiledExprs.length === 0) {
      return null;
    }
    return {
      type: "op",
      op: expr.op,
      exprs: compiledExprs
    };
  };

  ExprCompiler.prototype.compileColumnRef = function(column, tableAlias) {
    if (_.isString(column)) {
      return {
        type: "field",
        tableAlias: tableAlias,
        column: column
      };
    }
    return injectTableAlias(column, tableAlias);
  };

  ExprCompiler.prototype.compileTable = function(tableId, alias) {
    var table;
    table = this.schema.getTable(tableId);
    if (!table.jsonql) {
      return {
        type: "table",
        table: tableId,
        alias: alias
      };
    } else {
      return {
        type: "subquery",
        query: table.jsonql,
        alias: alias
      };
    }
  };

  return ExprCompiler;

})();
