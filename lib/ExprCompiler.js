var ColumnNotFoundException, ExprCompiler, ExprUtils, _, aliasNum, generateAlias, injectTableAlias, injectTableAliases, moment,
  bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };

_ = require('lodash');

injectTableAlias = require('./injectTableAlias');

injectTableAliases = require('./injectTableAliases');

ExprUtils = require('./ExprUtils');

moment = require('moment');

ColumnNotFoundException = require('./ColumnNotFoundException');

aliasNum = 1;

generateAlias = function() {
  var alias;
  alias = "j" + aliasNum;
  aliasNum += 1;
  return alias;
};

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
      case "score":
        compiledExpr = this.compileScoreExpr(options);
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
    var alias, expr, extraWhere, from, i, j, join, joinColumn, limit, nextAlias, onClause, orderBy, ordering, ref, scalar, scalarExpr, table, tableAlias, where;
    expr = options.expr;
    where = null;
    from = null;
    orderBy = null;
    limit = null;
    if (!expr.aggr && !expr.where && expr.joins.length === 1 && expr.expr.type === "id" && this.schema.getColumn(expr.table, expr.joins[0]).join.toColumn === this.schema.getTable(expr.expr.table).primaryKey) {
      return this.compileColumnRef(this.schema.getColumn(expr.table, expr.joins[0]).join.fromColumn, options.tableAlias);
    }
    table = expr.table;
    tableAlias = options.tableAlias;
    if (expr.joins && expr.joins.length > 0) {
      joinColumn = this.schema.getColumn(expr.table, expr.joins[0]);
      if (!joinColumn) {
        throw new ColumnNotFoundException("Join column " + expr.table + ":" + expr.joins[0] + " not found");
      }
      join = joinColumn.join;
      alias = generateAlias();
      if (join.jsonql) {
        where = injectTableAliases(join.jsonql, {
          "{from}": tableAlias,
          "{to}": alias
        });
      } else {
        where = {
          type: "op",
          op: "=",
          exprs: [this.compileColumnRef(join.toColumn, alias), this.compileColumnRef(join.fromColumn, tableAlias)]
        };
      }
      from = this.compileTable(join.toTable, alias);
      table = join.toTable;
      tableAlias = alias;
    }
    if (expr.joins.length > 1) {
      for (i = j = 1, ref = expr.joins.length; 1 <= ref ? j < ref : j > ref; i = 1 <= ref ? ++j : --j) {
        joinColumn = this.schema.getColumn(table, expr.joins[i]);
        if (!joinColumn) {
          throw new ColumnNotFoundException("Join column " + expr.table + ":" + expr.joins[0] + " not found");
        }
        join = joinColumn.join;
        nextAlias = generateAlias();
        if (join.jsonql) {
          onClause = injectTableAliases(join.jsonql, {
            "{from}": tableAlias,
            "{to}": nextAlias
          });
        } else {
          onClause = {
            type: "op",
            op: "=",
            exprs: [this.compileColumnRef(join.fromColumn, tableAlias), this.compileColumnRef(join.toColumn, nextAlias)]
          };
        }
        from = {
          type: "join",
          left: from,
          right: this.compileTable(join.toTable, nextAlias),
          kind: "left",
          on: onClause
        };
        table = join.toTable;
        tableAlias = nextAlias;
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
    var compiledExprs, expr, expr0Type, exprUtils, idTable;
    exprUtils = new ExprUtils(this.schema);
    expr = options.expr;
    compiledExprs = _.map(expr.exprs, (function(_this) {
      return function(e) {
        return _this.compileExpr({
          expr: e,
          tableAlias: options.tableAlias
        });
      };
    })(this));
    expr0Type = exprUtils.getExprType(expr.exprs[0]);
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
        if (compiledExprs[1].type === "literal" && compiledExprs[1].value.length === 0) {
          return null;
        }
        return {
          type: "op",
          op: "@>",
          exprs: [
            {
              type: "op",
              op: "::jsonb",
              exprs: [
                {
                  type: "op",
                  op: "to_json",
                  exprs: [compiledExprs[0]]
                }
              ]
            }, {
              type: "op",
              op: "::jsonb",
              exprs: [
                {
                  type: "op",
                  op: "to_json",
                  exprs: [compiledExprs[1]]
                }
              ]
            }
          ]
        };
      case "within":
        if (!compiledExprs[0] || !compiledExprs[1]) {
          return null;
        }
        idTable = exprUtils.getExprIdTable(expr.exprs[0]);
        return {
          type: "op",
          op: "in",
          exprs: [
            compiledExprs[0], {
              type: "scalar",
              expr: {
                type: "field",
                tableAlias: "subwithin",
                column: this.schema.getTable(idTable).primaryKey
              },
              from: {
                type: "table",
                table: idTable,
                alias: "subwithin"
              },
              where: {
                type: "op",
                op: "@>",
                exprs: [
                  {
                    type: "field",
                    tableAlias: "subwithin",
                    column: this.schema.getTable(idTable).ancestry
                  }, {
                    type: "op",
                    op: "::jsonb",
                    exprs: [
                      {
                        type: "op",
                        op: "json_build_array",
                        exprs: [compiledExprs[1]]
                      }
                    ]
                  }
                ]
              }
            }
          ]
        };
      case "latitude":
        if (!compiledExprs[0]) {
          return null;
        }
        return {
          type: "op",
          op: "ST_Y",
          exprs: [
            {
              type: "op",
              op: "ST_Transform",
              exprs: [compiledExprs[0], 4326]
            }
          ]
        };
      case "longitude":
        if (!compiledExprs[0]) {
          return null;
        }
        return {
          type: "op",
          op: "ST_X",
          exprs: [
            {
              type: "op",
              op: "ST_Transform",
              exprs: [compiledExprs[0], 4326]
            }
          ]
        };
      case 'thisyear':
        if (!compiledExprs[0]) {
          return null;
        }
        switch (expr0Type) {
          case "date":
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
          case "datetime":
            return {
              type: "op",
              op: "and",
              exprs: [
                {
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().startOf("year").toISOString()]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("year").add(1, 'years').toISOString()]
                }
              ]
            };
          default:
            return null;
        }
        break;
      case 'lastyear':
        if (!compiledExprs[0]) {
          return null;
        }
        switch (expr0Type) {
          case "date":
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
          case "datetime":
            return {
              type: "op",
              op: "and",
              exprs: [
                {
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().startOf("year").subtract(1, 'years').toISOString()]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("year").toISOString()]
                }
              ]
            };
          default:
            return null;
        }
        break;
      case 'thismonth':
        if (!compiledExprs[0]) {
          return null;
        }
        switch (expr0Type) {
          case "date":
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
          case "datetime":
            return {
              type: "op",
              op: "and",
              exprs: [
                {
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().startOf("month").toISOString()]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("month").add(1, 'months').toISOString()]
                }
              ]
            };
          default:
            return null;
        }
        break;
      case 'lastmonth':
        if (!compiledExprs[0]) {
          return null;
        }
        switch (expr0Type) {
          case "date":
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
          case "datetime":
            return {
              type: "op",
              op: "and",
              exprs: [
                {
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().startOf("month").subtract(1, 'months').toISOString()]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("month").toISOString()]
                }
              ]
            };
          default:
            return null;
        }
        break;
      case 'today':
        if (!compiledExprs[0]) {
          return null;
        }
        switch (expr0Type) {
          case "date":
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
          case "datetime":
            return {
              type: "op",
              op: "and",
              exprs: [
                {
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().startOf("day").toISOString()]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString()]
                }
              ]
            };
          default:
            return null;
        }
        break;
      case 'yesterday':
        if (!compiledExprs[0]) {
          return null;
        }
        switch (expr0Type) {
          case "date":
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
          case "datetime":
            return {
              type: "op",
              op: "and",
              exprs: [
                {
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().startOf("day").subtract(1, 'days').toISOString()]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("day").toISOString()]
                }
              ]
            };
          default:
            return null;
        }
        break;
      case 'last7days':
        if (!compiledExprs[0]) {
          return null;
        }
        switch (expr0Type) {
          case "date":
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
          case "datetime":
            return {
              type: "op",
              op: "and",
              exprs: [
                {
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().startOf("day").subtract(7, 'days').toISOString()]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString()]
                }
              ]
            };
          default:
            return null;
        }
        break;
      case 'last30days':
        if (!compiledExprs[0]) {
          return null;
        }
        switch (expr0Type) {
          case "date":
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
          case "datetime":
            return {
              type: "op",
              op: "and",
              exprs: [
                {
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().startOf("day").subtract(30, 'days').toISOString()]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString()]
                }
              ]
            };
          default:
            return null;
        }
        break;
      case 'last365days':
        if (!compiledExprs[0]) {
          return null;
        }
        switch (expr0Type) {
          case "date":
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
          case "datetime":
            return {
              type: "op",
              op: "and",
              exprs: [
                {
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().startOf("day").subtract(365, 'days').toISOString()]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString()]
                }
              ]
            };
          default:
            return null;
        }
        break;
      case 'distance':
        if (!compiledExprs[0] || !compiledExprs[1]) {
          return null;
        }
        return {
          type: "op",
          op: "ST_Distance_Sphere",
          exprs: [
            {
              type: "op",
              op: "ST_Transform",
              exprs: [compiledExprs[0], 4326]
            }, {
              type: "op",
              op: "ST_Transform",
              exprs: [compiledExprs[1], 4326]
            }
          ]
        };
      default:
        throw new Error("Unknown op " + expr.op);
    }
  };

  ExprCompiler.prototype.compileCaseExpr = function(options) {
    var compiled, expr;
    expr = options.expr;
    compiled = {
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
    compiled.cases = _.filter(compiled.cases, function(c) {
      return c.when != null;
    });
    if (compiled.cases.length === 0) {
      return null;
    }
    return compiled;
  };

  ExprCompiler.prototype.compileScoreExpr = function(options) {
    var expr, exprUtils, inputType;
    expr = options.expr;
    exprUtils = new ExprUtils(this.schema);
    if (_.isEmpty(expr.scores)) {
      return {
        type: "literal",
        value: 0
      };
    }
    inputType = exprUtils.getExprType(expr.input);
    switch (inputType) {
      case "enum":
        return {
          type: "case",
          input: {
            type: "field",
            tableAlias: "T1",
            column: "enum"
          },
          cases: _.map(_.pairs(expr.scores), (function(_this) {
            return function(pair) {
              return {
                when: {
                  type: "literal",
                  value: pair[0]
                },
                then: _this.compileExpr({
                  expr: pair[1],
                  tableAlias: options.tableAlias
                })
              };
            };
          })(this)),
          "else": {
            type: "literal",
            value: 0
          }
        };
      case "enumset":
        return {
          type: "op",
          op: "+",
          exprs: _.map(_.pairs(expr.scores), (function(_this) {
            return function(pair) {
              return {
                type: "case",
                cases: [
                  {
                    when: {
                      type: "op",
                      op: "@>",
                      exprs: [
                        {
                          type: "op",
                          op: "::jsonb",
                          exprs: [
                            {
                              type: "op",
                              op: "to_json",
                              exprs: [
                                _this.compileExpr({
                                  expr: expr.input,
                                  tableAlias: options.tableAlias
                                })
                              ]
                            }
                          ]
                        }, {
                          type: "op",
                          op: "::jsonb",
                          exprs: [
                            {
                              type: "op",
                              op: "to_json",
                              exprs: [
                                {
                                  type: "literal",
                                  value: [pair[0]]
                                }
                              ]
                            }
                          ]
                        }
                      ]
                    },
                    then: _this.compileExpr({
                      expr: pair[1],
                      tableAlias: options.tableAlias
                    })
                  }
                ],
                "else": {
                  type: "literal",
                  value: 0
                }
              };
            };
          })(this))
        };
      default:
        return null;
    }
  };

  ExprCompiler.prototype.compileComparisonExpr = function(options) {
    var expr, exprLhsType, exprUtils, exprs, lhsExpr, rhsExpr;
    expr = options.expr;
    exprUtils = new ExprUtils(this.schema);
    exprLhsType = exprUtils.getExprType(expr.lhs);
    if (!exprLhsType) {
      return null;
    }
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

  ExprCompiler.prototype.testResetAlias = function() {
    return aliasNum = 1;
  };

  return ExprCompiler;

})();
