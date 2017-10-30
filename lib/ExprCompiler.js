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
        compiledExpr = this.compileColumnRef(this.schema.getTable(expr.table).primaryKey, options.tableAlias);
        break;
      case "field":
        compiledExpr = this.compileFieldExpr(options);
        break;
      case "scalar":
        compiledExpr = this.compileScalarExpr(options);
        break;
      case "literal":
        if (expr.value != null) {
          compiledExpr = {
            type: "literal",
            value: expr.value
          };
        } else {
          compiledExpr = null;
        }
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
      case "build enumset":
        compiledExpr = this.compileBuildEnumsetExpr(options);
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
    var column, expr, ref;
    expr = options.expr;
    column = this.schema.getColumn(expr.table, expr.column);
    if (!column) {
      throw new ColumnNotFoundException("Column " + expr.table + "." + expr.column + " not found");
    }
    if (column.type === "join") {
      if ((ref = column.join.type) === '1-1' || ref === 'n-1') {
        return this.compileScalarExpr({
          expr: {
            type: "scalar",
            table: expr.table,
            joins: [column.id],
            expr: {
              type: "id",
              table: column.join.toTable
            }
          },
          tableAlias: options.tableAlias
        });
      } else {
        return {
          type: "scalar",
          expr: {
            type: "op",
            op: "to_jsonb",
            exprs: [
              {
                type: "op",
                op: "array_agg",
                exprs: [this.compileColumnRef(this.schema.getTable(column.join.toTable).primaryKey, "inner")]
              }
            ]
          },
          from: this.compileTable(column.join.toTable, "inner"),
          where: this.compileJoin(column.join, options.tableAlias, "inner"),
          limit: 1
        };
      }
    }
    if (column.expr) {
      return this.compileExpr({
        expr: column.expr,
        tableAlias: options.tableAlias
      });
    }
    return this.compileColumnRef(column.jsonql || column.id, options.tableAlias);
  };

  ExprCompiler.prototype.compileScalarExpr = function(options) {
    var alias, expr, extraWhere, from, generateAlias, i, j, join, joinColumn, nextAlias, onClause, orderBy, ordering, ref, scalar, scalarExpr, table, tableAlias, where;
    expr = options.expr;
    where = null;
    from = null;
    orderBy = null;
    if (!expr.expr) {
      return null;
    }
    if (!expr.aggr && !expr.where && expr.joins.length === 1 && expr.expr.type === "id" && this.schema.getColumn(expr.table, expr.joins[0]).join.toColumn === this.schema.getTable(expr.expr.table).primaryKey) {
      return this.compileColumnRef(this.schema.getColumn(expr.table, expr.joins[0]).join.fromColumn, options.tableAlias);
    }
    generateAlias = function(expr, joinIndex) {
      return expr.joins[joinIndex].replace(/[^a-zA-Z0-9]/g, "_").toLowerCase();
    };
    table = expr.table;
    tableAlias = options.tableAlias;
    if (expr.joins && expr.joins.length > 0) {
      joinColumn = this.schema.getColumn(expr.table, expr.joins[0]);
      if (!joinColumn) {
        throw new ColumnNotFoundException("Join column " + expr.table + ":" + expr.joins[0] + " not found");
      }
      join = joinColumn.join;
      alias = generateAlias(expr, 0);
      where = this.compileJoin(join, tableAlias, alias);
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
        nextAlias = generateAlias(expr, i);
        onClause = this.compileJoin(join, tableAlias, nextAlias);
        from = {
          type: "join",
          left: from,
          right: this.compileTable(join.toTable, nextAlias),
          kind: "inner",
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
    if (!scalarExpr) {
      return null;
    }
    if (!from && !where && !orderBy) {
      return scalarExpr;
    }
    scalar = {
      type: "scalar",
      expr: scalarExpr,
      limit: 1
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
    return scalar;
  };

  ExprCompiler.prototype.compileJoin = function(join, fromAlias, toAlias) {
    if (join.jsonql) {
      return injectTableAliases(join.jsonql, {
        "{from}": fromAlias,
        "{to}": toAlias
      });
    } else {
      return {
        type: "op",
        op: "=",
        exprs: [this.compileColumnRef(join.toColumn, toAlias), this.compileColumnRef(join.fromColumn, fromAlias)]
      };
    }
  };

  ExprCompiler.prototype.compileOpExpr = function(options) {
    var compiledExprs, enumValues, expr, expr0Type, exprUtils, idTable, ordering, ref, ref1, ref2;
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
      case "+":
        compiledExprs = _.compact(compiledExprs);
        if (compiledExprs.length === 0) {
          return null;
        }
        return {
          type: "op",
          op: expr.op,
          exprs: _.map(compiledExprs, function(e) {
            return {
              type: "op",
              op: "coalesce",
              exprs: [e, 0]
            };
          })
        };
      case "-":
      case ">":
      case "<":
      case ">=":
      case "<=":
      case "<>":
      case "=":
      case "~*":
      case "round":
      case "floor":
      case "ceiling":
      case "sum":
      case "avg":
      case "min":
      case "max":
      case "count":
      case "stdev":
      case "stdevp":
      case "var":
      case "varp":
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
      case "/":
        if (_.any(compiledExprs, function(ce) {
          return ce == null;
        })) {
          return null;
        }
        return {
          type: "op",
          op: expr.op,
          exprs: [
            compiledExprs[0], {
              type: "op",
              op: "nullif",
              exprs: [compiledExprs[1], 0]
            }
          ]
        };
      case "last":
        if (!compiledExprs[0]) {
          return null;
        }
        ordering = (ref = this.schema.getTable(expr.table)) != null ? ref.ordering : void 0;
        if (!ordering) {
          throw new Error("Table " + expr.table + " must be ordered to use last()");
        }
        return {
          type: "op",
          op: "[]",
          exprs: [
            {
              type: "op",
              op: "array_agg",
              exprs: [compiledExprs[0]],
              orderBy: [
                {
                  expr: this.compileColumnRef(ordering, options.tableAlias),
                  direction: "desc",
                  nulls: "last"
                }
              ]
            }, 1
          ]
        };
      case "last where":
        if (!compiledExprs[0]) {
          return null;
        }
        ordering = (ref1 = this.schema.getTable(expr.table)) != null ? ref1.ordering : void 0;
        if (!ordering) {
          throw new Error("Table " + expr.table + " must be ordered to use last()");
        }
        if (!compiledExprs[1]) {
          return {
            type: "op",
            op: "[]",
            exprs: [
              {
                type: "op",
                op: "array_agg",
                exprs: [compiledExprs[0]],
                orderBy: [
                  {
                    expr: this.compileColumnRef(ordering, options.tableAlias),
                    direction: "desc",
                    nulls: "last"
                  }
                ]
              }, 1
            ]
          };
        }
        return {
          type: "op",
          op: "[]",
          exprs: [
            {
              type: "op",
              op: "array_agg",
              exprs: [
                {
                  type: "case",
                  cases: [
                    {
                      when: compiledExprs[1],
                      then: compiledExprs[0]
                    }
                  ],
                  "else": null
                }
              ],
              orderBy: [
                {
                  expr: {
                    type: "case",
                    cases: [
                      {
                        when: compiledExprs[1],
                        then: 0
                      }
                    ],
                    "else": 1
                  }
                }, {
                  expr: this.compileColumnRef(ordering, options.tableAlias),
                  direction: "desc",
                  nulls: "last"
                }
              ]
            }, 1
          ]
        };
      case "previous":
        if (!compiledExprs[0]) {
          return null;
        }
        ordering = (ref2 = this.schema.getTable(expr.table)) != null ? ref2.ordering : void 0;
        if (!ordering) {
          throw new Error("Table " + expr.table + " must be ordered to use previous()");
        }
        return {
          type: "op",
          op: "[]",
          exprs: [
            {
              type: "op",
              op: "array_agg",
              exprs: [compiledExprs[0]],
              orderBy: [
                {
                  expr: this.compileColumnRef(ordering, options.tableAlias),
                  direction: "desc",
                  nulls: "last"
                }
              ]
            }, 2
          ]
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
        if (!compiledExprs[0]) {
          return null;
        }
        return {
          type: "op",
          op: expr.op,
          exprs: [
            {
              type: "op",
              op: "coalesce",
              exprs: [compiledExprs[0], false]
            }
          ]
        };
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
      case "intersects":
        if (!compiledExprs[0] || !compiledExprs[1]) {
          return null;
        }
        if (compiledExprs[1].type === "literal" && compiledExprs[1].value.length === 0) {
          return null;
        }
        return {
          type: "op",
          op: "?|",
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
            }, compiledExprs[1]
          ]
        };
      case "length":
        if (!compiledExprs[0]) {
          return null;
        }
        return {
          type: "op",
          op: "jsonb_array_length",
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
            }
          ]
        };
      case "to text":
        if (!compiledExprs[0]) {
          return null;
        }
        if (exprUtils.getExprType(expr.exprs[0]) === "enum") {
          enumValues = exprUtils.getExprEnumValues(expr.exprs[0]);
          if (!enumValues) {
            return null;
          }
          return {
            type: "case",
            input: compiledExprs[0],
            cases: _.map(enumValues, (function(_this) {
              return function(ev) {
                return {
                  when: {
                    type: "literal",
                    value: ev.id
                  },
                  then: {
                    type: "literal",
                    value: exprUtils.localizeString(ev.name, expr.locale)
                  }
                };
              };
            })(this))
          };
        }
        if (exprUtils.getExprType(expr.exprs[0]) === "number") {
          return {
            type: "op",
            op: "::text",
            exprs: [compiledExprs[0]]
          };
        }
        return null;
      case "count where":
        if (!compiledExprs[0]) {
          return null;
        }
        return {
          type: "op",
          op: "sum",
          exprs: [
            {
              type: "case",
              cases: [
                {
                  when: compiledExprs[0],
                  then: 1
                }
              ],
              "else": 0
            }
          ]
        };
      case "percent where":
        if (!compiledExprs[0]) {
          return null;
        }
        return {
          type: "op",
          op: "/",
          exprs: [
            {
              type: "op",
              op: "sum",
              exprs: [
                {
                  type: "case",
                  cases: [
                    {
                      when: compiledExprs[1] ? {
                        type: "op",
                        op: "and",
                        exprs: [compiledExprs[0], compiledExprs[1]]
                      } : compiledExprs[0],
                      then: {
                        type: "op",
                        op: "::decimal",
                        exprs: [100]
                      }
                    }
                  ],
                  "else": 0
                }
              ]
            }, compiledExprs[1] ? {
              type: "op",
              op: "nullif",
              exprs: [
                {
                  type: "op",
                  op: "sum",
                  exprs: [
                    {
                      type: "case",
                      cases: [
                        {
                          when: compiledExprs[1],
                          then: 1
                        }
                      ],
                      "else": 0
                    }
                  ]
                }, 0
              ]
            } : {
              type: "op",
              op: "sum",
              exprs: [1]
            }
          ]
        };
      case "sum where":
        if (!compiledExprs[0]) {
          return null;
        }
        if (!compiledExprs[1]) {
          return {
            type: "op",
            op: "sum",
            exprs: [compiledExprs[0]]
          };
        }
        return {
          type: "op",
          op: "sum",
          exprs: [
            {
              type: "case",
              cases: [
                {
                  when: compiledExprs[1],
                  then: compiledExprs[0]
                }
              ],
              "else": 0
            }
          ]
        };
      case "count distinct":
        if (!compiledExprs[0]) {
          return null;
        }
        return {
          type: "op",
          op: "count",
          exprs: [compiledExprs[0]],
          modifier: "distinct"
        };
      case "percent":
        return {
          type: "op",
          op: "/",
          exprs: [
            {
              type: "op",
              op: "*",
              exprs: [
                {
                  type: "op",
                  op: "count",
                  exprs: []
                }, {
                  type: "op",
                  op: "::decimal",
                  exprs: [100]
                }
              ]
            }, {
              type: "op",
              op: "sum",
              exprs: [
                {
                  type: "op",
                  op: "count",
                  exprs: []
                }
              ],
              over: {}
            }
          ]
        };
      case "within":
        if (!compiledExprs[0] || !compiledExprs[1]) {
          return null;
        }
        idTable = exprUtils.getExprIdTable(expr.exprs[0]);
        if (this.schema.getTable(idTable).ancestryTable) {
          return {
            type: "op",
            op: "exists",
            exprs: [
              {
                type: "scalar",
                expr: null,
                from: {
                  type: "table",
                  table: this.schema.getTable(idTable).ancestryTable,
                  alias: "subwithin"
                },
                where: {
                  type: "op",
                  op: "and",
                  exprs: [
                    {
                      type: "op",
                      op: "=",
                      exprs: [
                        {
                          type: "field",
                          tableAlias: "subwithin",
                          column: "ancestor"
                        }, compiledExprs[1]
                      ]
                    }, {
                      type: "op",
                      op: "=",
                      exprs: [
                        {
                          type: "field",
                          tableAlias: "subwithin",
                          column: "descendant"
                        }, compiledExprs[0]
                      ]
                    }
                  ]
                }
              }
            ]
          };
        }
        return {
          type: "op",
          op: "in",
          exprs: [
            compiledExprs[0], {
              type: "scalar",
              expr: this.compileColumnRef(this.schema.getTable(idTable).primaryKey, "subwithin"),
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
      case "within any":
        if (!compiledExprs[0] || !compiledExprs[1]) {
          return null;
        }
        idTable = exprUtils.getExprIdTable(expr.exprs[0]);
        if (this.schema.getTable(idTable).ancestryTable) {
          return {
            type: "op",
            op: "exists",
            exprs: [
              {
                type: "scalar",
                expr: null,
                from: {
                  type: "table",
                  table: this.schema.getTable(idTable).ancestryTable,
                  alias: "subwithin"
                },
                where: {
                  type: "op",
                  op: "and",
                  exprs: [
                    {
                      type: "op",
                      op: "=",
                      modifier: "any",
                      exprs: [
                        {
                          type: "field",
                          tableAlias: "subwithin",
                          column: "ancestor"
                        }, compiledExprs[1]
                      ]
                    }, {
                      type: "op",
                      op: "=",
                      exprs: [
                        {
                          type: "field",
                          tableAlias: "subwithin",
                          column: "descendant"
                        }, compiledExprs[0]
                      ]
                    }
                  ]
                }
              }
            ]
          };
        }
        if (compiledExprs[1].type !== "literal") {
          throw new Error("Non-literal RHS of within any not supported");
        }
        return {
          type: "op",
          op: "in",
          exprs: [
            compiledExprs[0], {
              type: "scalar",
              expr: this.compileColumnRef(this.schema.getTable(idTable).primaryKey, "subwithin"),
              from: {
                type: "table",
                table: idTable,
                alias: "subwithin"
              },
              where: {
                type: "op",
                op: "?|",
                exprs: [
                  {
                    type: "field",
                    tableAlias: "subwithin",
                    column: this.schema.getTable(idTable).ancestry
                  }, {
                    type: "literal",
                    value: _.map(compiledExprs[1].value, (function(_this) {
                      return function(value) {
                        if (_.isNumber(value)) {
                          return "" + value;
                        } else {
                          return value;
                        }
                      };
                    })(this))
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
      case 'days difference':
        if (!compiledExprs[0] || !compiledExprs[1]) {
          return null;
        }
        if (exprUtils.getExprType(expr.exprs[0]) === "date") {
          return {
            type: "op",
            op: "-",
            exprs: [
              {
                type: "op",
                op: "::date",
                exprs: [compiledExprs[0]]
              }, {
                type: "op",
                op: "::date",
                exprs: [compiledExprs[1]]
              }
            ]
          };
        }
        if (exprUtils.getExprType(expr.exprs[0]) === "datetime") {
          return {
            type: "op",
            op: "/",
            exprs: [
              {
                type: "op",
                op: "-",
                exprs: [
                  {
                    type: "op",
                    op: "date_part",
                    exprs: [
                      'epoch', {
                        type: "op",
                        op: "::timestamp",
                        exprs: [compiledExprs[0]]
                      }
                    ]
                  }, {
                    type: "op",
                    op: "date_part",
                    exprs: [
                      'epoch', {
                        type: "op",
                        op: "::timestamp",
                        exprs: [compiledExprs[1]]
                      }
                    ]
                  }
                ]
              }, 86400
            ]
          };
        }
        return null;
      case 'days since':
        if (!compiledExprs[0]) {
          return null;
        }
        switch (expr0Type) {
          case "date":
            return {
              type: "op",
              op: "-",
              exprs: [
                {
                  type: "op",
                  op: "::date",
                  exprs: [moment().format("YYYY-MM-DD")]
                }, {
                  type: "op",
                  op: "::date",
                  exprs: [compiledExprs[0]]
                }
              ]
            };
          case "datetime":
            return {
              type: "op",
              op: "/",
              exprs: [
                {
                  type: "op",
                  op: "-",
                  exprs: [
                    {
                      type: "op",
                      op: "date_part",
                      exprs: [
                        'epoch', {
                          type: "op",
                          op: "::timestamp",
                          exprs: [moment().toISOString()]
                        }
                      ]
                    }, {
                      type: "op",
                      op: "date_part",
                      exprs: [
                        'epoch', {
                          type: "op",
                          op: "::timestamp",
                          exprs: [compiledExprs[0]]
                        }
                      ]
                    }
                  ]
                }, 86400
              ]
            };
          default:
            return null;
        }
        break;
      case 'month':
        if (!compiledExprs[0]) {
          return null;
        }
        return {
          type: "op",
          op: "substr",
          exprs: [compiledExprs[0], 6, 2]
        };
      case 'yearmonth':
        if (!compiledExprs[0]) {
          return null;
        }
        return {
          type: "op",
          op: "rpad",
          exprs: [
            {
              type: "op",
              op: "substr",
              exprs: [compiledExprs[0], 1, 7]
            }, 10, "-01"
          ]
        };
      case 'year':
        if (!compiledExprs[0]) {
          return null;
        }
        return {
          type: "op",
          op: "rpad",
          exprs: [
            {
              type: "op",
              op: "substr",
              exprs: [compiledExprs[0], 1, 4]
            }, 10, "-01-01"
          ]
        };
      case 'weekofmonth':
        if (!compiledExprs[0]) {
          return null;
        }
        return {
          type: "op",
          op: "to_char",
          exprs: [
            {
              type: "op",
              op: "::timestamp",
              exprs: [compiledExprs[0]]
            }, "W"
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
      case 'last24hours':
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
                  op: "<=",
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
                  exprs: [compiledExprs[0], moment().subtract(24, 'hours').toISOString()]
                }, {
                  type: "op",
                  op: "<=",
                  exprs: [compiledExprs[0], moment().toISOString()]
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
      case 'last12months':
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
                  exprs: [compiledExprs[0], moment().subtract(11, "months").startOf('month').format("YYYY-MM-DD")]
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
                  exprs: [compiledExprs[0], moment().subtract(11, "months").startOf('month').toISOString()]
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
      case 'last6months':
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
                  exprs: [compiledExprs[0], moment().subtract(5, "months").startOf('month').format("YYYY-MM-DD")]
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
                  exprs: [compiledExprs[0], moment().subtract(5, "months").startOf('month').toISOString()]
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
      case 'last3months':
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
                  exprs: [compiledExprs[0], moment().subtract(2, "months").startOf('month').format("YYYY-MM-DD")]
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
                  exprs: [compiledExprs[0], moment().subtract(2, "months").startOf('month').toISOString()]
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
          op: "ST_DistanceSphere",
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
          input: this.compileExpr({
            expr: expr.input,
            tableAlias: options.tableAlias
          }),
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

  ExprCompiler.prototype.compileBuildEnumsetExpr = function(options) {
    var expr;
    expr = options.expr;
    return {
      type: "scalar",
      expr: {
        type: "op",
        op: "to_jsonb",
        exprs: [
          {
            type: "op",
            op: "array_agg",
            exprs: [
              {
                type: "field",
                tableAlias: "bes",
                column: "v"
              }
            ]
          }
        ]
      },
      from: {
        type: "subquery",
        alias: "bes",
        query: {
          type: "union all",
          queries: _.map(_.pairs(expr.values), (function(_this) {
            return function(pair) {
              return {
                type: "query",
                selects: [
                  {
                    type: "select",
                    expr: {
                      type: "case",
                      cases: [
                        {
                          when: _this.compileExpr({
                            expr: pair[1],
                            tableAlias: options.tableAlias
                          }),
                          then: pair[0]
                        }
                      ]
                    },
                    alias: "v"
                  }
                ]
              };
            };
          })(this))
        }
      },
      where: {
        type: "op",
        op: "is not null",
        exprs: [
          {
            type: "field",
            tableAlias: "bes",
            column: "v"
          }
        ]
      }
    };
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

  return ExprCompiler;

})();
