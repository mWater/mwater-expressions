"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var ColumnNotFoundException, ExprCompiler, ExprUtils, _, convertToJsonB, injectTableAlias, injectTableAliases, moment, nowExpr, nowMinus24HoursExpr;

_ = require('lodash');
injectTableAlias = require('./injectTableAliases').injectTableAlias;
injectTableAliases = require('./injectTableAliases').injectTableAliases;
ExprUtils = require('./ExprUtils');
moment = require('moment');
ColumnNotFoundException = require('./ColumnNotFoundException'); // now expression: (to_json(now() at time zone 'UTC')#>>'{}')

nowExpr = {
  type: "op",
  op: "#>>",
  exprs: [{
    type: "op",
    op: "to_json",
    exprs: [{
      type: "op",
      op: "at time zone",
      exprs: [{
        type: "op",
        op: "now",
        exprs: []
      }, "UTC"]
    }]
  }, "{}"]
}; // now 24 hours ago: (to_json((now() - interval '24 hour') at time zone 'UTC')#>>'{}')

nowMinus24HoursExpr = {
  type: "op",
  op: "#>>",
  exprs: [{
    type: "op",
    op: "to_json",
    exprs: [{
      type: "op",
      op: "at time zone",
      exprs: [{
        type: "op",
        op: "-",
        exprs: [{
          type: "op",
          op: "now",
          exprs: []
        }, {
          type: "op",
          op: "interval",
          exprs: [{
            type: "literal",
            value: "24 hour"
          }]
        }]
      }, "UTC"]
    }]
  }, "{}"]
}; // Compiles expressions to JsonQL

module.exports = ExprCompiler = /*#__PURE__*/function () {
  // Variable values are lookup of id to variable value
  function ExprCompiler(schema) {
    var variables = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : [];
    var variableValues = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : {};
    (0, _classCallCheck2["default"])(this, ExprCompiler);
    // Compile an expression. Pass expr and tableAlias.
    this.compileExpr = this.compileExpr.bind(this);
    this.schema = schema;
    this.variables = variables;
    this.variableValues = variableValues;
  }

  (0, _createClass2["default"])(ExprCompiler, [{
    key: "compileExpr",
    value: function compileExpr(options) {
      var compiledExpr, expr;
      expr = options.expr; // Handle null

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

        case "variable":
          compiledExpr = this.compileVariableExpr(options);
          break;

        case "count":
          // DEPRECATED
          compiledExpr = null;
          break;

        case "comparison":
          // DEPRECATED
          compiledExpr = this.compileComparisonExpr(options);
          break;

        case "logical":
          // DEPRECATED
          compiledExpr = this.compileLogicalExpr(options);
          break;

        default:
          throw new Error("Expr type ".concat(expr.type, " not supported"));
      }

      return compiledExpr;
    }
  }, {
    key: "compileFieldExpr",
    value: function compileFieldExpr(options) {
      var column, expr, ref;
      expr = options.expr;
      column = this.schema.getColumn(expr.table, expr.column);

      if (!column) {
        throw new ColumnNotFoundException("Column ".concat(expr.table, ".").concat(expr.column, " not found"));
      } // Handle joins specially


      if (column.type === "join") {
        // If id is result
        if ((ref = column.join.type) === '1-1' || ref === 'n-1') {
          // Use scalar to create
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
              exprs: [{
                type: "op",
                op: "array_agg",
                exprs: [this.compileColumnRef(this.schema.getTable(column.join.toTable).primaryKey, "inner")]
              }]
            },
            from: this.compileTable(column.join.toTable, "inner"),
            where: this.compileJoin(expr.table, column, options.tableAlias, "inner"),
            limit: 1 // Limit 1 to be safe

          };
        }
      } // Handle if has expr 


      if (column.expr) {
        return this.compileExpr({
          expr: column.expr,
          tableAlias: options.tableAlias
        });
      } // If column has custom jsonql, use that instead of id


      return this.compileColumnRef(column.jsonql || column.id, options.tableAlias);
    }
  }, {
    key: "compileScalarExpr",
    value: function compileScalarExpr(options) {
      var alias, expr, extraWhere, from, generateAlias, i, j, joinColumn, nextAlias, onClause, orderBy, ordering, ref, scalar, scalarExpr, table, tableAlias, toTable, where;
      expr = options.expr;
      where = null;
      from = null;
      orderBy = null; // Null expr is null

      if (!expr.expr) {
        return null;
      } // Simplify if a join to an id field where the join uses the primary key of the to table


      if (!expr.aggr && !expr.where && expr.joins.length === 1 && expr.expr.type === "id" && this.schema.getColumn(expr.table, expr.joins[0]).join.toColumn === this.schema.getTable(expr.expr.table).primaryKey) {
        return this.compileColumnRef(this.schema.getColumn(expr.table, expr.joins[0]).join.fromColumn, options.tableAlias);
      } // Generate a consistent, semi-unique alias


      generateAlias = function generateAlias(expr, joinIndex) {
        // Make alias-friendly (replace all symbols with _)
        return expr.joins[joinIndex].replace(/[^a-zA-Z0-9]/g, "_").toLowerCase();
      }; // Perform joins


      table = expr.table;
      tableAlias = options.tableAlias; // First join is in where clause

      if (expr.joins && expr.joins.length > 0) {
        joinColumn = this.schema.getColumn(expr.table, expr.joins[0]);

        if (!joinColumn) {
          throw new ColumnNotFoundException("Join column ".concat(expr.table, ":").concat(expr.joins[0], " not found"));
        } // Determine which column join is to


        toTable = joinColumn.type === "join" ? joinColumn.join.toTable : joinColumn.idTable; // Generate a consistent, semi-unique alias

        alias = generateAlias(expr, 0);
        where = this.compileJoin(table, joinColumn, tableAlias, alias);
        from = this.compileTable(toTable, alias); // We are now at j1, which is the to of the first join

        table = toTable;
        tableAlias = alias;
      } // Perform remaining joins


      if (expr.joins.length > 1) {
        for (i = j = 1, ref = expr.joins.length; 1 <= ref ? j < ref : j > ref; i = 1 <= ref ? ++j : --j) {
          joinColumn = this.schema.getColumn(table, expr.joins[i]);

          if (!joinColumn) {
            throw new ColumnNotFoundException("Join column ".concat(table, ":").concat(expr.joins[i], " not found"));
          } // Determine which column join is to


          toTable = joinColumn.type === "join" ? joinColumn.join.toTable : joinColumn.idTable; // Generate a consistent, semi-unique alias

          nextAlias = generateAlias(expr, i);
          onClause = this.compileJoin(table, joinColumn, tableAlias, nextAlias);
          from = {
            type: "join",
            left: from,
            right: this.compileTable(toTable, nextAlias),
            kind: "inner",
            on: onClause
          }; // We are now at jn

          table = toTable;
          tableAlias = nextAlias;
        }
      } // Compile where clause


      if (expr.where) {
        extraWhere = this.compileExpr({
          expr: expr.where,
          tableAlias: tableAlias
        }); // Add to existing 

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
      }); // Aggregate DEPRECATED

      if (expr.aggr) {
        switch (expr.aggr) {
          case "last":
            // Get ordering
            ordering = this.schema.getTable(table).ordering;

            if (!ordering) {
              throw new Error("No ordering defined");
            } // order descending


            orderBy = [{
              expr: this.compileFieldExpr({
                expr: {
                  type: "field",
                  table: table,
                  column: ordering
                },
                tableAlias: tableAlias
              }),
              direction: "desc"
            }];
            break;

          case "sum":
          case "count":
          case "avg":
          case "max":
          case "min":
          case "stdev":
          case "stdevp":
            // Don't include scalarExpr if null
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
            throw new Error("Unknown aggregation ".concat(expr.aggr));
        }
      } // If no expr, return null


      if (!scalarExpr) {
        return null;
      } // If no where, from, orderBy or limit, just return expr for simplicity


      if (!from && !where && !orderBy) {
        return scalarExpr;
      } // Create scalar


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
    } // Compile a join into an on or where clause
    //  fromTableID: column definition
    //  joinColumn: column definition
    //  fromAlias: alias of from table
    //  toAlias: alias of to table

  }, {
    key: "compileJoin",
    value: function compileJoin(fromTableId, joinColumn, fromAlias, toAlias) {
      var toTable; // For join columns

      if (joinColumn.type === "join") {
        if (joinColumn.join.jsonql) {
          return injectTableAliases(joinColumn.join.jsonql, {
            "{from}": fromAlias,
            "{to}": toAlias
          });
        } else {
          return {
            // Use manual columns
            type: "op",
            op: "=",
            exprs: [this.compileColumnRef(joinColumn.join.toColumn, toAlias), this.compileColumnRef(joinColumn.join.fromColumn, fromAlias)]
          };
        }
      } else if (joinColumn.type === "id") {
        // Get to table
        toTable = this.schema.getTable(joinColumn.idTable);
        return {
          // Create equal
          type: "op",
          op: "=",
          exprs: [this.compileFieldExpr({
            expr: {
              type: "field",
              table: fromTableId,
              column: joinColumn.id
            },
            tableAlias: fromAlias
          }), {
            type: "field",
            tableAlias: toAlias,
            column: toTable.primaryKey
          }]
        };
      } else if (joinColumn.type === "id[]") {
        // Get to table
        toTable = this.schema.getTable(joinColumn.idTable);
        return {
          // Create equal
          type: "op",
          op: "=",
          modifier: "any",
          exprs: [{
            type: "field",
            tableAlias: toAlias,
            column: toTable.primaryKey
          }, this.compileFieldExpr({
            expr: {
              type: "field",
              table: fromTableId,
              column: joinColumn.id
            },
            tableAlias: fromAlias
          })]
        };
      } else {
        throw new Error("Invalid join column type ".concat(joinColumn.type));
      }
    } // Compile an expression. Pass expr and tableAlias.

  }, {
    key: "compileOpExpr",
    value: function compileOpExpr(options) {
      var _this = this;

      var compiledExprs, enumValues, expr, expr0Type, exprUtils, filterCompiled, idTable, innerrnQuery, lhsCompiled, orderBy, ordering, outerrnQuery, ref, ref1, ref2, ref3, ref4;
      exprUtils = new ExprUtils(this.schema);
      expr = options.expr;
      compiledExprs = _.map(expr.exprs, function (e) {
        return _this.compileExpr({
          expr: e,
          tableAlias: options.tableAlias
        });
      }); // Get type of expr 0

      expr0Type = exprUtils.getExprType(expr.exprs[0]); // Handle multi

      switch (expr.op) {
        case "and":
        case "or":
          // Strip nulls
          compiledExprs = _.compact(compiledExprs);

          if (compiledExprs.length === 0) {
            return null;
          }

          return {
            type: "op",
            op: expr.op,
            exprs: compiledExprs
          };

        case "*":
          // Strip nulls
          compiledExprs = _.compact(compiledExprs);

          if (compiledExprs.length === 0) {
            return null;
          }

          return {
            // Cast to decimal before multiplying to prevent integer overflow
            type: "op",
            op: expr.op,
            exprs: _.map(compiledExprs, function (e) {
              return {
                type: "op",
                op: "::decimal",
                exprs: [e]
              };
            })
          };

        case "+":
          // Strip nulls
          compiledExprs = _.compact(compiledExprs);

          if (compiledExprs.length === 0) {
            return null;
          }

          return {
            // Cast to decimal before adding to prevent integer overflow. Do cast on internal expr to prevent coalesce mismatch
            type: "op",
            op: expr.op,
            exprs: _.map(compiledExprs, function (e) {
              return {
                type: "op",
                op: "coalesce",
                exprs: [{
                  type: "op",
                  op: "::decimal",
                  exprs: [e]
                }, 0]
              };
            })
          };

        case "-":
          // Null if any not present
          if (_.any(compiledExprs, function (ce) {
            return ce == null;
          })) {
            return null;
          }

          return {
            // Cast to decimal before subtracting to prevent integer overflow
            type: "op",
            op: expr.op,
            exprs: _.map(compiledExprs, function (e) {
              return {
                type: "op",
                op: "::decimal",
                exprs: [e]
              };
            })
          };

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
        case "array_agg":
          // Null if any not present
          if (_.any(compiledExprs, function (ce) {
            return ce == null;
          })) {
            return null;
          }

          return {
            type: "op",
            op: expr.op,
            exprs: compiledExprs
          };

        case "least":
        case "greatest":
          return {
            type: "op",
            op: expr.op,
            exprs: compiledExprs
          };

        case "/":
          // Null if any not present
          if (_.any(compiledExprs, function (ce) {
            return ce == null;
          })) {
            return null;
          }

          return {
            // Cast to decimal before dividing to prevent integer math
            type: "op",
            op: expr.op,
            exprs: [compiledExprs[0], {
              type: "op",
              op: "::decimal",
              exprs: [{
                type: "op",
                op: "nullif",
                exprs: [compiledExprs[1], 0]
              }]
            }]
          };

        case "last":
          // Null if not present
          if (!compiledExprs[0]) {
            return null;
          } // Get ordering


          ordering = (ref = this.schema.getTable(expr.table)) != null ? ref.ordering : void 0;

          if (!ordering) {
            throw new Error("Table ".concat(expr.table, " must be ordered to use last()"));
          }

          return {
            // (array_agg(xyz order by theordering desc nulls last))[1]
            type: "op",
            op: "[]",
            exprs: [{
              type: "op",
              op: "array_agg",
              exprs: [compiledExprs[0]],
              orderBy: [{
                expr: this.compileFieldExpr({
                  expr: {
                    type: "field",
                    table: expr.table,
                    column: ordering
                  },
                  tableAlias: options.tableAlias
                }),
                direction: "desc",
                nulls: "last"
              }]
            }, 1]
          };

        case "last where":
          // Null if not value present
          if (!compiledExprs[0]) {
            return null;
          } // Get ordering


          ordering = (ref1 = this.schema.getTable(expr.table)) != null ? ref1.ordering : void 0;

          if (!ordering) {
            throw new Error("Table ".concat(expr.table, " must be ordered to use last()"));
          } // Simple last if not condition present


          if (!compiledExprs[1]) {
            return {
              // (array_agg(xyz order by theordering desc nulls last))[1]
              type: "op",
              op: "[]",
              exprs: [{
                type: "op",
                op: "array_agg",
                exprs: [compiledExprs[0]],
                orderBy: [{
                  expr: this.compileFieldExpr({
                    expr: {
                      type: "field",
                      table: expr.table,
                      column: ordering
                    },
                    tableAlias: options.tableAlias
                  }),
                  direction: "desc",
                  nulls: "last"
                }]
              }, 1]
            };
          }

          return {
            // Compiles to:
            // (array_agg((case when <condition> then <value> else null end) order by (case when <condition> then 0 else 1 end), <ordering> desc nulls last))[1]
            // which prevents non-matching from appearing
            type: "op",
            op: "[]",
            exprs: [{
              type: "op",
              op: "array_agg",
              exprs: [{
                type: "case",
                cases: [{
                  when: compiledExprs[1],
                  then: compiledExprs[0]
                }],
                "else": null
              }],
              orderBy: [{
                expr: {
                  type: "case",
                  cases: [{
                    when: compiledExprs[1],
                    then: 0
                  }],
                  "else": 1
                }
              }, {
                expr: this.compileFieldExpr({
                  expr: {
                    type: "field",
                    table: expr.table,
                    column: ordering
                  },
                  tableAlias: options.tableAlias
                }),
                direction: "desc",
                nulls: "last"
              }]
            }, 1]
          };

        case "previous":
          // Null if not present
          if (!compiledExprs[0]) {
            return null;
          } // Get ordering


          ordering = (ref2 = this.schema.getTable(expr.table)) != null ? ref2.ordering : void 0;

          if (!ordering) {
            throw new Error("Table ".concat(expr.table, " must be ordered to use previous()"));
          }

          return {
            // (array_agg(xyz order by theordering desc nulls last))[2]
            type: "op",
            op: "[]",
            exprs: [{
              type: "op",
              op: "array_agg",
              exprs: [compiledExprs[0]],
              orderBy: [{
                expr: this.compileFieldExpr({
                  expr: {
                    type: "field",
                    table: expr.table,
                    column: ordering
                  },
                  tableAlias: options.tableAlias
                }),
                direction: "desc",
                nulls: "last"
              }]
            }, 2]
          };

        case "first":
          // Null if not present
          if (!compiledExprs[0]) {
            return null;
          } // Get ordering


          ordering = (ref3 = this.schema.getTable(expr.table)) != null ? ref3.ordering : void 0;

          if (!ordering) {
            throw new Error("Table ".concat(expr.table, " must be ordered to use first()"));
          }

          return {
            // (array_agg(xyz order by theordering asc nulls last))[1]
            type: "op",
            op: "[]",
            exprs: [{
              type: "op",
              op: "array_agg",
              exprs: [compiledExprs[0]],
              orderBy: [{
                expr: this.compileFieldExpr({
                  expr: {
                    type: "field",
                    table: expr.table,
                    column: ordering
                  },
                  tableAlias: options.tableAlias
                }),
                direction: "asc",
                nulls: "last"
              }]
            }, 1]
          };

        case "first where":
          // Null if not value present
          if (!compiledExprs[0]) {
            return null;
          } // Get ordering


          ordering = (ref4 = this.schema.getTable(expr.table)) != null ? ref4.ordering : void 0;

          if (!ordering) {
            throw new Error("Table ".concat(expr.table, " must be ordered to use first where()"));
          } // Simple first if not condition present


          if (!compiledExprs[1]) {
            return {
              // (array_agg(xyz order by theordering asc nulls last))[1]
              type: "op",
              op: "[]",
              exprs: [{
                type: "op",
                op: "array_agg",
                exprs: [compiledExprs[0]],
                orderBy: [{
                  expr: this.compileFieldExpr({
                    expr: {
                      type: "field",
                      table: expr.table,
                      column: ordering
                    },
                    tableAlias: options.tableAlias
                  }),
                  direction: "asc",
                  nulls: "last"
                }]
              }, 1]
            };
          }

          return {
            // Compiles to:
            // (array_agg((case when <condition> then <value> else null end) order by (case when <condition> then 0 else 1 end), <ordering> asc nulls last))[1]
            // which prevents non-matching from appearing
            type: "op",
            op: "[]",
            exprs: [{
              type: "op",
              op: "array_agg",
              exprs: [{
                type: "case",
                cases: [{
                  when: compiledExprs[1],
                  then: compiledExprs[0]
                }],
                "else": null
              }],
              orderBy: [{
                expr: {
                  type: "case",
                  cases: [{
                    when: compiledExprs[1],
                    then: 0
                  }],
                  "else": 1
                }
              }, {
                expr: this.compileFieldExpr({
                  expr: {
                    type: "field",
                    table: expr.table,
                    column: ordering
                  },
                  tableAlias: options.tableAlias
                }),
                direction: "asc",
                nulls: "last"
              }]
            }, 1]
          };

        case '= any':
          // Null if any not present
          if (_.any(compiledExprs, function (ce) {
            return ce == null;
          })) {
            return null;
          } // False if empty list on rhs


          if (expr.exprs[1].type === "literal") {
            if (!expr.exprs[1].value || _.isArray(expr.exprs[1].value) && expr.exprs[1].value.length === 0) {
              return false;
            }
          }

          return {
            type: "op",
            op: "=",
            modifier: "any",
            exprs: compiledExprs
          };

        case "between":
          // Null if first not present
          if (!compiledExprs[0]) {
            return null;
          } // Null if second and third not present


          if (!compiledExprs[1] && !compiledExprs[2]) {
            return null;
          } // >= if third missing


          if (!compiledExprs[2]) {
            return {
              type: "op",
              op: ">=",
              exprs: [compiledExprs[0], compiledExprs[1]]
            };
          } // <= if second missing


          if (!compiledExprs[1]) {
            return {
              type: "op",
              op: "<=",
              exprs: [compiledExprs[0], compiledExprs[2]]
            };
          }

          return {
            // Between
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
            exprs: [{
              type: "op",
              op: "coalesce",
              exprs: [compiledExprs[0], false]
            }]
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
          // Null if either not present
          if (!compiledExprs[0] || !compiledExprs[1]) {
            return null;
          } // Null if no expressions in literal list


          if (compiledExprs[1].type === "literal" && compiledExprs[1].value.length === 0) {
            return null;
          }

          return {
            // Cast both to jsonb and use @>. Also convert both to json first to handle literal arrays
            type: "op",
            op: "@>",
            exprs: [convertToJsonB(compiledExprs[0]), convertToJsonB(compiledExprs[1])]
          };

        case "intersects":
          // Null if either not present
          if (!compiledExprs[0] || !compiledExprs[1]) {
            return null;
          } // Null if no expressions in literal list


          if (compiledExprs[1].type === "literal" && compiledExprs[1].value.length === 0) {
            return null;
          }

          return {
            // Cast to jsonb and use ?| Also convert to json first to handle literal arrays
            type: "op",
            op: "?|",
            exprs: [convertToJsonB(compiledExprs[0]), compiledExprs[1]]
          };

        case "length":
          // 0 if null
          if (compiledExprs[0] == null) {
            return 0;
          }

          return {
            // Cast both to jsonb and use jsonb_array_length. Also convert both to json first to handle literal arrays. Coalesce to 0 so that null is 0
            type: "op",
            op: "coalesce",
            exprs: [{
              type: "op",
              op: "jsonb_array_length",
              exprs: [convertToJsonB(compiledExprs[0])]
            }, 0]
          };

        case "line length":
          // null if null
          if (compiledExprs[0] == null) {
            return null;
          }

          return {
            // ST_Length_Spheroid(ST_Transform(location,4326::integer), 'SPHEROID["GRS_1980",6378137,298.257222101]')
            type: "op",
            op: "ST_Length_Spheroid",
            exprs: [{
              type: "op",
              op: "ST_Transform",
              exprs: [compiledExprs[0], {
                type: "op",
                op: "::integer",
                exprs: [4326]
              }]
            }, 'SPHEROID["GRS_1980",6378137,298.257222101]']
          };

        case "to text":
          // Null if not present
          if (!compiledExprs[0]) {
            return null;
          }

          if (exprUtils.getExprType(expr.exprs[0]) === "enum") {
            // Null if no enum values
            enumValues = exprUtils.getExprEnumValues(expr.exprs[0]);

            if (!enumValues) {
              return null;
            }

            return {
              type: "case",
              input: compiledExprs[0],
              cases: _.map(enumValues, function (ev) {
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
              })
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

        case "to date":
          // Null if not present
          if (!compiledExprs[0]) {
            return null;
          }

          return {
            type: "op",
            op: "substr",
            exprs: [compiledExprs[0], 1, 10]
          };

        case "count where":
          // Null if not present
          if (!compiledExprs[0]) {
            return null;
          }

          return {
            type: "op",
            op: "coalesce",
            exprs: [{
              type: "op",
              op: "sum",
              exprs: [{
                type: "case",
                cases: [{
                  when: compiledExprs[0],
                  then: 1
                }],
                "else": 0
              }]
            }, 0]
          };

        case "percent where":
          // Null if not present
          if (!compiledExprs[0]) {
            return null;
          }

          return {
            // Compiles as sum(case when cond [and basis (if present)] then 100::decimal else 0 end)/sum(1 [or case when basis then 1 else 0 (if present)]) (prevent div by zero)        
            type: "op",
            op: "/",
            exprs: [{
              type: "op",
              op: "sum",
              exprs: [{
                type: "case",
                cases: [{
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
                }],
                "else": 0
              }]
            }, compiledExprs[1] ? {
              type: "op",
              op: "nullif",
              exprs: [{
                type: "op",
                op: "sum",
                exprs: [{
                  type: "case",
                  cases: [{
                    when: compiledExprs[1],
                    then: 1
                  }],
                  "else": 0
                }]
              }, 0]
            } : {
              type: "op",
              op: "sum",
              exprs: [1]
            }]
          };

        case "sum where":
          // Null if not present
          if (!compiledExprs[0]) {
            return null;
          } // Simple sum if not specified where


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
            exprs: [{
              type: "case",
              cases: [{
                when: compiledExprs[1],
                then: compiledExprs[0]
              }],
              "else": 0
            }]
          };

        case "min where":
          // Null if not present
          if (!compiledExprs[0]) {
            return null;
          } // Simple min if not specified where


          if (!compiledExprs[1]) {
            return {
              type: "op",
              op: "min",
              exprs: [compiledExprs[0]]
            };
          }

          return {
            type: "op",
            op: "min",
            exprs: [{
              type: "case",
              cases: [{
                when: compiledExprs[1],
                then: compiledExprs[0]
              }],
              "else": null
            }]
          };

        case "max where":
          // Null if not present
          if (!compiledExprs[0]) {
            return null;
          } // Simple max if not specified where


          if (!compiledExprs[1]) {
            return {
              type: "op",
              op: "max",
              exprs: [compiledExprs[0]]
            };
          }

          return {
            type: "op",
            op: "max",
            exprs: [{
              type: "case",
              cases: [{
                when: compiledExprs[1],
                then: compiledExprs[0]
              }],
              "else": null
            }]
          };

        case "count distinct":
          // Null if not present
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
            // Compiles as count(*) * 100::decimal / sum(count(*)) over()
            type: "op",
            op: "/",
            exprs: [{
              type: "op",
              op: "*",
              exprs: [{
                type: "op",
                op: "count",
                exprs: []
              }, {
                type: "op",
                op: "::decimal",
                exprs: [100]
              }]
            }, {
              type: "op",
              op: "sum",
              exprs: [{
                type: "op",
                op: "count",
                exprs: []
              }],
              over: {}
            }]
          };
        // Hierarchical test that uses ancestry column

        case "within":
          // Null if either not present
          if (!compiledExprs[0] || !compiledExprs[1]) {
            return null;
          } // Get table being used


          idTable = exprUtils.getExprIdTable(expr.exprs[0]); // Prefer ancestryTable

          if (this.schema.getTable(idTable).ancestryTable) {
            return {
              // exists (select null from <ancestryTable> as subwithin where ancestor = compiledExprs[1] and descendant = compiledExprs[0])
              type: "op",
              op: "exists",
              exprs: [{
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
                  exprs: [{
                    type: "op",
                    op: "=",
                    exprs: [{
                      type: "field",
                      tableAlias: "subwithin",
                      column: "ancestor"
                    }, compiledExprs[1]]
                  }, {
                    type: "op",
                    op: "=",
                    exprs: [{
                      type: "field",
                      tableAlias: "subwithin",
                      column: "descendant"
                    }, compiledExprs[0]]
                  }]
                }
              }]
            };
          }

          return {
            type: "op",
            op: "in",
            exprs: [compiledExprs[0], {
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
                exprs: [{
                  type: "field",
                  tableAlias: "subwithin",
                  column: this.schema.getTable(idTable).ancestry
                }, {
                  type: "op",
                  op: "::jsonb",
                  exprs: [{
                    type: "op",
                    op: "json_build_array",
                    exprs: [compiledExprs[1]]
                  }]
                }]
              }
            }]
          };
        // Hierarchical test that uses ancestry column

        case "within any":
          // Null if either not present
          if (!compiledExprs[0] || !compiledExprs[1]) {
            return null;
          } // Get table being used


          idTable = exprUtils.getExprIdTable(expr.exprs[0]); // Prefer ancestryTable

          if (this.schema.getTable(idTable).ancestryTable) {
            return {
              // exists (select null from <ancestryTable> as subwithin where ancestor = any(compiledExprs[1]) and descendant = compiledExprs[0])
              type: "op",
              op: "exists",
              exprs: [{
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
                  exprs: [{
                    type: "op",
                    op: "=",
                    modifier: "any",
                    exprs: [{
                      type: "field",
                      tableAlias: "subwithin",
                      column: "ancestor"
                    }, compiledExprs[1]]
                  }, {
                    type: "op",
                    op: "=",
                    exprs: [{
                      type: "field",
                      tableAlias: "subwithin",
                      column: "descendant"
                    }, compiledExprs[0]]
                  }]
                }
              }]
            };
          } // This older code fails now that admin_regions uses integer pk. Replaced with literal-only code
          // return {
          //   type: "op"
          //   op: "in"
          //   exprs: [
          //     compiledExprs[0]
          //     {
          //       type: "scalar"
          //       expr: @compileColumnRef(@schema.getTable(idTable).primaryKey, "subwithin")
          //       from: { type: "table", table: idTable, alias: "subwithin" }
          //       where: {
          //         type: "op"
          //         op: "?|"
          //         exprs: [
          //           { type: "field", tableAlias: "subwithin", column: @schema.getTable(idTable).ancestry }
          //           compiledExprs[1]
          //         ]
          //       }
          //     }            
          //   ]
          // }
          // If not literal, fail


          if (compiledExprs[1].type !== "literal") {
            throw new Error("Non-literal RHS of within any not supported");
          }

          return {
            type: "op",
            op: "in",
            exprs: [compiledExprs[0], {
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
                exprs: [{
                  type: "field",
                  tableAlias: "subwithin",
                  column: this.schema.getTable(idTable).ancestryText || this.schema.getTable(idTable).ancestry
                }, {
                  type: "literal",
                  value: _.map(compiledExprs[1].value, function (value) {
                    if (_.isNumber(value)) {
                      return "" + value;
                    } else {
                      return value;
                    }
                  })
                }]
              }
            }]
          };

        case "latitude":
          if (!compiledExprs[0]) {
            return null;
          }

          return {
            type: "op",
            op: "ST_Y",
            exprs: [{
              type: "op",
              op: "ST_Centroid",
              exprs: [{
                type: "op",
                op: "ST_Transform",
                exprs: [compiledExprs[0], {
                  type: "op",
                  op: "::integer",
                  exprs: [4326]
                }]
              }]
            }]
          };

        case "longitude":
          if (!compiledExprs[0]) {
            return null;
          }

          return {
            type: "op",
            op: "ST_X",
            exprs: [{
              type: "op",
              op: "ST_Centroid",
              exprs: [{
                type: "op",
                op: "ST_Transform",
                exprs: [compiledExprs[0], {
                  type: "op",
                  op: "::integer",
                  exprs: [4326]
                }]
              }]
            }]
          };

        case 'days difference':
          if (!compiledExprs[0] || !compiledExprs[1]) {
            return null;
          }

          if (exprUtils.getExprType(expr.exprs[0]) === "datetime" || exprUtils.getExprType(expr.exprs[1]) === "datetime") {
            return {
              type: "op",
              op: "/",
              exprs: [{
                type: "op",
                op: "-",
                exprs: [{
                  type: "op",
                  op: "date_part",
                  exprs: ['epoch', {
                    type: "op",
                    op: "::timestamp",
                    exprs: [compiledExprs[0]]
                  }]
                }, {
                  type: "op",
                  op: "date_part",
                  exprs: ['epoch', {
                    type: "op",
                    op: "::timestamp",
                    exprs: [compiledExprs[1]]
                  }]
                }]
              }, 86400]
            };
          }

          if (exprUtils.getExprType(expr.exprs[0]) === "date") {
            return {
              type: "op",
              op: "-",
              exprs: [{
                type: "op",
                op: "::date",
                exprs: [compiledExprs[0]]
              }, {
                type: "op",
                op: "::date",
                exprs: [compiledExprs[1]]
              }]
            };
          }

          return null;

        case 'months difference':
          if (!compiledExprs[0] || !compiledExprs[1]) {
            return null;
          }

          if (exprUtils.getExprType(expr.exprs[0]) === "datetime" || exprUtils.getExprType(expr.exprs[1]) === "datetime") {
            return {
              type: "op",
              op: "/",
              exprs: [{
                type: "op",
                op: "-",
                exprs: [{
                  type: "op",
                  op: "date_part",
                  exprs: ['epoch', {
                    type: "op",
                    op: "::timestamp",
                    exprs: [compiledExprs[0]]
                  }]
                }, {
                  type: "op",
                  op: "date_part",
                  exprs: ['epoch', {
                    type: "op",
                    op: "::timestamp",
                    exprs: [compiledExprs[1]]
                  }]
                }]
              }, 86400 * 30.5]
            };
          }

          if (exprUtils.getExprType(expr.exprs[0]) === "date") {
            return {
              type: "op",
              op: "/",
              exprs: [{
                type: "op",
                op: "-",
                exprs: [{
                  type: "op",
                  op: "::date",
                  exprs: [compiledExprs[0]]
                }, {
                  type: "op",
                  op: "::date",
                  exprs: [compiledExprs[1]]
                }]
              }, 30.5]
            };
          }

          return null;

        case 'years difference':
          if (!compiledExprs[0] || !compiledExprs[1]) {
            return null;
          }

          if (exprUtils.getExprType(expr.exprs[0]) === "datetime" || exprUtils.getExprType(expr.exprs[1]) === "datetime") {
            return {
              type: "op",
              op: "/",
              exprs: [{
                type: "op",
                op: "-",
                exprs: [{
                  type: "op",
                  op: "date_part",
                  exprs: ['epoch', {
                    type: "op",
                    op: "::timestamp",
                    exprs: [compiledExprs[0]]
                  }]
                }, {
                  type: "op",
                  op: "date_part",
                  exprs: ['epoch', {
                    type: "op",
                    op: "::timestamp",
                    exprs: [compiledExprs[1]]
                  }]
                }]
              }, 86400 * 365]
            };
          }

          if (exprUtils.getExprType(expr.exprs[0]) === "date") {
            return {
              type: "op",
              op: "/",
              exprs: [{
                type: "op",
                op: "-",
                exprs: [{
                  type: "op",
                  op: "::date",
                  exprs: [compiledExprs[0]]
                }, {
                  type: "op",
                  op: "::date",
                  exprs: [compiledExprs[1]]
                }]
              }, 365]
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
                exprs: [{
                  type: "op",
                  op: "::date",
                  exprs: [moment().format("YYYY-MM-DD")]
                }, {
                  type: "op",
                  op: "::date",
                  exprs: [compiledExprs[0]]
                }]
              };

            case "datetime":
              return {
                type: "op",
                op: "/",
                exprs: [{
                  type: "op",
                  op: "-",
                  exprs: [{
                    type: "op",
                    op: "date_part",
                    exprs: ['epoch', {
                      type: "op",
                      op: "::timestamp",
                      exprs: [nowExpr]
                    }]
                  }, {
                    type: "op",
                    op: "date_part",
                    exprs: ['epoch', {
                      type: "op",
                      op: "::timestamp",
                      exprs: [compiledExprs[0]]
                    }]
                  }]
                }, 86400]
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
            exprs: [{
              type: "op",
              op: "substr",
              exprs: [compiledExprs[0], 1, 7]
            }, 10, "-01"]
          };

        case 'yearquarter':
          if (!compiledExprs[0]) {
            return null;
          }

          return {
            type: "op",
            op: "to_char",
            exprs: [{
              type: "op",
              op: "::date",
              exprs: [compiledExprs[0]]
            }, "YYYY-Q"]
          };

        case 'yearweek':
          if (!compiledExprs[0]) {
            return null;
          }

          return {
            type: "op",
            op: "to_char",
            exprs: [{
              type: "op",
              op: "::date",
              exprs: [compiledExprs[0]]
            }, "IYYY-IW"]
          };

        case 'weekofyear':
          if (!compiledExprs[0]) {
            return null;
          }

          return {
            type: "op",
            op: "to_char",
            exprs: [{
              type: "op",
              op: "::date",
              exprs: [compiledExprs[0]]
            }, "IW"]
          };

        case 'year':
          if (!compiledExprs[0]) {
            return null;
          }

          return {
            type: "op",
            op: "rpad",
            exprs: [{
              type: "op",
              op: "substr",
              exprs: [compiledExprs[0], 1, 4]
            }, 10, "-01-01"]
          };

        case 'weekofmonth':
          if (!compiledExprs[0]) {
            return null;
          }

          return {
            type: "op",
            op: "to_char",
            exprs: [{
              type: "op",
              op: "::timestamp",
              exprs: [compiledExprs[0]]
            }, "W"]
          };

        case 'dayofmonth':
          if (!compiledExprs[0]) {
            return null;
          }

          return {
            type: "op",
            op: "to_char",
            exprs: [{
              type: "op",
              op: "::timestamp",
              exprs: [compiledExprs[0]]
            }, "DD"]
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
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().startOf("year").format("YYYY-MM-DD")]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("year").add(1, 'years').format("YYYY-MM-DD")]
                }]
              };

            case "datetime":
              return {
                type: "op",
                op: "and",
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().startOf("year").toISOString()]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("year").add(1, 'years').toISOString()]
                }]
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
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().startOf("year").subtract(1, 'years').format("YYYY-MM-DD")]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("year").format("YYYY-MM-DD")]
                }]
              };

            case "datetime":
              return {
                type: "op",
                op: "and",
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().startOf("year").subtract(1, 'years').toISOString()]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("year").toISOString()]
                }]
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
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().startOf("month").format("YYYY-MM-DD")]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("month").add(1, 'months').format("YYYY-MM-DD")]
                }]
              };

            case "datetime":
              return {
                type: "op",
                op: "and",
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().startOf("month").toISOString()]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("month").add(1, 'months').toISOString()]
                }]
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
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().startOf("month").subtract(1, 'months').format("YYYY-MM-DD")]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("month").format("YYYY-MM-DD")]
                }]
              };

            case "datetime":
              return {
                type: "op",
                op: "and",
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().startOf("month").subtract(1, 'months').toISOString()]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("month").toISOString()]
                }]
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
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().format("YYYY-MM-DD")]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD")]
                }]
              };

            case "datetime":
              return {
                type: "op",
                op: "and",
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().startOf("day").toISOString()]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString()]
                }]
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
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().subtract(1, 'days').format("YYYY-MM-DD")]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().format("YYYY-MM-DD")]
                }]
              };

            case "datetime":
              return {
                type: "op",
                op: "and",
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().startOf("day").subtract(1, 'days').toISOString()]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("day").toISOString()]
                }]
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
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().subtract(1, 'days').format("YYYY-MM-DD")]
                }, {
                  type: "op",
                  op: "<=",
                  exprs: [compiledExprs[0], moment().format("YYYY-MM-DD")]
                }]
              };

            case "datetime":
              return {
                type: "op",
                op: "and",
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], nowMinus24HoursExpr]
                }, {
                  type: "op",
                  op: "<=",
                  exprs: [compiledExprs[0], nowExpr]
                }]
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
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().subtract(7, 'days').format("YYYY-MM-DD")]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD")]
                }]
              };

            case "datetime":
              return {
                type: "op",
                op: "and",
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().startOf("day").subtract(7, 'days').toISOString()]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString()]
                }]
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
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().subtract(30, 'days').format("YYYY-MM-DD")]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD")]
                }]
              };

            case "datetime":
              return {
                type: "op",
                op: "and",
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().startOf("day").subtract(30, 'days').toISOString()]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString()]
                }]
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
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().subtract(365, 'days').format("YYYY-MM-DD")]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD")]
                }]
              };

            case "datetime":
              return {
                type: "op",
                op: "and",
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().startOf("day").subtract(365, 'days').toISOString()]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString()]
                }]
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
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().subtract(11, "months").startOf('month').format("YYYY-MM-DD")]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD")]
                }]
              };

            case "datetime":
              return {
                type: "op",
                op: "and",
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().subtract(11, "months").startOf('month').toISOString()]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString()]
                }]
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
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().subtract(5, "months").startOf('month').format("YYYY-MM-DD")]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD")]
                }]
              };

            case "datetime":
              return {
                type: "op",
                op: "and",
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().subtract(5, "months").startOf('month').toISOString()]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString()]
                }]
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
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().subtract(2, "months").startOf('month').format("YYYY-MM-DD")]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD")]
                }]
              };

            case "datetime":
              return {
                type: "op",
                op: "and",
                exprs: [{
                  type: "op",
                  op: ">=",
                  exprs: [compiledExprs[0], moment().subtract(2, "months").startOf('month').toISOString()]
                }, {
                  type: "op",
                  op: "<",
                  exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString()]
                }]
              };

            default:
              return null;
          }

          break;

        case 'future':
          if (!compiledExprs[0]) {
            return null;
          }

          switch (expr0Type) {
            case "date":
              return {
                type: "op",
                op: ">",
                exprs: [compiledExprs[0], moment().format("YYYY-MM-DD")]
              };

            case "datetime":
              return {
                type: "op",
                op: ">",
                exprs: [compiledExprs[0], nowExpr]
              };

            default:
              return null;
          }

          break;

        case 'notfuture':
          if (!compiledExprs[0]) {
            return null;
          }

          switch (expr0Type) {
            case "date":
              return {
                type: "op",
                op: "<=",
                exprs: [compiledExprs[0], moment().format("YYYY-MM-DD")]
              };

            case "datetime":
              return {
                type: "op",
                op: "<=",
                exprs: [compiledExprs[0], nowExpr]
              };

            default:
              return null;
          }

          break;

        case 'current date':
          return {
            type: "literal",
            value: moment().format("YYYY-MM-DD")
          };

        case 'current datetime':
          return {
            type: "literal",
            value: moment().toISOString()
          };

        case 'distance':
          if (!compiledExprs[0] || !compiledExprs[1]) {
            return null;
          }

          return {
            type: "op",
            op: "ST_DistanceSphere",
            exprs: [{
              type: "op",
              op: "ST_Transform",
              exprs: [compiledExprs[0], {
                type: "op",
                op: "::integer",
                exprs: [4326]
              }]
            }, {
              type: "op",
              op: "ST_Transform",
              exprs: [compiledExprs[1], {
                type: "op",
                op: "::integer",
                exprs: [4326]
              }]
            }]
          };

        case 'is latest':
          lhsCompiled = this.compileExpr({
            expr: expr.exprs[0],
            tableAlias: "innerrn"
          });

          if (!lhsCompiled) {
            return null;
          }

          filterCompiled = this.compileExpr({
            expr: expr.exprs[1],
            tableAlias: "innerrn"
          }); // Get ordering

          ordering = this.schema.getTable(expr.table).ordering;

          if (!ordering) {
            throw new Error("No ordering defined");
          } // order descending


          orderBy = [{
            expr: this.compileFieldExpr({
              expr: {
                type: "field",
                table: expr.table,
                column: ordering
              },
              tableAlias: "innerrn"
            }),
            direction: "desc"
          }]; // _id in (select outerrn.id from (select innerrn.id, row_number() over (partition by EXPR1 order by ORDERING desc) as rn from the_table as innerrn where filter) as outerrn where outerrn.rn = 1)
          // Create innerrn query

          innerrnQuery = {
            type: "query",
            selects: [{
              type: "select",
              expr: this.compileExpr({
                expr: {
                  type: "id",
                  table: expr.table
                },
                tableAlias: "innerrn"
              }),
              alias: "id"
            }, {
              type: "select",
              expr: {
                type: "op",
                op: "row_number",
                exprs: [],
                over: {
                  partitionBy: [lhsCompiled],
                  orderBy: orderBy
                }
              },
              alias: "rn"
            }],
            from: {
              type: "table",
              table: expr.table,
              alias: "innerrn"
            }
          };

          if (filterCompiled) {
            innerrnQuery.where = filterCompiled;
          } // Wrap in outer query


          outerrnQuery = {
            type: "scalar",
            expr: {
              type: "field",
              tableAlias: "outerrn",
              column: "id"
            },
            from: {
              type: "subquery",
              query: innerrnQuery,
              alias: "outerrn"
            },
            where: {
              type: "op",
              op: "=",
              exprs: [{
                type: "field",
                tableAlias: "outerrn",
                column: "rn"
              }, 1]
            }
          };
          return {
            type: "op",
            op: "in",
            exprs: [this.compileExpr({
              expr: {
                type: "id",
                table: expr.table
              },
              tableAlias: options.tableAlias
            }), outerrnQuery]
          };

        default:
          throw new Error("Unknown op ".concat(expr.op));
      }
    }
  }, {
    key: "compileCaseExpr",
    value: function compileCaseExpr(options) {
      var _this2 = this;

      var compiled, expr;
      expr = options.expr;
      compiled = {
        type: "case",
        cases: _.map(expr.cases, function (c) {
          return {
            when: _this2.compileExpr({
              expr: c.when,
              tableAlias: options.tableAlias
            }),
            then: _this2.compileExpr({
              expr: c.then,
              tableAlias: options.tableAlias
            })
          };
        }),
        "else": this.compileExpr({
          expr: expr["else"],
          tableAlias: options.tableAlias
        })
      }; // Remove null cases

      compiled.cases = _.filter(compiled.cases, function (c) {
        return c.when != null;
      }); // Return null if no cases

      if (compiled.cases.length === 0) {
        return null;
      }

      return compiled;
    }
  }, {
    key: "compileScoreExpr",
    value: function compileScoreExpr(options) {
      var _this3 = this;

      var expr, exprUtils, inputType;
      expr = options.expr;
      exprUtils = new ExprUtils(this.schema); // If empty, literal 0

      if (_.isEmpty(expr.scores)) {
        return {
          type: "literal",
          value: 0
        };
      } // Get type of input


      inputType = exprUtils.getExprType(expr.input);

      switch (inputType) {
        case "enum":
          return {
            type: "case",
            input: this.compileExpr({
              expr: expr.input,
              tableAlias: options.tableAlias
            }),
            cases: _.map(_.pairs(expr.scores), function (pair) {
              return {
                when: {
                  type: "literal",
                  value: pair[0]
                },
                then: _this3.compileExpr({
                  expr: pair[1],
                  tableAlias: options.tableAlias
                })
              };
            }),
            "else": {
              type: "literal",
              value: 0
            }
          };

        case "enumset":
          return {
            type: "op",
            op: "+",
            exprs: _.map(_.pairs(expr.scores), function (pair) {
              return {
                type: "case",
                cases: [{
                  when: {
                    type: "op",
                    op: "@>",
                    exprs: [convertToJsonB(_this3.compileExpr({
                      expr: expr.input,
                      tableAlias: options.tableAlias
                    })), convertToJsonB({
                      type: "literal",
                      value: [pair[0]]
                    })]
                  },
                  then: _this3.compileExpr({
                    expr: pair[1],
                    tableAlias: options.tableAlias
                  })
                }],
                "else": {
                  type: "literal",
                  value: 0
                }
              };
            })
          };

        default:
          // Null if no expression
          return null;
      }
    }
  }, {
    key: "compileBuildEnumsetExpr",
    value: function compileBuildEnumsetExpr(options) {
      var _this4 = this;

      var expr; // Create enumset
      // select to_jsonb(array_agg(bes.v)) from (select (case when true then 'x' end) as v union all select (case when true then 'y' end) as v ...) as bes where v is not null

      expr = options.expr; // Handle empty case

      if (_.keys(expr.values).length === 0) {
        return null;
      }

      return {
        type: "scalar",
        expr: {
          type: "op",
          op: "to_jsonb",
          exprs: [{
            type: "op",
            op: "array_agg",
            exprs: [{
              type: "field",
              tableAlias: "bes",
              column: "v"
            }]
          }]
        },
        from: {
          type: "subquery",
          alias: "bes",
          query: {
            type: "union all",
            queries: _.map(_.pairs(expr.values), function (pair) {
              return {
                type: "query",
                selects: [{
                  type: "select",
                  expr: {
                    type: "case",
                    cases: [{
                      when: _this4.compileExpr({
                        expr: pair[1],
                        tableAlias: options.tableAlias
                      }),
                      then: pair[0]
                    }]
                  },
                  alias: "v"
                }]
              };
            })
          }
        },
        where: {
          type: "op",
          op: "is not null",
          exprs: [{
            type: "field",
            tableAlias: "bes",
            column: "v"
          }]
        }
      };
    }
  }, {
    key: "compileComparisonExpr",
    value: function compileComparisonExpr(options) {
      var expr, exprLhsType, exprUtils, exprs, lhsExpr, rhsExpr;
      expr = options.expr;
      exprUtils = new ExprUtils(this.schema); // Missing left-hand side type means null condition

      exprLhsType = exprUtils.getExprType(expr.lhs);

      if (!exprLhsType) {
        return null;
      } // Missing right-hand side means null condition


      if (exprUtils.getComparisonRhsType(exprLhsType, expr.op) && expr.rhs == null) {
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
      } // Handle special cases 


      switch (expr.op) {
        case '= true':
          return {
            type: "op",
            op: "=",
            exprs: [lhsExpr, {
              type: "literal",
              value: true
            }]
          };

        case '= false':
          return {
            type: "op",
            op: "=",
            exprs: [lhsExpr, {
              type: "literal",
              value: false
            }]
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
            exprs: [lhsExpr, {
              type: "literal",
              value: expr.rhs.value[0]
            }, {
              type: "literal",
              value: expr.rhs.value[1]
            }]
          };

        default:
          return {
            type: "op",
            op: expr.op,
            exprs: exprs
          };
      }
    }
  }, {
    key: "compileLogicalExpr",
    value: function compileLogicalExpr(options) {
      var _this5 = this;

      var compiledExprs, expr;
      expr = options.expr;
      compiledExprs = _.map(expr.exprs, function (e) {
        return _this5.compileExpr({
          expr: e,
          tableAlias: options.tableAlias
        });
      }); // Remove nulls

      compiledExprs = _.compact(compiledExprs); // Simplify

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
    } // Compiles a reference to a column or a JsonQL expression
    // If parameter is a string, create a simple field expression
    // If parameter is an object, inject tableAlias for `{alias}`

  }, {
    key: "compileColumnRef",
    value: function compileColumnRef(column, tableAlias) {
      if (_.isString(column)) {
        return {
          type: "field",
          tableAlias: tableAlias,
          column: column
        };
      }

      return injectTableAlias(column, tableAlias);
    } // Compiles a table, substituting with custom jsonql if required

  }, {
    key: "compileTable",
    value: function compileTable(tableId, alias) {
      var table;
      table = this.schema.getTable(tableId);

      if (!table) {
        throw new Error("Table ".concat(tableId, " not found"));
      }

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
    }
  }, {
    key: "compileVariableExpr",
    value: function compileVariableExpr(options) {
      var value, variable; // Get variable

      variable = _.findWhere(this.variables, {
        id: options.expr.variableId
      });

      if (!variable) {
        throw new Error("Variable ".concat(options.expr.variableId, " not found"));
      } // Get value


      value = this.variableValues[variable.id]; // If expression, compile

      if (variable.table) {
        return this.compileExpr({
          expr: value,
          tableAlias: options.tableAlias
        });
      } else if (value != null) {
        return {
          type: "literal",
          value: value
        };
      } else {
        return null;
      }
    }
  }]);
  return ExprCompiler;
}(); // Converts a compiled expression to jsonb. Literals cannot use to_jsonb as they will
// trigger "could not determine polymorphic type because input has type unknown" unless the 
// SQL is inlined


convertToJsonB = function convertToJsonB(compiledExpr) {
  if (!compiledExpr) {
    return compiledExpr;
  } // Literals are special and are cast to jsonb from a JSON string


  if (compiledExpr.type === "literal") {
    return {
      type: "op",
      op: "::jsonb",
      exprs: [{
        type: "literal",
        value: JSON.stringify(compiledExpr.value)
      }]
    };
  }

  return {
    // First convert using to_jsonb in case is array
    type: "op",
    op: "to_jsonb",
    exprs: [compiledExpr]
  };
};