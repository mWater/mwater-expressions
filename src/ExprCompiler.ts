import { JsonQLCase, JsonQLExpr, JsonQLFrom, JsonQLLiteral, JsonQLQuery, JsonQLScalar, JsonQLSelectQuery, JsonQLTableFrom } from "jsonql";
import _ from "lodash";
import moment from "moment";
import ColumnNotFoundException from "./ColumnNotFoundException";
import ExprUtils from "./ExprUtils";
import { getExprExtension } from "./extensions";
import { injectTableAlias, injectTableAliases } from "./injectTableAliases";
import Schema from "./Schema";
import { BuildEnumsetExpr, CaseExpr, Column, Expr, FieldExpr, LegacyComparisonExpr, LegacyLogicalExpr, LiteralExpr, OpExpr, ScalarExpr, ScoreExpr, Variable, VariableExpr } from "./types";

// now expression: (to_json(now() at time zone 'UTC')#>>'{}')
const nowExpr: JsonQLExpr = {
  type: "op",
  op: "#>>",
  exprs: [
    { type: "op", op: "to_json", exprs: [
      { type: "op", op: "at time zone", exprs: [
        { type: "op", op: "now", exprs: [] },
        "UTC"
      ]}
    ]},
    "{}"
  ]
};

// now 24 hours ago: (to_json((now() - interval '24 hour') at time zone 'UTC')#>>'{}')
const nowMinus24HoursExpr: JsonQLExpr = {
  type: "op",
  op: "#>>",
  exprs: [
    { type: "op", op: "to_json", exprs: [
      { type: "op", op: "at time zone", exprs: [
        { type: "op", op: "-", exprs: [{ type: "op", op: "now", exprs: [] }, { type: "op", op: "interval", exprs: [{ type: "literal", value: "24 hour" }]}] },
        "UTC"
      ]}
    ]},
    "{}"
  ]
};


/** Compiles expressions to JsonQL. Assumes that geometry is in Webmercator (3857) */
export default class ExprCompiler { 
  schema: Schema
  variables: Variable[]
  variableValues: { [variableId: string]: Expr }

  // Variable values are lookup of id to variable value, which is always an expression
  constructor(schema: Schema, variables?: Variable[], variableValues?: { [variableId: string]: Expr }) {
    this.schema = schema
    this.variables = variables || []
    this.variableValues = variableValues || {}
  }

  /** Compile an expression. Pass expr and tableAlias. */
  compileExpr(options: { expr: Expr, tableAlias: string }): JsonQLExpr {
    const { expr, tableAlias } = options

    // Handle null
    if (!expr) {
      return null
    }

    switch (expr.type) { 
      case "id":
        return this.compileColumnRef(this.schema.getTable(expr.table)!.primaryKey, options.tableAlias)
      case "field":
        return this.compileFieldExpr({ expr, tableAlias })
      case "scalar":
        return this.compileScalarExpr({ expr, tableAlias })
      case "literal": 
        if (expr.value != null) {
          return { type: "literal", value: expr.value }
        } else {
          return null;
        }
      case "op":
        return this.compileOpExpr({ expr, tableAlias })
      case "case":
        return this.compileCaseExpr({ expr, tableAlias })
      case "score":
        return this.compileScoreExpr({ expr, tableAlias })
      case "build enumset":
        return this.compileBuildEnumsetExpr({ expr, tableAlias })
      case "variable":
        return this.compileVariableExpr({ expr, tableAlias })
      case "extension":
        return getExprExtension(expr.extension).compileExpr(expr, tableAlias, this.schema, this.variables, this.variableValues);
      case "count": // DEPRECATED
        return null;
      case "comparison": // DEPRECATED
        return this.compileComparisonExpr({ expr, tableAlias });
      case "logical": // DEPRECATED
        return this.compileLogicalExpr({ expr, tableAlias });
      default:
        throw new Error(`Expr type ${(expr as any).type} not supported`);
    }
  }

  /** Compile a field expressions */
  compileFieldExpr(options: { expr: FieldExpr, tableAlias: string }): JsonQLExpr {
    const { expr } = options

    const column = this.schema.getColumn(expr.table, expr.column);
    if (!column) {
      throw new ColumnNotFoundException(`Column ${expr.table}.${expr.column} not found`);
    }

    // Handle joins specially
    if (column.type === "join") {
      // If id is result
      if (['1-1', 'n-1'].includes(column.join!.type)) {
        // Use scalar to create
        return this.compileScalarExpr({expr: { type: "scalar", table: expr.table, joins: [column.id], expr: { type: "id", table: column.join!.toTable }}, tableAlias: options.tableAlias});
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
                exprs: [
                  this.compileColumnRef(this.schema.getTable(column.join!.toTable)!.primaryKey, "inner")
                ]
              }
            ]
          },
          from: this.compileTable(column.join!.toTable, "inner"),
          where: this.compileJoin(expr.table, column, options.tableAlias, "inner"),
          limit: 1  // Limit 1 to be safe
        };
      }
    }

    // Handle if has expr 
    if (column.expr) {
      return this.compileExpr({ expr: column.expr, tableAlias: options.tableAlias });
    }

    // If column has custom jsonql, use that instead of id
    return this.compileColumnRef(column.jsonql || column.id, options.tableAlias);
  }

  compileScalarExpr(options: { expr: ScalarExpr, tableAlias: string }): JsonQLExpr {
    let joinColumn, toTable;
    const { expr } = options;

    let where = null;
    let from: JsonQLFrom | undefined = undefined;
    let orderBy: { expr: JsonQLExpr, direction: "asc" | "desc" }[] | undefined = undefined;

    // Null expr is null
    if (!expr.expr) {
      return null
    }

    // Simplify if a join to an id field where the join uses the primary key of the to table
    if (!expr.aggr && !expr.where && (expr.joins.length === 1) && (expr.expr.type === "id")) { 
      const fromColumn = this.schema.getColumn(expr.table, expr.joins[0])!

      if (fromColumn.type === "id") {
        return this.compileColumnRef(fromColumn.id, options.tableAlias);
      }
      if (fromColumn.join && (fromColumn.join.toColumn === this.schema.getTable(expr.expr.table)!.primaryKey)) {
        return this.compileColumnRef(fromColumn.join.fromColumn, options.tableAlias);
      }
    }

    // Generate a consistent, semi-unique alias. Make alias-friendly (replace all symbols with _)
    const generateAlias = (expr: ScalarExpr, joinIndex: number) =>  
      expr.joins[joinIndex].replace(/[^a-zA-Z0-9]/g, "_").toLowerCase()

    // Perform joins
    let { table } = expr
    let { tableAlias } = options

    // First join is in where clause
    if (expr.joins && (expr.joins.length > 0)) {
      joinColumn = this.schema.getColumn(expr.table, expr.joins[0]);
      if (!joinColumn) {
        throw new ColumnNotFoundException(`Join column ${expr.table}:${expr.joins[0]} not found`);
      }

      // Determine which column join is to
      toTable = joinColumn.type === "join" ? joinColumn.join!.toTable : joinColumn.idTable!

      // Generate a consistent, semi-unique alias
      const alias = generateAlias(expr, 0);

      where = this.compileJoin(table, joinColumn, tableAlias, alias);

      from = this.compileTable(toTable, alias);

      // We are now at j1, which is the to of the first join
      table = toTable
      tableAlias = alias
    }

    // Perform remaining joins
    if (expr.joins.length > 1) {
      for (let i = 1, end = expr.joins.length, asc = 1 <= end; asc ? i < end : i > end; asc ? i++ : i--) {
        joinColumn = this.schema.getColumn(table, expr.joins[i]);
        if (!joinColumn) {
          throw new ColumnNotFoundException(`Join column ${table}:${expr.joins[i]} not found`);
        }

        // Determine which column join is to
        toTable = joinColumn.type === "join" ? joinColumn.join!.toTable : joinColumn.idTable!

        // Generate a consistent, semi-unique alias
        const nextAlias = generateAlias(expr, i);

        const onClause = this.compileJoin(table, joinColumn, tableAlias, nextAlias);

        from = {
          type: "join",
          left: from!,
          right: this.compileTable(toTable, nextAlias),
          kind: "inner",
          on: onClause
        };

        // We are now at jn
        table = toTable;
        tableAlias = nextAlias;
      }
    }

    // Compile where clause
    if (expr.where) {
      const extraWhere = this.compileExpr({expr: expr.where, tableAlias});

      // Add to existing 
      if (where) {
        where = { type: "op", op: "and", exprs: [where, extraWhere]};
      } else {
        where = extraWhere;
      }
    }

    let scalarExpr = this.compileExpr({expr: expr.expr, tableAlias});
    
    // Aggregate DEPRECATED
    if (expr.aggr) {
      switch (expr.aggr) {
        case "last":
          // Get ordering
          var { ordering } = this.schema.getTable(table)!
          if (!ordering) {
            throw new Error("No ordering defined");
          }

          // order descending
          orderBy = [{ expr: this.compileFieldExpr({expr: { type: "field", table, column: ordering}, tableAlias}), direction: "desc" }]
          break;
        case "sum": case "count": case "avg": case "max": case "min": case "stdev": case "stdevp":
          // Don't include scalarExpr if null
          if (!scalarExpr) {
            scalarExpr = { type: "op", op: expr.aggr, exprs: [] };
          } else {
            scalarExpr = { type: "op", op: expr.aggr, exprs: [scalarExpr] };
          }
          break;
        default:
          throw new Error(`Unknown aggregation ${expr.aggr}`);
      }
    }

    // If no expr, return null
    if (!scalarExpr) {
      // TODO extend to include null!
      return (null as unknown) as JsonQLExpr
    }

    // If no where, from, orderBy or limit, just return expr for simplicity
    if (!from && !where && !orderBy) {
      return scalarExpr;
    }

    // Create scalar
    const scalar: JsonQLScalar = {
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
  }

  /** Compile a join into an on or where clause
   *  fromTableID: column definition
   *  joinColumn: column definition
   *  fromAlias: alias of from table
   *  toAlias: alias of to table
   */
  compileJoin(fromTableId: string, joinColumn: Column, fromAlias: string, toAlias: string) {
    // For join columns
    let toTable;
    if (joinColumn.type === "join") {
      if (joinColumn.join!.jsonql) {
        return injectTableAliases(joinColumn.join!.jsonql, { "{from}": fromAlias, "{to}": toAlias });
      } else {
        // Use manual columns
        return { 
          type: "op", op: "=",
          exprs: [
            this.compileColumnRef(joinColumn.join!.toColumn, toAlias),
            this.compileColumnRef(joinColumn.join!.fromColumn, fromAlias)
          ]
        };
      }
    } else if (joinColumn.type === "id") {
      // Get to table
      toTable = this.schema.getTable(joinColumn.idTable!)!

      // Create equal
      return { 
        type: "op", op: "=",
        exprs: [
          this.compileFieldExpr({ expr: { type: "field", table: fromTableId, column: joinColumn.id }, tableAlias: fromAlias}),
          { type: "field", tableAlias: toAlias, column: toTable.primaryKey }
        ]
      };
    } else if (joinColumn.type === "id[]") {
      // Get to table
      toTable = this.schema.getTable(joinColumn.idTable!)!

      const compiledFrom = this.compileFieldExpr({ expr: { type: "field", table: fromTableId, column: joinColumn.id }, tableAlias: fromAlias})
      const compiledTo: JsonQLExpr = { type: "field", tableAlias: toAlias, column: toTable.primaryKey! }

      // Use to_jsonb(fromTable.fromColumn) @> to_jsonb(toTable.toColumn)
      return { 
        type: "op", op: "@>", 
        exprs: [convertToJsonB(compiledFrom), convertToJsonB(compiledTo)]
      }
    } else {
      throw new Error(`Invalid join column type ${joinColumn.type}`);
    }
  }

  // Compile an expression. Pass expr and tableAlias.
  compileOpExpr(options: { expr: OpExpr, tableAlias: string }): JsonQLExpr {
    var ordering: string | undefined
    const exprUtils = new ExprUtils(this.schema)

    const {
      expr
    } = options;

    let compiledExprs = _.map(expr.exprs, e => this.compileExpr({expr: e, tableAlias: options.tableAlias}));

    // Get type of expr 0
    const expr0Type = exprUtils.getExprType(expr.exprs[0]);

    // Handle multi
    switch (expr.op) {
      case "and": case "or":
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

        // Cast to decimal before multiplying to prevent integer overflow
        return { 
          type: "op",
          op: expr.op,
          exprs: _.map(compiledExprs, e => ({
            type: "op",
            op: "::decimal",
            exprs: [e]
          }))
        };
      case "+":
        // Strip nulls
        compiledExprs = _.compact(compiledExprs);
        if (compiledExprs.length === 0) {
          return null;
        }

        // Cast to decimal before adding to prevent integer overflow. Do cast on internal expr to prevent coalesce mismatch
        return { 
          type: "op",
          op: expr.op,
          exprs: _.map(compiledExprs, e => ({
            type: "op",
            op: "coalesce",
            exprs: [{ type: "op", op: "::decimal", exprs: [e] }, 0]
          } as JsonQLExpr))
        };
      case "-":
        // Null if any not present
        if (_.any(compiledExprs, ce => ce == null)) {
          return null;
        }

        // Cast to decimal before subtracting to prevent integer overflow
        return { 
          type: "op",
          op: expr.op,
          exprs: _.map(compiledExprs, e => ({
            type: "op",
            op: "::decimal",
            exprs: [e]
          }))
        };
      case ">": case "<": case ">=": case "<=": case "<>": case "=": case "~*": case "round": case "floor": case "ceiling": case "sum": case "avg": case "min": case "max": case "count": case "stdev": case "stdevp": case "var": case "varp": case "array_agg":
        // Null if any not present
        if (_.any(compiledExprs, ce => ce == null)) {
          return null;
        }

        return { 
          type: "op",
          op: expr.op,
          exprs: compiledExprs
        };
      case "least": case "greatest":
        return { 
          type: "op",
          op: expr.op,
          exprs: compiledExprs
        };
      case "/":
        // Null if any not present
        if (_.any(compiledExprs, ce => ce == null)) {
          return null;
        }

        // Cast to decimal before dividing to prevent integer math
        return { 
          type: "op",
          op: expr.op,
          exprs: [
            compiledExprs[0],
            { type: "op", op: "::decimal", exprs: [{ type: "op", op: "nullif", exprs: [compiledExprs[1], 0] }] }
          ]
        };
      case "last":
        // Null if not present
        if (compiledExprs[0] == null) {
          return null;
        }

        // Get ordering
        ordering = this.schema.getTable(expr.table!)!.ordering
        if (!ordering) {
          throw new Error(`Table ${expr.table} must be ordered to use last()`);
        }

        // (array_agg(xyz order by theordering desc nulls last))[1]
        return { 
          type: "op",
          op: "[]",
          exprs: [
            { type: "op", op: "array_agg", exprs: [compiledExprs[0]], orderBy: [{ expr: this.compileFieldExpr({expr: { type: "field", table: expr.table!, column: ordering}, tableAlias: options.tableAlias}), direction: "desc", nulls: "last" }] },
            1
          ]
        }

      case "last where":
        // Null if not value present
        if (compiledExprs[0] == null) {
          return null;
        }

        // Get ordering
        ordering = this.schema.getTable(expr.table!)!.ordering
        if (!ordering) {
          throw new Error(`Table ${expr.table} must be ordered to use last()`);
        }

        // Simple last if not condition present
        if (compiledExprs[1] == null) {
          // (array_agg(xyz order by theordering desc nulls last))[1]
          return { 
            type: "op",
            op: "[]",
            exprs: [
              { type: "op", op: "array_agg", exprs: [compiledExprs[0]], orderBy: [{ expr: this.compileFieldExpr({expr: { type: "field", table: expr.table!, column: ordering}, tableAlias: options.tableAlias}), direction: "desc", nulls: "last" }] },
              1
            ]
          };
        }

        // Compiles to:
        // (array_agg((case when <condition> then <value> else null end) order by (case when <condition> then 0 else 1 end), <ordering> desc nulls last))[1]
        // which prevents non-matching from appearing
        return { 
          type: "op",
          op: "[]",
          exprs: [
            { 
              type: "op",
              op: "array_agg",
              exprs: [
                { type: "case", cases: [{ when: compiledExprs[1], then: compiledExprs[0] }], else: null }
              ],
              orderBy: [
                { expr: { type: "case", cases: [{ when: compiledExprs[1], then: 0 }], else: 1 } },
                { expr: this.compileFieldExpr({expr: { type: "field", table: expr.table!, column: ordering}, tableAlias: options.tableAlias}), direction: "desc", nulls: "last" }
              ] 
            },
            1
          ]
        };

      case "previous":
        // Null if not present
        if (compiledExprs[0] == null) {
          return null;
        }

        // Get ordering
        ordering = this.schema.getTable(expr.table!)!.ordering
        if (!ordering) {
          throw new Error(`Table ${expr.table} must be ordered to use previous()`);
        }

        // (array_agg(xyz order by theordering desc nulls last))[2]
        return { 
          type: "op",
          op: "[]",
          exprs: [
            { type: "op", op: "array_agg", exprs: [compiledExprs[0]], orderBy: [{ expr: this.compileFieldExpr({expr: { type: "field", table: expr.table!, column: ordering }, tableAlias: options.tableAlias}), direction: "desc", nulls: "last" }] },
            2
          ]
        };

      case "first":
        // Null if not present
        if (compiledExprs[0] == null) {
          return null;
        }

        // Get ordering
        ordering = this.schema.getTable(expr.table!)!.ordering
        if (!ordering) {
          throw new Error(`Table ${expr.table} must be ordered to use first()`);
        }

        // (array_agg(xyz order by theordering asc nulls last))[1]
        return { 
          type: "op",
          op: "[]",
          exprs: [
            { type: "op", op: "array_agg", exprs: [compiledExprs[0]], orderBy: [{ expr: this.compileFieldExpr({expr: { type: "field", table: expr.table!, column: ordering}, tableAlias: options.tableAlias}), direction: "asc", nulls: "last" }] },
            1
          ]
        };

      case "first where":
        // Null if not value present
        if (compiledExprs[0] == null) {
          return null;
        }

        // Get ordering
        ordering = this.schema.getTable(expr.table!)!.ordering
        if (!ordering) {
          throw new Error(`Table ${expr.table} must be ordered to use first where()`);
        }

        // Simple first if not condition present
        if (compiledExprs[1] == null) {
          // (array_agg(xyz order by theordering asc nulls last))[1]
          return { 
            type: "op",
            op: "[]",
            exprs: [
              { type: "op", op: "array_agg", exprs: [compiledExprs[0]], orderBy: [{ expr: this.compileFieldExpr({expr: { type: "field", table: expr.table!, column: ordering}, tableAlias: options.tableAlias}), direction: "asc", nulls: "last" }] },
              1
            ]
          };
        }

        // Compiles to:
        // (array_agg((case when <condition> then <value> else null end) order by (case when <condition> then 0 else 1 end), <ordering> asc nulls last))[1]
        // which prevents non-matching from appearing
        return { 
          type: "op",
          op: "[]",
          exprs: [
            { 
              type: "op",
              op: "array_agg",
              exprs: [
                { type: "case", cases: [{ when: compiledExprs[1], then: compiledExprs[0] }], else: null }
              ],
              orderBy: [
                { expr: { type: "case", cases: [{ when: compiledExprs[1], then: 0 }], else: 1 } },
                { expr: this.compileFieldExpr({expr: { type: "field", table: expr.table!, column: ordering}, tableAlias: options.tableAlias}), direction: "asc", nulls: "last" }
              ] 
            },
            1
          ]
        };

      case '= any':
        // Null if any not present
        if (_.any(compiledExprs, ce => ce == null)) {
          return null;
        }

        // False if empty list on rhs
        if (expr.exprs[1]!.type === "literal") {
          const rhsLiteral = expr.exprs[1] as LiteralExpr
          if (rhsLiteral.value == null || (_.isArray(rhsLiteral.value) && (rhsLiteral.value.length === 0))) {
            return false
          }
        }

        return { type: "op", op: "=", modifier: "any", exprs: compiledExprs };

      case "between":
        // Null if first not present
        if (compiledExprs[0] == null) {
          return null;
        }

        // Null if second and third not present
        if (compiledExprs[1] == null && compiledExprs[2] == null) {
          return null;
        }

        // >= if third missing
        if (compiledExprs[2] == null) {
          return {
            type: "op",
            op: ">=",
            exprs: [compiledExprs[0], compiledExprs[1]]
          };
        }

        // <= if second missing
        if (compiledExprs[1] == null) {
          return {
            type: "op",
            op: "<=",
            exprs: [compiledExprs[0], compiledExprs[2]]
          };
        }

        // Between
        return {
          type: "op",
          op: "between",
          exprs: compiledExprs
        };

      case "not":
        if (compiledExprs[0] == null) {
          return null;
        }

        return {
          type: "op",
          op: expr.op,
          exprs: [
            { type: "op", op: "coalesce", exprs: [compiledExprs[0], false] }
          ]
        };

      case "is null": case "is not null":
        if (compiledExprs[0] == null) {
          return null;
        }

        return {
          type: "op",
          op: expr.op,
          exprs: compiledExprs
        };
        
      case "contains":
        // Null if either not present
        if (compiledExprs[0] == null || compiledExprs[1] == null) {
          return null;
        }

        // Null if no expressions in literal list
        if (((compiledExprs[1] as any).type === "literal") && ((compiledExprs[1] as any).value.length === 0)) {
          return null;
        }

        // Cast both to jsonb and use @>. Also convert both to json first to handle literal arrays
        return {
          type: "op",
          op: "@>",
          exprs: [
            convertToJsonB(compiledExprs[0]),
            convertToJsonB(compiledExprs[1])
          ]
        }

      case "intersects":
        // Null if either not present
        if (compiledExprs[0] == null || compiledExprs[1] == null) {
          return null;
        }

        // Use (select bool_or(x.value) from (select LEFT::jsonb @> jsonb_array_elements(RIGHT::jsonb) as value) as x)
        return {
          type: "scalar",
          expr: { type: "op", op: "bool_or", exprs: [{ type: "field", tableAlias: "elements", column: "value" }] },
          from: { 
            type: "subquery",
            alias: "elements",
            query: {
              type: "query",
              selects: [
                { 
                  type: "select", 
                  expr: { type: "op", op: "@>", exprs: [
                    convertToJsonB(compiledExprs[0]),
                    { type: "op", op: "jsonb_array_elements", exprs: [convertToJsonB(compiledExprs[1])] }
                  ]}, 
                  alias: "value" 
                }
              ]
            }
          }
        }

      case "includes":
        // Null if either not present
        if (compiledExprs[0] == null || compiledExprs[1] == null) {
          return null;
        }

        // Cast both to jsonb and use @>. Also convert both to json first to handle literal arrays
        return {
          type: "op",
          op: "@>",
          exprs: [
            convertToJsonB(compiledExprs[0]),
            convertToJsonB(compiledExprs[1])
          ]
        }
  
      case "length":
        // 0 if null
        if ((compiledExprs[0] == null)) {
          return 0;
        }

        // Cast both to jsonb and use jsonb_array_length. Also convert both to json first to handle literal arrays. Coalesce to 0 so that null is 0
        return {
          type: "op",
          op: "coalesce",
          exprs: [
            {
              type: "op",
              op: "jsonb_array_length",
              exprs: [
                convertToJsonB(compiledExprs[0])
              ]
            },
            0
          ]
        };

      case "line length":
        // null if null
        if ((compiledExprs[0] == null)) {
          return null;
        }

        // ST_Length_Spheroid(ST_Transform(location,4326::integer), 'SPHEROID["GRS_1980",6378137,298.257222101]'::spheroid)
        return {
          type: "op",
          op: "ST_LengthSpheroid",
          exprs: [
            {
              type: "op",
              op: "ST_Transform",
              exprs: [compiledExprs[0], { type: "op", op: "::integer", exprs: [4326] }]
            },
            { type: "op", op: "::spheroid", exprs: ['SPHEROID["GRS_1980",6378137,298.257222101]' ]}
          ]
        };

      case "to text":
        // Null if not present
        if (compiledExprs[0] == null) {
          return null;
        }

        if (exprUtils.getExprType(expr.exprs[0]) === "enum") {
          // Null if no enum values
          const enumValues = exprUtils.getExprEnumValues(expr.exprs[0]);
          if (!enumValues) {
            return null;
          }

          return {
            type: "case",
            input: compiledExprs[0],
            cases: _.map(enumValues, ev => {
              return {
                when: { type: "literal", value: ev.id },
                then: { type: "literal", value: exprUtils.localizeString(ev.name) }
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

        if (exprUtils.getExprType(expr.exprs[0]) === "text[]") {
          return {
            type: "op",
            op: "array_to_string",
            exprs: [
              { 
                type: "scalar",
                expr: {
                  type: "op",
                  op: "array_agg",
                  exprs: [{ type: "field", tableAlias: "values" }]
                },
                from: {
                  type: "subexpr",
                  expr: {
                    type: "op",
                    op: "jsonb_array_elements_text",
                    exprs: [{ type: "op", op: "to_jsonb", exprs: [
                      compiledExprs[0]
                    ]}]
                  },
                  alias: "values"
                }
              },
              // Requires explicit text type
              { type: "op", op: "::text", exprs: [', '] }
            ]
          }
        }

        return null

      case "to date":
        // Null if not present
        if (compiledExprs[0] == null) {
          return null;
        }

        return {
          type: "op",
          op: "substr",
          exprs: [
            compiledExprs[0],
            1,
            10
          ]
        };      

      case "to number":
        // Null if not present
        if (compiledExprs[0] == null) { 
          return null;
        }

        // case when EXPR ~ '^([0-9]+[.]?[0-9]*|[.][0-9]+)$' then (EXPR::text)::numeric else null end
        return {
          type: "case",
          cases: [
            {
              when: {
                type: "op",
                op: "~",
                exprs: [compiledExprs[0], "^([0-9]+[.]?[0-9]*|[.][0-9]+)$" ]
              },
              then: {
                type: "op",
                op: "::numeric",
                exprs: [
                  { type: "op", op: "::text", exprs: [compiledExprs[0]] }
                ]
              }
            }
          ],
          else: { type: "literal", value: null }
        }

      case "count where":
        // Null if not present
        if (compiledExprs[0] == null) { 
          return null;
        }

        return {
          type: "op",
          op: "coalesce",
          exprs: [
            {
              type: "op",
              op: "sum",
              exprs: [
                { 
                  type: "case",
                  cases: [{
                    when: compiledExprs[0],
                    then: 1
                  }
                  ],
                  else: 0
                }
              ]
            },
            0
          ]
        };

      case "percent where":
        // Null if not present
        if (compiledExprs[0] == null) { 
          return null;
        }

        // Compiles as sum(case when cond [and basis (if present)] then 100::decimal else 0 end)/sum(1 [or case when basis then 1 else 0 (if present)]) (prevent div by zero)        
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
                  cases: [{
                    when: compiledExprs[1] ? { type: "op", op: "and", exprs: [compiledExprs[0], compiledExprs[1]] } : compiledExprs[0],
                    then: { type: "op", op: "::decimal", exprs: [100] }
                  }
                  ],
                  else: 0
                }
              ]
            },
            compiledExprs[1] ?
              {
                type: "op",
                op: "nullif",
                exprs: [
                  { 
                    type: "op",
                    op: "sum",
                    exprs: [
                      { 
                        type: "case",
                        cases: [{
                          when: compiledExprs[1],
                          then: 1
                        }
                        ],
                        else: 0
                      }
                    ]
                  },
                  0
                ]
              }
            :
              { type: "op", op: "sum", exprs: [1] }
          ]
        };

      case "sum where":
        // Null if not present
        if (compiledExprs[0] == null) { 
          return null;
        }

        // Simple sum if not specified where
        if (compiledExprs[1] == null) {
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
              cases: [{
                when: compiledExprs[1],
                then: compiledExprs[0]
              }
              ],
              else: 0
            }
          ]
        };

      case "min where":
        // Null if not present
        if (compiledExprs[0] == null) { 
          return null;
        }

        // Simple min if not specified where
        if (compiledExprs[1] == null) {
          return {
            type: "op",
            op: "min",
            exprs: [compiledExprs[0]]
          };
        }

        return {
          type: "op",
          op: "min",
          exprs: [
            { 
              type: "case",
              cases: [{
                when: compiledExprs[1],
                then: compiledExprs[0]
              }
              ],
              else: null
            }
          ]
        };

      case "max where":
        // Null if not present
        if (compiledExprs[0] == null) { 
          return null;
        }

        // Simple max if not specified where
        if (compiledExprs[1] == null) {
          return {
            type: "op",
            op: "max",
            exprs: [compiledExprs[0]]
          };
        }

        return {
          type: "op",
          op: "max",
          exprs: [
            { 
              type: "case",
              cases: [{
                when: compiledExprs[1],
                then: compiledExprs[0]
              }
              ],
              else: null
            }
          ]
        };

      case "count distinct":
        // Null if not present
        if (compiledExprs[0] == null) { 
          return null;
        }

        return {
          type: "op",
          op: "count",
          exprs: [compiledExprs[0]],
          modifier: "distinct"
        };

      case "percent":
        // Compiles as count(*) * 100::decimal / sum(count(*)) over()
        return {
          type: "op",
          op: "/",
          exprs: [
            {
              type: "op",
              op: "*",
              exprs: [
                { type: "op", op: "count", exprs: [] },
                { type: "op", op: "::decimal", exprs: [100] }
              ]
            },
            { 
              type: "op",
              op: "sum",
              exprs: [
                { type: "op", op: "count", exprs: [] }
              ],
              over: {}
            }
          ]
        };

      // Hierarchical test that uses ancestry column
      case "within":
        // Null if either not present
        if (compiledExprs[0] == null || compiledExprs[1] == null) {
          return null;
        }

        // Get table being used
        var idTable = exprUtils.getExprIdTable(expr.exprs[0])!

        // Prefer ancestryTable
        if (this.schema.getTable(idTable)!.ancestryTable) {
          // exists (select null from <ancestryTable> as subwithin where ancestor = compiledExprs[1] and descendant = compiledExprs[0])
          return {
            type: "op",
            op: "exists",
            exprs: [
              {
                type: "scalar",
                expr: null,
                from: { type: "table", table: this.schema.getTable(idTable)!.ancestryTable!, alias: "subwithin" },
                where: {
                  type: "op",
                  op: "and",
                  exprs: [
                    { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "subwithin", column: "ancestor" }, compiledExprs[1]]},
                    { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "subwithin", column: "descendant" }, compiledExprs[0]]}
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
            compiledExprs[0],
            {
              type: "scalar",
              expr: this.compileColumnRef(this.schema.getTable(idTable)!.primaryKey, "subwithin"),
              from: { type: "table", table: idTable, alias: "subwithin" },
              where: {
                type: "op",
                op: "@>",
                exprs: [
                  { type: "field", tableAlias: "subwithin", column: this.schema.getTable(idTable)!.ancestry! },
                  { type: "op", op: "::jsonb", exprs: [{ type: "op", op: "json_build_array", exprs: [compiledExprs[1]] }] }
                ]
              }
            }            
          ]
        };

      // Hierarchical test that uses ancestry column
      case "within any":
        // Null if either not present
        if (compiledExprs[0] == null || compiledExprs[1] == null) {
          return null;
        }

        // Get table being used
        idTable = exprUtils.getExprIdTable(expr.exprs[0])!

        // Prefer ancestryTable
        if (this.schema.getTable(idTable)!.ancestryTable) {
          // exists (select null from <ancestryTable> as subwithin where ancestor = any(compiledExprs[1]) and descendant = compiledExprs[0])
          return {
            type: "op",
            op: "exists",
            exprs: [
              {
                type: "scalar",
                expr: null,
                from: { type: "table", table: this.schema.getTable(idTable)!.ancestryTable!, alias: "subwithin" },
                where: {
                  type: "op",
                  op: "and",
                  exprs: [
                    { type: "op", op: "=", modifier: "any", exprs: [{ type: "field", tableAlias: "subwithin", column: "ancestor" }, compiledExprs[1]]},
                    { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "subwithin", column: "descendant" }, compiledExprs[0]]}
                  ]
                }
              }
            ]
          };
        }

        // This older code fails now that admin_regions uses integer pk. Replaced with literal-only code
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
        if ((compiledExprs[1] as any).type !== "literal") {
          throw new Error("Non-literal RHS of within any not supported");
        }

        return {
          type: "op",
          op: "in",
          exprs: [
            compiledExprs[0],
            {
              type: "scalar",
              expr: this.compileColumnRef(this.schema.getTable(idTable)!.primaryKey, "subwithin"),
              from: { type: "table", table: idTable, alias: "subwithin" },
              where: {
                type: "op",
                op: "?|",
                exprs: [
                  { type: "field", tableAlias: "subwithin", column: this.schema.getTable(idTable)!.ancestryText || this.schema.getTable(idTable)!.ancestry! },
                  { type: "literal", value: _.map((compiledExprs[1] as JsonQLLiteral).value, value => { 
                    if (_.isNumber(value)) {
                      return "" + value;
                    } else { 
                      return value;
                    }
                    })
                  }
                ]
              }
            }            
          ]
        };

      case "latitude":
        if (compiledExprs[0] == null) {
          return null;
        }

        return {
          type: "op",
          op: "ST_Y",
          exprs: [
            { type: "op", op: "ST_Centroid", exprs: [
              { type: "op", op: "ST_Transform", exprs: [compiledExprs[0], { type: "op", op: "::integer", exprs: [4326] }] }
            ] }
          ]
        };

      case "longitude":
        if (compiledExprs[0] == null) {
          return null;
        }

        return {
          type: "op",
          op: "ST_X",
          exprs: [
            { type: "op", op: "ST_Centroid", exprs: [
              { type: "op", op: "ST_Transform", exprs: [compiledExprs[0], { type: "op", op: "::integer", exprs: [4326] }] }
            ] }
          ]
        };

      case 'days difference':
        if (compiledExprs[0] == null || compiledExprs[1] == null) {
          return null;
        }

        if ((exprUtils.getExprType(expr.exprs[0]) === "datetime") || (exprUtils.getExprType(expr.exprs[1]) === "datetime")) {
          return {
            type: "op",
            op: "/",
            exprs: [
              {
                type: "op",
                op: "-",
                exprs: [
                  { type: "op", op: "date_part", exprs: ['epoch', { type: "op", op: "::timestamp", exprs: [compiledExprs[0]] }] },
                  { type: "op", op: "date_part", exprs: ['epoch', { type: "op", op: "::timestamp", exprs: [compiledExprs[1]] }] }
                ]
              },
              86400
            ]
          };
        }

        if (exprUtils.getExprType(expr.exprs[0]) === "date") {
          return {
            type: "op",
            op: "-",
            exprs: [
              { type: "op", op: "::date", exprs: [compiledExprs[0]] },
              { type: "op", op: "::date", exprs: [compiledExprs[1]] }
            ]
          };
        }

        return null;

      case 'months difference':
        if (compiledExprs[0] == null || compiledExprs[1] == null) {
          return null;
        }

        if ((exprUtils.getExprType(expr.exprs[0]) === "datetime") || (exprUtils.getExprType(expr.exprs[1]) === "datetime")) {
          return {
            type: "op",
            op: "/",
            exprs: [
              {
                type: "op",
                op: "-",
                exprs: [
                  { type: "op", op: "date_part", exprs: ['epoch', { type: "op", op: "::timestamp", exprs: [compiledExprs[0]] }] },
                  { type: "op", op: "date_part", exprs: ['epoch', { type: "op", op: "::timestamp", exprs: [compiledExprs[1]] }] }
                ]
              },
              86400 * 30.5
            ]
          };
        }

        if (exprUtils.getExprType(expr.exprs[0]) === "date") {
          return {
            type: "op",
            op: "/",
            exprs: [
              {
                type: "op",
                op: "-",
                exprs: [
                  { type: "op", op: "::date", exprs: [compiledExprs[0]] },
                  { type: "op", op: "::date", exprs: [compiledExprs[1]] }
                ]
              },
              30.5
            ]
          };
        }

        return null;

      case 'years difference':
        if (compiledExprs[0] == null || compiledExprs[1] == null) {
          return null;
        }

        if ((exprUtils.getExprType(expr.exprs[0]) === "datetime") || (exprUtils.getExprType(expr.exprs[1]) === "datetime")) {
          return {
            type: "op",
            op: "/",
            exprs: [
              {
                type: "op",
                op: "-",
                exprs: [
                  { type: "op", op: "date_part", exprs: ['epoch', { type: "op", op: "::timestamp", exprs: [compiledExprs[0]] }] },
                  { type: "op", op: "date_part", exprs: ['epoch', { type: "op", op: "::timestamp", exprs: [compiledExprs[1]] }] }
                ]
              },
              86400 * 365
            ]
          };
        }

        if (exprUtils.getExprType(expr.exprs[0]) === "date") {
          return {
            type: "op",
            op: "/",
            exprs: [
              {
                type: "op",
                op: "-",
                exprs: [
                  { type: "op", op: "::date", exprs: [compiledExprs[0]] },
                  { type: "op", op: "::date", exprs: [compiledExprs[1]] }
                ]
              },
              365
            ]
          };
        }

        return null;

      case 'days since':
        if (compiledExprs[0] == null) {
          return null;
        }

        switch (expr0Type) {
          case "date":
            return { 
              type: "op",
              op: "-",
              exprs: [
                { type: "op", op: "::date", exprs: [moment().format("YYYY-MM-DD")] },
                { type: "op", op: "::date", exprs: [compiledExprs[0]] }
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
                    { type: "op", op: "date_part", exprs: ['epoch', { type: "op", op: "::timestamp", exprs: [nowExpr] }] },
                    { type: "op", op: "date_part", exprs: ['epoch', { type: "op", op: "::timestamp", exprs: [compiledExprs[0]] }] }
                  ]
                },
                86400
              ]
            };
          default:
            return null;
        }

      case 'month':
        if (compiledExprs[0] == null) {
          return null;
        }

        return {
          type: "op",
          op: "substr",
          exprs: [
            compiledExprs[0],
            6,
            2
          ]
        };

      case 'yearmonth':
        if (compiledExprs[0] == null) {
          return null;
        }

        return {
          type: "op",
          op: "rpad",
          exprs: [
            { type: "op", op: "substr", exprs: [compiledExprs[0], 1, 7] },
            10,
            "-01"
          ]
        };

      case 'yearquarter':
        if (compiledExprs[0] == null) {
          return null;
        }

        return {
          type: "op",
          op: "to_char",
          exprs: [
            { type: "op", op: "::date", exprs: [compiledExprs[0]] },
            "YYYY-Q"
          ]
        };

      case 'yearweek':
        if (compiledExprs[0] == null) {
          return null;
        }

        return {
          type: "op",
          op: "to_char",
          exprs: [
            { type: "op", op: "::date", exprs: [compiledExprs[0]] },
            "IYYY-IW"
          ]
        };

      case 'weekofyear':
        if (compiledExprs[0] == null) {
          return null;
        }

        return {
          type: "op",
          op: "to_char",
          exprs: [
            { type: "op", op: "::date", exprs: [compiledExprs[0]] },
            "IW"
          ]
        };

      case 'year':
        if (compiledExprs[0] == null) {
          return null;
        }

        return {
          type: "op",
          op: "rpad",
          exprs: [
            { type: "op", op: "substr", exprs: [compiledExprs[0], 1, 4] },
            10,
            "-01-01"
          ]
        };

      case 'weekofmonth':
        if (compiledExprs[0] == null) {
          return null;
        }

        return {
          type: "op",
          op: "to_char",
          exprs: [
            { type: "op", op: "::timestamp", exprs: [compiledExprs[0]] },
            "W"
          ]
        };

      case 'dayofmonth':
        if (compiledExprs[0] == null) {
          return null;
        }

        return {
          type: "op",
          op: "to_char",
          exprs: [
            { type: "op", op: "::timestamp", exprs: [compiledExprs[0]] },
            "DD"
          ]
        };

      case 'thisyear':
        if (compiledExprs[0] == null) {
          return null;
        }

        switch (expr0Type) {
          case "date":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("year").format("YYYY-MM-DD") ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("year").add(1, 'years').format("YYYY-MM-DD") ] }
              ]
            };
          case "datetime":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("year").toISOString() ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("year").add(1, 'years').toISOString() ] }
              ]
            };
          default:
            return null;
        }

      case 'lastyear':
        if (compiledExprs[0] == null) {
          return null;
        }

        switch (expr0Type) {
          case "date":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("year").subtract(1, 'years').format("YYYY-MM-DD") ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("year").format("YYYY-MM-DD") ] }
              ]
            };
          case "datetime":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("year").subtract(1, 'years').toISOString() ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("year").toISOString() ] }
              ]
            };
          default:
            return null;
        }

      case 'thismonth':
        if (compiledExprs[0] == null) {
          return null;
        }

        switch (expr0Type) {
          case "date":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("month").format("YYYY-MM-DD") ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("month").add(1, 'months').format("YYYY-MM-DD") ] }
              ]
            };
          case "datetime":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("month").toISOString() ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("month").add(1, 'months').toISOString() ] }
              ]
            };
          default:
            return null;
        }

      case 'lastmonth':
        if (compiledExprs[0] == null) {
          return null;
        }

        switch (expr0Type) {
          case "date":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("month").subtract(1, 'months').format("YYYY-MM-DD") ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("month").format("YYYY-MM-DD") ] }
              ]
            };
          case "datetime":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("month").subtract(1, 'months').toISOString() ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("month").toISOString() ] }
              ]
            };
          default:
            return null;
        }

      case 'today':
        if (compiledExprs[0] == null) {
          return null;
        }

        switch (expr0Type) {
          case "date":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().format("YYYY-MM-DD") ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD") ] }
              ]
            };
          case "datetime":
           return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("day").toISOString() ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString() ] }
              ]
            };
          default:
            return null;
        }

      case 'yesterday':
        if (compiledExprs[0] == null) {
          return null;
        }

        switch (expr0Type) {
          case "date":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().subtract(1, 'days').format("YYYY-MM-DD") ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().format("YYYY-MM-DD") ] }
              ]
            };
          case "datetime":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("day").subtract(1, 'days').toISOString() ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("day").toISOString() ] }
              ]
            };
          default:
            return null;
        }

      case 'last24hours':
        if (compiledExprs[0] == null) {
          return null;
        }

        switch (expr0Type) {
          case "date":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().subtract(1, 'days').format("YYYY-MM-DD") ] },
                { type: "op", op: "<=", exprs: [compiledExprs[0], moment().format("YYYY-MM-DD") ] }
              ]
            };
          case "datetime":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], nowMinus24HoursExpr] },
                { type: "op", op: "<=", exprs: [compiledExprs[0], nowExpr] }
              ]
            };
          default:
            return null;
        }

      case 'last7days':
        if (compiledExprs[0] == null) {
          return null;
        }

        switch (expr0Type) {
          case "date":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().subtract(7, 'days').format("YYYY-MM-DD") ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD") ] }
              ]
            };
          case "datetime":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("day").subtract(7, 'days').toISOString() ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString() ] }
              ]
            };
          default:
            return null;
        }

      case 'last30days':
        if (compiledExprs[0] == null) {
          return null;
        }

        switch (expr0Type) {
          case "date":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().subtract(30, 'days').format("YYYY-MM-DD") ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD") ] }
              ]
            };
          case "datetime":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("day").subtract(30, 'days').toISOString() ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString() ] }
              ]
            };
          default:
            return null;
        }

      case 'last365days':
        if (compiledExprs[0] == null) {
          return null;
        }

        switch (expr0Type) {
          case "date":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().subtract(365, 'days').format("YYYY-MM-DD") ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD") ] }
              ]
            };
          case "datetime":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().startOf("day").subtract(365, 'days').toISOString() ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString() ] }
              ]
            };
          default:
            return null;
        }

      case 'last12months':
        if (compiledExprs[0] == null) {
          return null;
        }

        switch (expr0Type) {
          case "date":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().subtract(11, "months").startOf('month').format("YYYY-MM-DD") ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD") ] }
              ]
            };
          case "datetime":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().subtract(11, "months").startOf('month').toISOString() ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString() ] }
              ]
            };
          default:
            return null;
        }
      
      case 'last6months':
        if (compiledExprs[0] == null) {
          return null;
        }

        switch (expr0Type) {
          case "date":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().subtract(5, "months").startOf('month').format("YYYY-MM-DD") ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD") ] }
              ]
            };
          case "datetime":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().subtract(5, "months").startOf('month').toISOString() ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString() ] }
              ]
            };
          default:
            return null;
        }

      case 'last3months':
        if (compiledExprs[0] == null) {
          return null;
        }

        switch (expr0Type) {
          case "date":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().subtract(2, "months").startOf('month').format("YYYY-MM-DD") ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().add(1, 'days').format("YYYY-MM-DD") ] }
              ]
            };
          case "datetime":
            return { 
              type: "op",
              op: "and",
              exprs: [
                { type: "op", op: ">=", exprs: [compiledExprs[0], moment().subtract(2, "months").startOf('month').toISOString() ] },
                { type: "op", op: "<", exprs: [compiledExprs[0], moment().startOf("day").add(1, 'days').toISOString() ] }
              ]
            };
          default:
            return null;
        }

      case 'future':
        if (compiledExprs[0] == null) {
          return null;
        }

        switch (expr0Type) {
          case "date":
            return { 
              type: "op", 
              op: ">", 
              exprs: [compiledExprs[0], moment().format("YYYY-MM-DD") ] 
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

      case 'notfuture':
        if (compiledExprs[0] == null) {
          return null;
        }

        switch (expr0Type) {
          case "date":
            return { 
              type: "op", 
              op: "<=", 
              exprs: [compiledExprs[0], moment().format("YYYY-MM-DD") ] 
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

      case 'current date':
        return { type: "literal", value: moment().format("YYYY-MM-DD") };

      case 'current datetime':
        return { type: "literal", value: moment().toISOString() };

      case 'distance':
        if (compiledExprs[0] == null || compiledExprs[1] == null) {
          return null;
        }

        return {
          type: "op",
          op: "ST_DistanceSphere",
          exprs: [
            { type: "op", op: "ST_Transform", exprs: [compiledExprs[0], { type: "op", op: "::integer", exprs: [4326] }] },
            { type: "op", op: "ST_Transform", exprs: [compiledExprs[1], { type: "op", op: "::integer", exprs: [4326] }] }
          ]
        };

      case 'is latest':
        var lhsCompiled = this.compileExpr({expr: expr.exprs[0], tableAlias: "innerrn"});
        if (!lhsCompiled) {
          return null;
        }

        var filterCompiled = this.compileExpr({expr: expr.exprs[1], tableAlias: "innerrn"});

        // Get ordering
        ordering = this.schema.getTable(expr.table!)!.ordering

        if (!ordering) {
          throw new Error("No ordering defined");
        }

        // order descending
        var orderBy: { expr: JsonQLExpr, direction: "desc" }[] = [{ expr: this.compileFieldExpr({expr: { type: "field", table: expr.table!, column: ordering}, tableAlias: "innerrn"}), direction: "desc" }];

        // _id in (select outerrn.id from (select innerrn.id, row_number() over (partition by EXPR1 order by ORDERING desc) as rn from the_table as innerrn where filter) as outerrn where outerrn.rn = 1)

        // Create innerrn query
        var innerrnQuery: JsonQLQuery = {
          type: "query",
          selects: [
            { type: "select", expr: this.compileExpr({expr: { type: "id", table: expr.table! }, tableAlias: "innerrn" }), alias: "id" },
            { 
              type: "select",
              expr: {
                type: "op",
                op: "row_number",
                exprs: [],
                over: {
                  partitionBy: [lhsCompiled],
                  orderBy
                }
              },
              alias: "rn" 
            }
          ],
          from: { type: "table", table: expr.table!, alias: "innerrn" }
        };
        if (filterCompiled) {
          innerrnQuery.where = filterCompiled;
        }

        // Wrap in outer query
        var outerrnQuery: JsonQLScalar = {
          type: "scalar",
          expr: { type: "field", tableAlias: "outerrn", column: "id" },
          from: {
            type: "subquery",
            query: innerrnQuery,
            alias: "outerrn"
          },
          where: { type: "op", op: "=", exprs: [{ type: "field", tableAlias: "outerrn", column: "rn" }, 1]}
        };

        return {
          type: "op",
          op: "in",
          exprs: [
            this.compileExpr({expr: { type: "id", table: expr.table! }, tableAlias: options.tableAlias}),
            outerrnQuery
          ]
        };

      default:
        throw new Error(`Unknown op ${expr.op}`);
    }
  }

  compileCaseExpr(options: { expr: CaseExpr, tableAlias: string }): JsonQLExpr {
    const {
      expr
    } = options;

    const compiled: JsonQLCase = {
      type: "case",
      cases: _.map(expr.cases, c => {
        return {
          when: this.compileExpr({expr: c.when, tableAlias: options.tableAlias}),
          then: this.compileExpr({expr: c.then, tableAlias: options.tableAlias})
        };
    }),
      else: this.compileExpr({expr: expr.else, tableAlias: options.tableAlias})
    };

    // Remove null cases
    compiled.cases = _.filter(compiled.cases, c => c.when != null);

    // Return null if no cases
    if (compiled.cases.length === 0) {
      return null;
    }

    return compiled;
  }

  compileScoreExpr(options: { expr: ScoreExpr, tableAlias: string }): JsonQLExpr {
    const {
      expr
    } = options;
    const exprUtils = new ExprUtils(this.schema);

    // If empty, literal 0
    if (_.isEmpty(expr.scores)) {
      return { type: "literal", value: 0 };
    }

    // Get type of input
    const inputType = exprUtils.getExprType(expr.input);

    switch (inputType) {
      case "enum":
        return {
          type: "case",
          input: this.compileExpr({expr: expr.input, tableAlias: options.tableAlias}),
          cases: _.map(_.pairs(expr.scores), pair => {
            return { 
              when: { type: "literal", value: pair[0] },
              then: this.compileExpr({expr: pair[1], tableAlias: options.tableAlias}) 
            };
          }),
          else: { type: "literal", value: 0 }
        };
      case "enumset":
        return {
          type: "op",
          op: "+",
          exprs: _.map(_.pairs(expr.scores), pair => {
            return {
              type: "case",
              cases: [
                { 
                  when: {
                    type: "op",
                    op: "@>",
                    exprs: [
                      convertToJsonB(this.compileExpr({expr: expr.input, tableAlias: options.tableAlias})),
                      convertToJsonB({ type: "literal", value: [pair[0]] })
                    ]
                  },
                  then: this.compileExpr({expr: pair[1], tableAlias: options.tableAlias}) 
                }
              ],
              else: { type: "literal", value: 0 }
            };
          })
        };

      // Null if no expression
      default:
        return null;
    }
  }

  compileBuildEnumsetExpr(options: { expr: BuildEnumsetExpr, tableAlias: string }): JsonQLExpr {
    // Create enumset
    // select to_jsonb(array_agg(bes.v)) from (select (case when true then 'x' end) as v union all select (case when true then 'y' end) as v ...) as bes where v is not null

    const {
      expr
    } = options;

    // Handle empty case
    if (_.keys(expr.values).length === 0) {
      return null;
    }

    return {
      type: "scalar",
      expr: {
        type: "op",
        op: "to_jsonb",
        exprs: [
          { 
            type: "op",
            op: "array_agg",
            exprs: [{ type: "field", tableAlias: "bes", column: "v" }]
          }
        ]
      },
      from: {
        type: "subquery",
        alias: "bes",
        query: {
          type: "union all",
          queries: _.map(_.pairs(expr.values), pair => {
            return {
              type: "query",
              selects: [
                { 
                  type: "select",
                  expr: {
                    type: "case",
                    cases: [{ when: this.compileExpr({expr: pair[1], tableAlias: options.tableAlias}), then: pair[0] }]
                  },
                  alias: "v"
                }
              ]
            } as JsonQLSelectQuery
        })
        }
      },

      where: {
        type: "op",
        op: "is not null",
        exprs: [{ type: "field", tableAlias: "bes", column: "v" }]
      }
    };
  }

  compileComparisonExpr(options: { expr: LegacyComparisonExpr, tableAlias: string }): JsonQLExpr {
    let exprs;
    const {
      expr
    } = options;
    const exprUtils = new ExprUtils(this.schema);

    // Missing left-hand side type means null condition
    const exprLhsType = exprUtils.getExprType(expr.lhs);
    if (!exprLhsType) {
      return null;
    }

    // Missing right-hand side means null condition
    if (exprUtils.getComparisonRhsType(exprLhsType, expr.op) && (expr.rhs == null)) {
      return null;
    }

    const lhsExpr = this.compileExpr({expr: expr.lhs, tableAlias: options.tableAlias}); 
    if (expr.rhs) {
      const rhsExpr = this.compileExpr({expr: expr.rhs, tableAlias: options.tableAlias});
      exprs = [lhsExpr, rhsExpr];
    } else {
      exprs = [lhsExpr];
    }

    // Handle special cases 
    switch (expr.op) {
      case '= true':
        return { type: "op", op: "=", exprs: [lhsExpr, { type: "literal", value: true }]};
      case '= false':
        return { type: "op", op: "=", exprs: [lhsExpr, { type: "literal", value: false }]};
      case '= any':
        return { type: "op", op: "=", modifier: "any", exprs };
      case 'between':
        return { type: "op", op: "between", exprs: [lhsExpr, { type: "literal", value: (expr.rhs as any).value[0] }, { type: "literal", value: (expr.rhs as any).value[1] }] };
      default:
        return { 
          type: "op",
          op: expr.op,
          exprs
        };
    }
  }

  compileLogicalExpr(options: { expr: LegacyLogicalExpr, tableAlias: string }): JsonQLExpr {
    const {
      expr
    } = options;

    let compiledExprs = _.map(expr.exprs, e => this.compileExpr({expr: e, tableAlias: options.tableAlias}));

    // Remove nulls
    compiledExprs = _.compact(compiledExprs);

    // Simplify
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
  }

  // Compiles a reference to a column or a JsonQL expression
  // If parameter is a string, create a simple field expression
  // If parameter is an object, inject tableAlias for `{alias}`
  compileColumnRef(column: any, tableAlias: string): JsonQLExpr {
    if (_.isString(column)) {
      return { type: "field", tableAlias, column };
    }

    return injectTableAlias(column, tableAlias) as JsonQLExpr
  }

  // Compiles a table, substituting with custom jsonql if required
  compileTable(tableId: string, alias: string): JsonQLFrom {
    const table = this.schema.getTable(tableId);
    if (!table) {
      throw new Error(`Table ${tableId} not found`);
    }
      
    if (!table.jsonql) {
      return { type: "table", table: tableId, alias };
    } else {
      return { type: "subquery", query: table.jsonql, alias };
    }
  }

  compileVariableExpr(options: { expr: VariableExpr, tableAlias: string }): JsonQLExpr {
    // Get variable
    const variable = _.findWhere(this.variables, {id: options.expr.variableId});
    if (!variable) {
      throw new Error(`Variable ${options.expr.variableId} not found`);
    }

    // Get value (which is always an expression)
    const value = this.variableValues[variable.id];

    // If expression, compile
    if (value != null) {
      return this.compileExpr({ expr: value, tableAlias: options.tableAlias });
    } else {
      return null;
    }
  }
}

// Converts a compiled expression to jsonb. Literals cannot use to_jsonb as they will
// trigger "could not determine polymorphic type because input has type unknown" unless the 
// SQL is inlined
function convertToJsonB(compiledExpr: JsonQLExpr): JsonQLExpr {
  if (compiledExpr == null) {
    return compiledExpr;
  }

  if (typeof compiledExpr == "number" || typeof compiledExpr == "boolean" || typeof compiledExpr == "string") {
    return { type: "op", op: "::jsonb", exprs: [{ type: "literal", value: JSON.stringify(compiledExpr) }] };
  }

  // Literals are special and are cast to jsonb from a JSON string
  if ((compiledExpr as any).type === "literal") {
    return { type: "op", op: "::jsonb", exprs: [{ type: "literal", value: JSON.stringify((compiledExpr as JsonQLLiteral).value) }] };
  }
  
  // First convert using to_jsonb in case is array
  return { type: "op", op: "to_jsonb", exprs: [compiledExpr] }
}
