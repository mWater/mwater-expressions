import _ from "lodash";
import ExprUtils from "./ExprUtils";
import { getExprExtension } from "./extensions";
import Schema from "./Schema";
import { AggrStatus, Expr, LiteralType, Variable } from "./types";
import { WeakCache } from "./WeakCache";

// Weak cache is global to allow validator to be created and destroyed
const weakCache = new WeakCache();

export interface ValidateOptions  { 
  table?: string
  types?: LiteralType[]
  enumValueIds?: string[]
  idTable?: string
  aggrStatuses?: AggrStatus[] 
}

/** Validates expressions. If an expression has been cleaned, it will always be valid */
export default class ExprValidator {
  schema: Schema
  variables: Variable[]
  exprUtils: ExprUtils
  
  constructor(schema: Schema, variables?: Variable[]) {
    this.schema = schema;
    this.variables = variables || [];
    this.exprUtils = new ExprUtils(schema, variables);
  }

  /** Validates an expression, returning null if it is valid, otherwise return an error string
   * NOTE: This uses global weak caching and assumes that expressions are never mutated after
   * having been validated!
   * options are:
   *   table: optional current table. expression must be related to this table or will be stripped
   *   types: optional types to limit to
   *   enumValueIds: ids of enum values that are valid if type is enum
   *   idTable: table that type of id must be from
   *   aggrStatuses: statuses of aggregation to allow. list of "individual", "literal", "aggregate". Default: ["individual", "literal"]
   */
  validateExpr(expr: Expr, options?: ValidateOptions): string | null {
    options = options || {}

    if (!expr) {
      return null;
    }

    if (!this.schema) { 
      return weakCache.cacheFunction([expr], [this.variables, options], () => { 
        return this.validateExprInternal(expr, options!);
      });
    }

    return weakCache.cacheFunction([this.schema, expr], [this.variables, options], () => { 
      return this.validateExprInternal(expr, options!);
    });
  }

  validateExprInternal = (expr: Expr, options: { 
    table?: string
    types?: LiteralType[]
    enumValueIds?: string[]
    idTable?: string
    aggrStatuses?: AggrStatus[] 
    depth?: number
  }): string | null => {
    let error, key, value
    let enumValueIds
    const aggrStatuses = options.aggrStatuses || {aggrStatuses: ["individual", "literal"]};

    if (!expr) {
      return null;
    }

    // Allow {} placeholder
    if (_.isEmpty(expr)) {
      return null;
    }

    // Prevent infinite recursion
    if ((options.depth || 0) > 100) {
      return "Circular reference";
    }

    // Check table if not literal
    if (options.table && this.exprUtils.getExprTable(expr) && this.exprUtils.getExprTable(expr) !== options.table) { 
      return `Wrong table ${this.exprUtils.getExprTable(expr)} (expected ${options.table})`;
    }

    // Literal is ok if right type
    switch (expr.type) {
      case "literal":
        if (options.types && !options.types.includes(expr.valueType)) {
          return "Wrong type";
        }

        if (options.idTable && (expr.valueType === "id") && (options.idTable !== expr.idTable)) {
          return "Wrong table";
        }
        break;

      case "field":
        var column = this.schema.getColumn(expr.table, expr.column);
        if (!column) {
          return "Missing column";
        }

        // Validate expression
        if (column.expr) {
          // Use depth to prevent infinite recursion
          error = this.validateExprInternal(column.expr, _.extend({}, options, {depth: (options.depth || 0) + 1}));
          if (error) {
            return error;
          }
        }
        break;

      case "op":
        // Validate exprs
        for (let subexpr of expr.exprs) {
          error = this.validateExprInternal(subexpr, _.omit(options, "types", "enumValueIds", "idTable"));
          if (error) {
            return error;
          }
        }

        // Find op
        var opItems = this.exprUtils.findMatchingOpItems({op: expr.op, lhsExpr: expr.exprs[0], resultTypes: options.types});
        if (opItems.length === 0) {
          return "No matching op";
        }
        break;

      case "scalar":
        // Validate joins
        if (!this.exprUtils.areJoinsValid(expr.table, expr.joins)) {
          return "Invalid joins";
        }

        var exprTable = this.exprUtils.followJoins(expr.table, expr.joins);
        error = this.validateExprInternal(expr.expr, _.extend({}, options, {table: exprTable}));
        if (error) {
          return error;
        }
        break;

      case "case":
        // Validate cases
        for (let cse of expr.cases) {
          error = this.validateExprInternal(cse.when, _.extend({}, options, {types: ["boolean"]}));
          if (error) {
            return error;
          }

          error = this.validateExprInternal(cse.then, options);
          if (error) {
            return error;
          }
        }

        error = this.validateExprInternal(expr.else, options);
        if (error) {
          return error;
        }
        break;

      case "score":
        error = this.validateExprInternal(expr.input, _.extend({}, options, {types: ["enum", "enumset"]}));
        if (error) {
          return error;
        }

        if (expr.input) {
          enumValueIds = _.pluck(this.exprUtils.getExprEnumValues(expr.input) || [], "id");
        } else {
          enumValueIds = null;
        }

        for (key in expr.scores) {
          value = expr.scores[key];
          if (enumValueIds && !enumValueIds.includes(key)) {
            return "Invalid score enum";
          }

          error = this.validateExprInternal(value, _.extend({}, options, {types: ["number"]}));
          if (error) {
            return error;
          }
        }
        break;

      case "build enumset":
        for (key in expr.values) {
          value = expr.values[key];
          if (options.enumValueIds && !options.enumValueIds.includes(key)) {
            return "Invalid score enum";
          }

          error = this.validateExprInternal(value, _.extend({}, options, {types: ["boolean"]}));
          if (error) {
            return error;
          }
        }
        break;

      case "variable":
        // Get variable
        var variable = _.findWhere(this.variables, {id: expr.variableId});
        if (!variable) {
          return `Missing variable ${expr.variableId}`;
        }
        break;

      case "spatial join":
        if (!expr.toTable) {
          return "Missing to table";
        }
        if (!expr.fromGeometryExpr) {
          return "Missing from geometry";
        }
        if (!expr.toGeometryExpr) {
          return "Missing to geometry";
        }
        if (!expr.radiusExpr) {
          return "Radius required";
        }
        break;
      case "extension":
        const err = getExprExtension(expr.extension).validateExpr(expr, options, this.schema, this.variables)
        if (err) {
          return err
        }
    }

    // Validate table
    if (options.idTable && this.exprUtils.getExprIdTable(expr) && (this.exprUtils.getExprIdTable(expr) !== options.idTable)) {
      return "Wrong idTable";
    }

    // Validate type if present
    if (options.types && !options.types.includes(this.exprUtils.getExprType(expr)!)) {
      return "Invalid type";
    }

    // Validate enums
    if (options.enumValueIds && (this.exprUtils.getExprType(expr) == "enum" || this.exprUtils.getExprType(expr) == "enumset")) {
      if (_.difference(_.pluck(this.exprUtils.getExprEnumValues(expr) || [], "id"), options.enumValueIds).length > 0) {
        return "Invalid enum";
      }
    }

    return null;
  }
}

