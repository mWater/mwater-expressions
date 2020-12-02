import Schema from "./Schema";
import { Variable, Expr, AggrStatus } from "./types";

/** Validates expressions. If an expression has been cleaned, it will always be valid */
export default class ExprValidator {
  constructor(schema: Schema)

  /** Validates an expression, returning null if it is valid, otherwise return an error string
   * options are:
   *   table: optional current table. expression must be related to this table or will be stripped
   *   types: optional types to limit to
   *   enumValueIds: ids of enum values that are valid if type is enum
   *   idTable: table that type of id must be from
   *   aggrStatuses: statuses of aggregation to allow. list of "individual", "literal", "aggregate". Default: ["individual", "literal"]
   */
  validateExpr(expr: Expr, options: { table?: string, types?: string[], enumValueIds?: string[], idTable?: string, aggrStatuses?: AggrStatus[] }): string | null
}
