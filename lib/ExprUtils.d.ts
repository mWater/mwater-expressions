import Schema from "./Schema";
import { Variable, AggrStatus, Expr, LocalizedString, FieldExpr, EnumValue } from "./types";

export default class ExprUtils {
  constructor(schema: Schema, variables?: Variable[])

  summarizeExpr(expr: Expr, locale?: string): string

  getExprType(expr: Expr): string | null

  getExprAggrStatus(expr: Expr): AggrStatus | null

  getExprEnumValues(expr: Expr): EnumValue[] | null

  /** Gets the id table of an expression of type id */
  getExprIdTable(expr: Expr): string | null

  /** Converts a literal value related to an expression to a string, using name of enums. preferEnumCodes tries to use code over name */
  stringifyExprLiteral(expr: Expr, literal: any, locale?: string, preferEnumCodes?: boolean): string

  /** Localize a localized string */
  static localizeString(str?: LocalizedString | null, locale?: string): string

  /** Localize a localized string */
  localizeString(str?: LocalizedString | null, locale?: string): string

  /** Get a list of fields that are referenced in a an expression
   * Useful to know which fields and joins are used. Includes joins as fields */
  getReferencedFields(expr: Expr): FieldExpr[]

  /** Replace variables with literal values */
  inlineVariableValues(expr: Expr, variableValues: { [variableId: string]: any }): Expr

  /** Determine if op is aggregate */
  static isOpAggr(op: string): boolean

  /** Determine if op is prefix */
  static isOpPrefix(op: string): boolean
}
