import Schema from "./Schema";
import { Variable, AggrStatus, Expr, LocalizedString, FieldExpr, EnumValue, LiteralType } from "./types";

export default class ExprUtils {
  constructor(schema: Schema, variables?: Variable[])

  summarizeExpr(expr: Expr, locale?: string): string

  getExprType(expr: Expr): LiteralType | null

  getExprAggrStatus(expr: Expr): AggrStatus | null

  getExprEnumValues(expr: Expr): EnumValue[] | null

  /** Gets the id table of an expression of type id */
  getExprIdTable(expr: Expr): string | null

  /** Converts a literal value related to an expression to a string, using name of enums. preferEnumCodes tries to use code over name */
  stringifyExprLiteral(expr: Expr, literal: any, locale?: string, preferEnumCodes?: boolean): string

  /** Localize a localized string */
  static localizeString(str?: LocalizedString | string | null, locale?: string): string

  /** Localize a localized string */
  localizeString(str?: LocalizedString | string | null, locale?: string): string

  /** Get a list of fields that are referenced in a an expression
   * Useful to know which fields and joins are used. Includes joins as fields */
  getReferencedFields(expr: Expr): FieldExpr[]

  /** Replace variables with literal values */
  inlineVariableValues(expr: Expr, variableValues: { [variableId: string]: Expr }): Expr

  /** Determine if op is aggregate */
  static isOpAggr(op: string): boolean

  /** Determine if op is prefix */
  static isOpPrefix(op: string): boolean

  /** Follows a list of joins to determine final table */
  followJoins(startTable: string, joins: string[]): string

  /** Determines if an set of joins contains a multiple */
  isMultipleJoins(table: string, joins: string[]): boolean

  /** Determines if an set of joins are valid */
  areJoinsValid(table: string, joins: string[]): boolean

  /** 
   * Search can contain resultTypes, lhsExpr, op, aggr. lhsExpr is actual expression of lhs. resultTypes is optional array of result types
   * If search ordered is not true, excludes ordered ones
   * If prefix, only prefix
   * Results are array of opItems. */
  findMatchingOpItems(search: {
    resultTypes?: LiteralType[]
    lhsExpr?: Expr
    op?: string
    ordered?: boolean
    prefix?: boolean
    aggr?: boolean
  }): OpItem[] 
}

/** opItems are a list of ops for various types */
interface OpItem {
  /** e.g. "=" */
  op: string
  /** e.g. "is" */
  name: string
  /** resulting type from op. e.g. "boolean" */
  resultType: LiteralType
  /** array of types of expressions required for arguments */
  exprTypes: LiteralType[]
  /** type of n more expressions (like "and" that takes n arguments) */
  moreExprType?: LiteralType
  /** true if name goes before LHS value */
  prefix?: boolean
  /** overrides name when displayed as prefix */
  prefixLabel?: string
  /** optional condition function on LHS expr that tests if applicable (for "within" which only applies to hierarchical tables) */
  lhsCond?: (lhs: Expr) => boolean
  /** prefer rhs literal */
  rhsLiteral?: boolean
  /** string to put between exprs when prefix type */
  joiner?: string
  /** true if aggregating (e.g. sum) */
  aggr?: boolean
  /** for aggr = true if table must be have ordering */
  ordered?: boolean
  /** placeholder for lhs expression */
  lhsPlaceholder?: string
  /** placeholder for rhs expression */
  rhsPlaceholder?: string
}
