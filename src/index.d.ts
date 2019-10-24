import { JsonQL, JsonQLExpr, JsonQLFrom } from "jsonql"
import Schema from "./Schema"
import { Variable, Expr, AggrStatus, LocalizedString, EnumValue, FieldExpr } from "./types"

export * from './types'
export { default as DataSource } from './DataSource'
export { default as ExprEvaluator } from './ExprEvaluator'
export { default as ExprValidator } from './ExprValidator'
export { default as Schema } from './Schema'
export * from './PromiseExprEvaluator'

export class ExprUtils {
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
}

export class ExprCompiler {
  constructor(schema: Schema, variables?: Variable[], variableValues?: { [variableId: string]: any })

  compileExpr(options: { expr: Expr, tableAlias: string }): JsonQLExpr
  compileTable(table: string, alias: string): JsonQLFrom
}

/** Cleans expressions. Cleaning means nulling invalid (not just incomplete) expressions if they cannot be auto-fixed. */
export class ExprCleaner {
  constructor(schema: Schema, variables?: Variable[])

  /** Clean an expression, returning null if completely invalid, otherwise removing
    invalid parts. Attempts to correct invalid types by wrapping in other expressions.
    e.g. if an enum is chosen when a boolean is required, it will be wrapped in "= any" op
    options: 
      table: optional current table. expression must be related to this table or will be stripped
      types: optional types to limit to
      enumValueIds: ids of enum values that are valid if type is enum
      idTable: table that type of id must be from
      aggrStatuses: statuses of aggregation to allow. list of "individual", "literal", "aggregate". Default: ["individual", "literal"]
  */
  cleanExpr(expr:Expr, options: { table?: string, types?: string[], enumValueIds?: string[], idTable?: string, aggrStatuses?: AggrStatus[] }): Expr
}

/** Recursively inject table alias tableAlias for `{alias}` */
export function injectTableAlias(jsonql: JsonQL, tableAlias: string): JsonQL
