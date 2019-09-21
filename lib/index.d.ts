import { JsonQL, JsonQLExpr, JsonQLFrom } from "jsonql"
import { Schema, EnumValue } from "./Schema"
import { Variable, Expr, AggrStatus } from "./Expr"

export * from './DataSource'
export * from './Schema'
export * from './Expr'
export * from './ExprValidator'
export * from './ExprEvaluator'
export * from './PromiseExprEvaluator'
export * from './LocalizedString'

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
