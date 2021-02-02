import { JsonQLExpr } from "jsonql";
import { CleanExprOptions } from "./ExprCleaner";
import { ValidateOptions } from "./ExprValidator";
import { PromiseExprEvaluatorContext } from "./PromiseExprEvaluator";
import Schema from "./Schema";
import { AggrStatus, EnumValue, Expr, ExtensionExpr, FieldExpr, LiteralType, Variable } from "./types";

/** Global lookup of extensions */
const exprExtensions: { [id: string]: ExprExtension<ExtensionExpr> } = {}

/** Extension to the expression language. Referenced by ExtentionExprs  */
export interface ExprExtension<T extends ExtensionExpr> {
  cleanExpr(expr: T, options: CleanExprOptions, schema: Schema, variables: Variable[]): Expr

  getExprAggrStatus(expr: T, schema: Schema, variables: Variable[]): AggrStatus | null
  
  validateExpr(expr: T, options: ValidateOptions, schema: Schema, variables: Variable[]): string | null

  /** Return array of { id: <enum value>, name: <localized label of enum value> } */
  getExprEnumValues(expr: T, schema: Schema, variables: Variable[]): EnumValue[] | null 

  /** Gets the id table of an expression of type id */
  getExprIdTable(expr: Expr, schema: Schema, variables: Variable[]): string | null

  /** Gets the type of an expression */
  getExprType(expr: Expr, schema: Schema, variables: Variable[]): LiteralType | null 

  /** Summarizes expression as text */
  summarizeExpr(expr: Expr, locale: string | undefined, schema: Schema, variables: Variable[]): string

  /** Get a list of fields that are referenced in a an expression
   * Useful to know which fields and joins are used. Includes joins as fields
   */
  getReferencedFields(expr: Expr, schema: Schema, variables: Variable[]): FieldExpr[]

  /** Compile to JsonQL */
  compileExpr(expr: Expr, tableAlias: string, schema: Schema, variables: Variable[], variableValues: { [variableId: string]: Expr }): JsonQLExpr

  /** Evaluate an expression given the context */
  evaluate(expr: Expr, 
    context: PromiseExprEvaluatorContext, 
    schema: Schema | undefined, 
    locale: string | undefined, 
    variables: Variable[] | undefined,
    variableValues: { [variableId: string]: any } | undefined): Promise<any> 

  /** Evaluate an expression synchronously */
  evaluateSync(expr: Expr, 
    schema: Schema | undefined, 
    locale: string | undefined, 
    variables: Variable[] | undefined,
    variableValues: { [variableId: string]: any } | undefined): any
}

/** Register an extension to expressions. 
 * @param id referenced in type { type: "extension", extension: <id>, ... }
 */
export function registerExprExtension(id: string, extension: ExprExtension<ExtensionExpr>) {
  exprExtensions[id] = extension
}

export function getExprExtension(id: string): ExprExtension<ExtensionExpr> {
  const extension = exprExtensions[id]
  if (!extension) {
    throw new Error(`Extension ${id} not found`)
  }
  return extension
}
