import { JsonQLExpr } from "jsonql"
import {
  ExtensionExpr,
  Schema,
  Variable,
  Expr,
  AggrStatus,
  EnumValue,
  LiteralType,
  FieldExpr,
  PromiseExprEvaluatorContext
} from "../src"
import { CleanExprOptions } from "../src/ExprCleaner"
import { ValidateOptions } from "../src/ExprValidator"
import { ExprExtension, registerExprExtension } from "../src/extensions"

/** Sets up a test extension */
export function setupTestExtension() {
  registerExprExtension("test", testExtension)
}

/** Extension to the expression language. Referenced by ExtentionExprs  */
const testExtension: ExprExtension<ExtensionExpr> = {
  cleanExpr(expr: ExtensionExpr, options: CleanExprOptions, schema: Schema, variables: Variable[]): Expr {
    return expr
  },

  getExprAggrStatus(expr: ExtensionExpr, schema: Schema, variables: Variable[]) {
    return "individual"
  },

  validateExpr(expr: ExtensionExpr, options: ValidateOptions, schema: Schema, variables: Variable[]) {
    return "test"
  },

  /** Return array of { id: <enum value>, name: <localized label of enum value> } */
  getExprEnumValues(expr: ExtensionExpr, schema: Schema, variables: Variable[]) {
    return null
  },

  /** Gets the id table of an expression of type id */
  getExprIdTable(expr: Expr, schema: Schema, variables: Variable[]) {
    return "t1"
  },

  /** Gets the type of an expression */
  getExprType(expr: Expr, schema: Schema, variables: Variable[]) {
    return "number"
  },

  /** Summarizes expression as text */
  summarizeExpr(expr: Expr, locale: string | undefined, schema: Schema, variables: Variable[]) {
    return "test"
  },

  /** Get a list of fields that are referenced in a an expression
   * Useful to know which fields and joins are used. Includes joins as fields
   */
  getReferencedFields(expr: Expr, schema: Schema, variables: Variable[]) {
    return []
  },

  /** Compile to JsonQL */
  compileExpr(
    expr: Expr,
    tableAlias: string,
    schema: Schema,
    variables: Variable[],
    variableValues: { [variableId: string]: Expr }
  ): JsonQLExpr {
    return { type: "literal", value: 4 }
  },

  /** Evaluate an expression given the context */
  evaluate(
    expr: Expr,
    context: PromiseExprEvaluatorContext,
    schema: Schema | undefined,
    locale: string | undefined,
    variables: Variable[] | undefined,
    variableValues: { [variableId: string]: any } | undefined
  ): Promise<any> {
    return Promise.resolve(4)
  },

  /** Evaluate an expression synchronously */
  evaluateSync(
    expr: Expr,
    schema: Schema | undefined,
    locale: string | undefined,
    variables: Variable[] | undefined,
    variableValues: { [variableId: string]: any } | undefined
  ): any {
    return 4
  }
}
