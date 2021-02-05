import { JsonQLExpr, JsonQLFrom } from "jsonql";
import Schema from "./Schema";
import { Expr, Variable } from "./types";

export default class ExprCompiler {
  constructor(schema: Schema, variables?: Variable[], variableValues?: { [variableId: string]: Expr })

  compileExpr(options: { expr: Expr, tableAlias: string }): JsonQLExpr | null
  compileTable(table: string, alias: string): JsonQLFrom
}
