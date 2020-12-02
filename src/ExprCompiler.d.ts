import { JsonQLExpr, JsonQLFrom } from "jsonql";
import Schema from "./Schema";
import { Expr, Variable } from "./types";

export default class ExprCompiler {
  constructor(schema: Schema, variableValues?: { [variableId: string]: any })

  compileExpr(options: { expr: Expr, tableAlias: string }): JsonQLExpr
  compileTable(table: string, alias: string): JsonQLFrom
}
