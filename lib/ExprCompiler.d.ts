import { JsonQLExpr, JsonQLFrom } from "jsonql";
import Schema from "./Schema";
import { BuildEnumsetExpr, CaseExpr, Column, Expr, FieldExpr, LegacyComparisonExpr, LegacyLogicalExpr, OpExpr, ScalarExpr, ScoreExpr, Variable, VariableExpr } from "./types";
/** Compiles expressions to JsonQL. Assumes that geometry is in Webmercator (3857) */
export default class ExprCompiler {
    schema: Schema;
    variables: Variable[];
    variableValues: {
        [variableId: string]: Expr;
    };
    constructor(schema: Schema, variables?: Variable[], variableValues?: {
        [variableId: string]: Expr;
    });
    /** Compile an expression. Pass expr and tableAlias. */
    compileExpr(options: {
        expr: Expr;
        tableAlias: string;
    }): JsonQLExpr;
    /** Compile a field expressions */
    compileFieldExpr(options: {
        expr: FieldExpr;
        tableAlias: string;
    }): JsonQLExpr;
    compileScalarExpr(options: {
        expr: ScalarExpr;
        tableAlias: string;
    }): JsonQLExpr;
    /** Compile a join into an on or where clause
     *  fromTableID: column definition
     *  joinColumn: column definition
     *  fromAlias: alias of from table
     *  toAlias: alias of to table
     */
    compileJoin(fromTableId: string, joinColumn: Column, fromAlias: string, toAlias: string): any;
    compileOpExpr(options: {
        expr: OpExpr;
        tableAlias: string;
    }): JsonQLExpr;
    compileCaseExpr(options: {
        expr: CaseExpr;
        tableAlias: string;
    }): JsonQLExpr;
    compileScoreExpr(options: {
        expr: ScoreExpr;
        tableAlias: string;
    }): JsonQLExpr;
    compileBuildEnumsetExpr(options: {
        expr: BuildEnumsetExpr;
        tableAlias: string;
    }): JsonQLExpr;
    compileComparisonExpr(options: {
        expr: LegacyComparisonExpr;
        tableAlias: string;
    }): JsonQLExpr;
    compileLogicalExpr(options: {
        expr: LegacyLogicalExpr;
        tableAlias: string;
    }): JsonQLExpr;
    compileColumnRef(column: any, tableAlias: string): JsonQLExpr;
    compileTable(tableId: string, alias: string): JsonQLFrom;
    compileVariableExpr(options: {
        expr: VariableExpr;
        tableAlias: string;
    }): JsonQLExpr;
}
