import ExprUtils from "./ExprUtils";
import { Schema } from ".";
import { Variable, Expr, LiteralType, AggrStatus, FieldExpr, OpExpr, ScalarExpr, LiteralExpr, CaseExpr, IdExpr, ScoreExpr, BuildEnumsetExpr, VariableExpr, LegacyComparisonExpr, LegacyLogicalExpr, LegacyCountExpr } from "./types";
export interface CleanExprOptions {
    /** optional current table. expression must be related to this table or will be stripped */
    table?: string;
    /** optional types to limit to */
    types?: LiteralType[];
    /** ids of enum values that are valid if type is enum */
    enumValueIds?: string[];
    /** table that type of id must be from */
    idTable?: string;
    /** statuses of aggregation to allow. list of "individual", "literal", "aggregate". Default: ["individual", "literal"] */
    aggrStatuses: AggrStatus[];
}
/** Cleans expressions. Cleaning means nulling invalid (not just incomplete) expressions if they cannot be auto-fixed.
 * The resulting expression must be valid. */
export default class ExprCleaner {
    schema: Schema;
    variables: Variable[];
    exprUtils: ExprUtils;
    constructor(schema: Schema, variables?: Variable[]);
    cleanExpr(expr: Expr, options?: {
        /** optional current table. expression must be related to this table or will be stripped */
        table?: string;
        /** optional types to limit to */
        types?: LiteralType[];
        /** ids of enum values that are valid if type is enum */
        enumValueIds?: string[];
        /** table that type of id must be from */
        idTable?: string;
        /** statuses of aggregation to allow. list of "individual", "literal", "aggregate". Default: ["individual", "literal"] */
        aggrStatuses?: AggrStatus[];
    }): Expr;
    /** Removes references to non-existent tables */
    cleanFieldExpr(expr: FieldExpr, options: CleanExprOptions): FieldExpr | null;
    cleanOpExpr(expr: OpExpr, options: CleanExprOptions): Expr;
    cleanScalarExpr(expr: ScalarExpr, options: CleanExprOptions): Expr;
    cleanLiteralExpr(expr: LiteralExpr, options: CleanExprOptions): LiteralExpr | null;
    cleanCaseExpr(expr: CaseExpr, options: CleanExprOptions): LiteralExpr | FieldExpr | OpExpr | IdExpr | ScalarExpr | CaseExpr | ScoreExpr | BuildEnumsetExpr | VariableExpr | import("./types").ExtensionExpr | import("./types").LegacyExpr | null;
    cleanIdExpr(expr: IdExpr, options: CleanExprOptions): IdExpr | null;
    cleanScoreExpr(expr: ScoreExpr, options: CleanExprOptions): ScoreExpr;
    cleanBuildEnumsetExpr(expr: BuildEnumsetExpr, options: CleanExprOptions): BuildEnumsetExpr;
    cleanVariableExpr(expr: VariableExpr, options: CleanExprOptions): VariableExpr | null;
    cleanComparisonExpr(expr: LegacyComparisonExpr, options: CleanExprOptions): Expr;
    cleanLogicalExpr(expr: LegacyLogicalExpr, options: CleanExprOptions): Expr;
    cleanCountExpr(expr: LegacyCountExpr, options: CleanExprOptions): Expr;
}
