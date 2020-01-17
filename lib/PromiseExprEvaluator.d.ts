import { Expr, Variable, CaseExpr, ScalarExpr, VariableExpr, ScoreExpr, BuildEnumsetExpr } from "./types";
import Schema from "./Schema";
/** Represents a row to be evaluated */
export interface PromiseExprEvaluatorRow {
    /** gets primary key of row */
    getPrimaryKey(): Promise<any>;
    /** gets the value of a column.
     * For joins, getField will get array of rows for 1-n and n-n joins and a row or null for n-1 and 1-1 joins
     */
    getField(columnId: string): Promise<any>;
}
export interface PromiseExprEvaluatorContext {
    /** current row. Optional for aggr expressions */
    row?: PromiseExprEvaluatorRow;
    /** array of rows (for aggregate expressions) */
    rows?: PromiseExprEvaluatorRow[];
}
/** Expression evaluator that is promise-based */
export declare class PromiseExprEvaluator {
    schema?: Schema;
    locale?: string;
    variables?: Variable[];
    variableValues?: {
        [variableId: string]: any;
    };
    constructor(options: {
        schema?: Schema;
        locale?: string;
        variables?: Variable[];
        variableValues?: {
            [variableId: string]: any;
        };
    });
    /** Evaluate an expression given the context */
    evaluate(expr: Expr, context: PromiseExprEvaluatorContext): Promise<any>;
    evaluateBuildEnumset(expr: BuildEnumsetExpr, context: PromiseExprEvaluatorContext): Promise<any>;
    evaluateScore(expr: ScoreExpr, context: PromiseExprEvaluatorContext): Promise<any>;
    evaluateCase(expr: CaseExpr, context: PromiseExprEvaluatorContext): Promise<any>;
    evaluateScalar(expr: ScalarExpr, context: PromiseExprEvaluatorContext): Promise<any>;
    evaluateOp(table: string | undefined, op: string, exprs: Expr[], context: PromiseExprEvaluatorContext): Promise<any>;
    /** NOTE: This is not technically correct. It's not a window function (as window
     * functions can't be used in where clauses) but rather a special query */
    evaluateIsLatest(table: string, exprs: Expr[], context: PromiseExprEvaluatorContext): Promise<boolean | null>;
    evaluteAggrOp(table: string, op: string, exprs: Expr[], context: PromiseExprEvaluatorContext): Promise<any>;
    evaluateOpValues(op: string, exprs: Expr[], values: any[]): any;
    evaluateVariable(expr: VariableExpr, context: PromiseExprEvaluatorContext): Promise<any>;
}
