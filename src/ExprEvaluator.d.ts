import { Schema } from "./Schema";
import { Variable, Expr } from "./Expr";

/** a row is a plain object that has the following functions as properties */
export interface ExprEvaluatorRow {
  /** gets primary key of row. callback is called with (error, value) */
  getPrimaryKey(callback: (error: any, value?: any) => void): void

  /** gets the value of a column. callback is called with (error, value) 
   * For joins, getField will get array of rows for 1-n and n-n joins and a row for n-1 and 1-1 joins
   */
  getField(columnId: string, callback: (error: any, value?: any) => void): void
}

export interface ExprEvaluatorContext {
  /** current row. Optional for aggr expressions */
  row?: ExprEvaluatorRow
  /** array of rows (for aggregate expressions) */
  rows?: ExprEvaluatorRow[]
}

/**
 * Evaluates an expression given a context.
 *
 * context is a plain object that contains:
 * row: current row (see below)
 * rows: array of rows (for aggregate expressions. See below for row definition)
 *
 * a row is a plain object that has the following functions as properties:
 *  getPrimaryKey(callback) : gets primary key of row. callback is called with (error, value)
 *  getField(columnId, callback) : gets the value of a column. callback is called with (error, value)
 *
 * For joins, getField will get array of rows for 1-n and n-n joins and a row for n-1 and 1-1 joins
 */
export default class ExprEvaluator {
  constructor(schema: Schema, locale?: string, variables?: Variable[], variableValues?: { [variableId: string]: any })
  evaluate(expr: Expr, context: ExprEvaluatorContext, callback: (error: any, value?: any) => void): void
}
