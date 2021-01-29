import ExprUtils from "./ExprUtils";
import Schema from "./Schema";
import { AggrStatus, Expr, LiteralType, Variable } from "./types";
export interface ValidateOptions {
    table?: string;
    types?: LiteralType[];
    enumValueIds?: string[];
    idTable?: string;
    aggrStatuses?: AggrStatus[];
}
/** Validates expressions. If an expression has been cleaned, it will always be valid */
export default class ExprValidator {
    schema: Schema;
    variables: Variable[];
    exprUtils: ExprUtils;
    constructor(schema: Schema, variables?: Variable[]);
    /** Validates an expression, returning null if it is valid, otherwise return an error string
     * NOTE: This uses global weak caching and assumes that expressions are never mutated after
     * having been validated!
     * options are:
     *   table: optional current table. expression must be related to this table or will be stripped
     *   types: optional types to limit to
     *   enumValueIds: ids of enum values that are valid if type is enum
     *   idTable: table that type of id must be from
     *   aggrStatuses: statuses of aggregation to allow. list of "individual", "literal", "aggregate". Default: ["individual", "literal"]
     */
    validateExpr(expr: Expr, options?: ValidateOptions): string | null;
    validateExprInternal: (expr: Expr, options: {
        table?: string | undefined;
        types?: LiteralType[] | undefined;
        enumValueIds?: string[] | undefined;
        idTable?: string | undefined;
        aggrStatuses?: AggrStatus[] | undefined;
        depth?: number | undefined;
    }) => string | null;
}
