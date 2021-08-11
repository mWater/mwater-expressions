import Schema from "./Schema";
import { AggrStatus, CaseExpr, EnumValue, Expr, FieldExpr, LiteralType, LocalizedString, ScalarExpr, Variable } from "./types";
/** opItems are a list of ops for various types */
interface OpItem {
    /** e.g. "=" */
    op: string;
    /** e.g. "is" */
    name: string;
    /** Optional description */
    desc?: string;
    /** resulting type from op. e.g. "boolean" */
    resultType: LiteralType;
    /** array of types of expressions required for arguments */
    exprTypes: LiteralType[];
    /** type of n more expressions (like "and" that takes n arguments) */
    moreExprType?: LiteralType;
    /** true if name goes before LHS value */
    prefix?: boolean;
    /** overrides name when displayed as prefix */
    prefixLabel?: string;
    /** optional condition function on LHS expr that tests if applicable (for "within" which only applies to hierarchical tables) */
    lhsCond?: (lhs: Expr, exprUtils: ExprUtils) => boolean;
    /** prefer rhs literal */
    rhsLiteral?: boolean;
    /** string to put between exprs when prefix type */
    joiner?: string;
    /** true if aggregating (e.g. sum) */
    aggr?: boolean;
    /** for aggr = true if table must be have ordering */
    ordered?: boolean;
    /** placeholder for lhs expression */
    lhsPlaceholder?: string;
    /** placeholder for rhs expression */
    rhsPlaceholder?: string;
}
export default class ExprUtils {
    schema: Schema;
    variables: Variable[];
    constructor(schema: Schema, variables?: Variable[]);
    /**
     * Search can contain resultTypes, lhsExpr, op, aggr. lhsExpr is actual expression of lhs. resultTypes is optional array of result types
     * If search ordered is not true, excludes ordered ones
     * If prefix, only prefix
     * Results are array of opItems. */
    findMatchingOpItems(search: {
        resultTypes?: LiteralType[];
        lhsExpr?: Expr;
        op?: string;
        ordered?: boolean;
        prefix?: boolean;
        aggr?: boolean;
    }): OpItem[];
    /** Determine if op is aggregate */
    static isOpAggr(op: string): boolean;
    /** Determine if op is prefix */
    static isOpPrefix(op: string): boolean;
    /** Follows a list of joins to determine final table */
    followJoins(startTable: string, joins: string[]): string;
    /** Determines if an set of joins contains a multiple */
    isMultipleJoins(table: string, joins: string[]): boolean;
    /** Return array of { id: <enum value>, name: <localized label of enum value> } */
    getExprEnumValues(expr: Expr): EnumValue[] | null;
    /** Gets the id table of an expression of type id */
    getExprIdTable(expr: Expr): string | null;
    /** Gets the type of an expression */
    getExprType(expr: Expr): LiteralType | null;
    /** Determines the aggregation status of an expression. This is whether the expression is
     * aggregate (like sum, avg, etc) or individual (a regular field-containing expression) or
     * literal (which is neither, just a number or text).
     * Invisible second parameter is depth to prevent infinite recursion */
    getExprAggrStatus(expr: Expr, _depth?: number): AggrStatus | null;
    /** Determines if an set of joins are valid */
    areJoinsValid(table: string, joins: string[]): boolean;
    getExprTable(expr: Expr): string | null | undefined;
    getAggrTypes(expr: Expr): any[];
    localizeString(name: LocalizedString | string | null | undefined, locale?: string): string;
    static localizeString(name: LocalizedString | string | null | undefined, locale?: string): string;
    static andExprs(table: string, ...exprs: Expr[]): import("./types").LiteralExpr | FieldExpr | import("./types").OpExpr | import("./types").IdExpr | ScalarExpr | CaseExpr | import("./types").ScoreExpr | import("./types").BuildEnumsetExpr | import("./types").VariableExpr | import("./types").ExtensionExpr | import("./types").LegacyComparisonExpr | import("./types").LegacyCountExpr | Expr[] | {
        type: string;
        op: string;
        table: string;
        exprs: (Expr | Expr[])[];
    } | null;
    /** Summarizes expression as text */
    summarizeExpr(expr: Expr, locale?: string): string;
    summarizeScalarExpr(expr: ScalarExpr, locale?: string): string;
    summarizeCaseExpr(expr: CaseExpr, locale?: string): string;
    /** Converts a literal value related to an expression to a string, using name of enums. preferEnumCodes tries to use code over name */
    stringifyExprLiteral(expr: Expr, literal: any, locale?: string, preferEnumCodes?: boolean): string;
    stringifyLiteralValue(type: LiteralType, value: any, locale?: string, enumValues?: EnumValue[] | null, preferEnumCodes?: boolean): any;
    /** Get all comparison ops (id and name) for a given left hand side type DEPRECATED
     * @deprecated
     */
    getComparisonOps(lhsType: any): {
        id: string;
        name: string;
    }[];
    /** Get the right hand side type for a comparison DEPRECATED
     * @deprecated
     */
    getComparisonRhsType(lhsType: any, op: any): any;
    /** Get a list of fields that are referenced in a an expression
     * Useful to know which fields and joins are used. Includes joins as fields
     */
    getReferencedFields(expr: Expr): FieldExpr[];
    /** Replace variables with literal values */
    inlineVariableValues(expr: Expr, variableValues: {
        [variableId: string]: Expr;
    }): Expr;
}
export {};
