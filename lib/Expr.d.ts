import { LiteralType, EnumValue } from "./Schema";
import { LocalizedString } from "./LocalizedString";
/** Expression. Can be null */
export declare type Expr = LiteralExpr | FieldExpr | OpExpr | IdExpr | ScalarExpr | VariableExpr | null;
export interface LiteralExpr {
    type: "literal";
    valueType: string;
    idTable?: string;
    value: any;
}
export interface IdExpr {
    type: "id";
    table: string;
}
export interface FieldExpr {
    type: "field";
    table: string;
    column: string;
}
export interface OpExpr {
    type: "op";
    table: string;
    op: string;
    exprs: Expr[];
}
/** Expression that references a variable */
export interface VariableExpr {
    type: "variable";
    /** Table of expression that variable references (if relevant) */
    table?: string;
    variableId: string;
}
/** Variable that is referenced in an expression */
export interface Variable {
    /** Unique id of the variable */
    id: string;
    /** Localized name of the variable */
    name: LocalizedString;
    /** Localized description of the variable */
    desc?: LocalizedString;
    /** Type of the value of the variable */
    type: LiteralType;
    /** Table that variable expression is for. If present, is a non-literal variable and the value is an expression. Note: must be non-aggregate */
    table?: string;
    /** For enum and enumset variables */
    enumValues?: EnumValue[];
    /** table for id, id[] fields */
    idTable?: string;
}
export interface ScalarExpr {
    type: "scalar";
    /** Table id of start table */
    table: string;
    /** Array of join columns to follow to get to table of expr. All must be `join` type */
    joins: string[];
    /** Expression from final table to get value */
    expr: Expr;
}
export declare type AggrStatus = "individual" | "literal" | "aggregate";
