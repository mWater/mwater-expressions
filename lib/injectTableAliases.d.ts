import { JsonQL } from "jsonql";
/** Recursively inject table aliases
 * aliases is map of replacement to table aliases. For example, { "{a}": b } will replace "{a}" with "b"
 */
export declare function injectTableAliases(jsonql: any, aliases: {
    [from: string]: string;
}): any;
/** Recursively inject table alias tableAlias for `{alias}` */
export declare function injectTableAlias(jsonql: JsonQL, tableAlias: string): JsonQL;
