"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.injectTableAlias = exports.injectTableAliases = void 0;
const lodash_1 = __importDefault(require("lodash"));
/** Recursively inject table aliases
 * aliases is map of replacement to table aliases. For example, { "{a}": b } will replace "{a}" with "b"
 */
function injectTableAliases(jsonql, aliases) {
    // Handle empty
    if (!jsonql) {
        return jsonql;
    }
    // Handle arrays
    if (lodash_1.default.isArray(jsonql)) {
        return lodash_1.default.map(jsonql, (item) => {
            return injectTableAliases(item, aliases);
        });
    }
    // Handle non-objects by leaving alone
    if (!lodash_1.default.isObject(jsonql)) {
        return jsonql;
    }
    // Handle field
    if (jsonql.type === "field" && aliases[jsonql.tableAlias]) {
        return lodash_1.default.extend({}, jsonql, {
            tableAlias: aliases[jsonql.tableAlias]
        });
    }
    // Recurse object keys
    return lodash_1.default.mapValues(jsonql, (value) => {
        return injectTableAliases(value, aliases);
    });
}
exports.injectTableAliases = injectTableAliases;
/** Recursively inject table alias tableAlias for `{alias}` */
function injectTableAlias(jsonql, tableAlias) {
    return injectTableAliases(jsonql, { "{alias}": tableAlias });
}
exports.injectTableAlias = injectTableAlias;
