"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getExprExtension = exports.registerExprExtension = void 0;
/** Global lookup of extensions */
var exprExtensions = {};
/** Register an extension to expressions.
 * @param id referenced in type { type: "extension", extension: <id>, ... }
 */
function registerExprExtension(id, extension) {
    exprExtensions[id] = extension;
}
exports.registerExprExtension = registerExprExtension;
function getExprExtension(id) {
    var extension = exprExtensions[id];
    if (!extension) {
        throw new Error("Extension " + id + " not found");
    }
    return extension;
}
exports.getExprExtension = getExprExtension;
