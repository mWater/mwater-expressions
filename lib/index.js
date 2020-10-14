"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    Object.defineProperty(o, k2, { enumerable: true, get: function() { return m[k]; } });
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __exportStar = (this && this.__exportStar) || function(m, exports) {
    for (var p in m) if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports, p)) __createBinding(exports, m, p);
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.flattenContents = exports.ColumnNotFoundException = exports.NullDataSource = exports.PriorityDataQueue = exports.ExprCompiler = exports.ExprCleaner = exports.WeakCache = exports.ExprUtils = exports.Schema = exports.ExprValidator = exports.DataSource = void 0;
__exportStar(require("./types"), exports);
var DataSource_1 = require("./DataSource");
Object.defineProperty(exports, "DataSource", { enumerable: true, get: function () { return __importDefault(DataSource_1).default; } });
var ExprValidator_1 = require("./ExprValidator");
Object.defineProperty(exports, "ExprValidator", { enumerable: true, get: function () { return __importDefault(ExprValidator_1).default; } });
var Schema_1 = require("./Schema");
Object.defineProperty(exports, "Schema", { enumerable: true, get: function () { return __importDefault(Schema_1).default; } });
__exportStar(require("./PromiseExprEvaluator"), exports);
var ExprUtils_1 = require("./ExprUtils");
Object.defineProperty(exports, "ExprUtils", { enumerable: true, get: function () { return __importDefault(ExprUtils_1).default; } });
var WeakCache_1 = require("./WeakCache");
Object.defineProperty(exports, "WeakCache", { enumerable: true, get: function () { return WeakCache_1.WeakCache; } });
/** Cleans expressions. Cleaning means nulling invalid (not just incomplete) expressions if they cannot be auto-fixed. */
var ExprCleaner_1 = require("./ExprCleaner");
Object.defineProperty(exports, "ExprCleaner", { enumerable: true, get: function () { return __importDefault(ExprCleaner_1).default; } });
var ExprCompiler_1 = require("./ExprCompiler");
Object.defineProperty(exports, "ExprCompiler", { enumerable: true, get: function () { return __importDefault(ExprCompiler_1).default; } });
var PriorityDataQueue_1 = require("./PriorityDataQueue");
Object.defineProperty(exports, "PriorityDataQueue", { enumerable: true, get: function () { return __importDefault(PriorityDataQueue_1).default; } });
var NullDataSource_1 = require("./NullDataSource");
Object.defineProperty(exports, "NullDataSource", { enumerable: true, get: function () { return __importDefault(NullDataSource_1).default; } });
var ColumnNotFoundException_1 = require("./ColumnNotFoundException");
Object.defineProperty(exports, "ColumnNotFoundException", { enumerable: true, get: function () { return __importDefault(ColumnNotFoundException_1).default; } });
__exportStar(require("./injectTableAliases"), exports);
/** Flatten a list of contents to columns */
function flattenContents(contents) {
    var columns = [];
    for (var _i = 0, contents_1 = contents; _i < contents_1.length; _i++) {
        var item = contents_1[_i];
        if (item.type == "section") {
            columns = columns.concat(flattenContents(item.contents));
        }
        else {
            columns.push(item);
        }
    }
    return columns;
}
exports.flattenContents = flattenContents;
