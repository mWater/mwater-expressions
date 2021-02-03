"use strict";
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
var lodash_1 = __importDefault(require("lodash"));
var ExprUtils_1 = __importDefault(require("./ExprUtils"));
var extensions_1 = require("./extensions");
var WeakCache_1 = require("./WeakCache");
// Weak cache is global to allow validator to be created and destroyed
var weakCache = new WeakCache_1.WeakCache();
/** Validates expressions. If an expression has been cleaned, it will always be valid */
var ExprValidator = /** @class */ (function () {
    function ExprValidator(schema, variables) {
        var _this = this;
        this.validateExprInternal = function (expr, options) {
            var error, key, value;
            var enumValueIds;
            var aggrStatuses = options.aggrStatuses || ["individual", "literal"];
            if (!expr) {
                return null;
            }
            // Allow {} placeholder
            if (lodash_1.default.isEmpty(expr)) {
                return null;
            }
            // Prevent infinite recursion
            if ((options.depth || 0) > 100) {
                return "Circular reference";
            }
            // Check table if not literal
            if (options.table && _this.exprUtils.getExprTable(expr) && _this.exprUtils.getExprTable(expr) !== options.table) {
                return "Wrong table " + _this.exprUtils.getExprTable(expr) + " (expected " + options.table + ")";
            }
            // Literal is ok if right type
            switch (expr.type) {
                case "literal":
                    if (options.types && !options.types.includes(expr.valueType)) {
                        return "Wrong type";
                    }
                    if (options.idTable && (expr.valueType === "id") && (options.idTable !== expr.idTable)) {
                        return "Wrong table";
                    }
                    break;
                case "field":
                    var column = _this.schema.getColumn(expr.table, expr.column);
                    if (!column) {
                        return "Missing column";
                    }
                    // Validate expression
                    if (column.expr) {
                        // Use depth to prevent infinite recursion
                        error = _this.validateExprInternal(column.expr, lodash_1.default.extend({}, options, { depth: (options.depth || 0) + 1 }));
                        if (error) {
                            return error;
                        }
                    }
                    break;
                case "op":
                    // Validate exprs
                    for (var _i = 0, _a = expr.exprs; _i < _a.length; _i++) {
                        var subexpr = _a[_i];
                        // If op is aggregate, only allow non-aggregate
                        if (ExprUtils_1.default.isOpAggr(expr.op)) {
                            error = _this.validateExprInternal(subexpr, __assign(__assign({}, lodash_1.default.omit(options, "types", "enumValueIds", "idTable")), { aggrStatuses: ["literal", "individual"] }));
                        }
                        else {
                            error = _this.validateExprInternal(subexpr, lodash_1.default.omit(options, "types", "enumValueIds", "idTable"));
                        }
                        if (error) {
                            return error;
                        }
                    }
                    // Find op
                    var opItems = _this.exprUtils.findMatchingOpItems({ op: expr.op, lhsExpr: expr.exprs[0], resultTypes: options.types });
                    if (opItems.length === 0) {
                        return "No matching op";
                    }
                    break;
                case "scalar":
                    // Validate joins
                    if (!_this.exprUtils.areJoinsValid(expr.table, expr.joins)) {
                        return "Invalid joins";
                    }
                    var exprTable = _this.exprUtils.followJoins(expr.table, expr.joins);
                    // If joins are 1-n, allow aggrStatus of "aggregate"
                    if (_this.exprUtils.isMultipleJoins(expr.table, expr.joins)) {
                        error = _this.validateExprInternal(expr.expr, lodash_1.default.extend({}, options, { table: exprTable, aggrStatuses: ["literal", "aggregate"] }));
                    }
                    else {
                        error = _this.validateExprInternal(expr.expr, lodash_1.default.extend({}, options, { table: exprTable }));
                    }
                    if (error) {
                        return error;
                    }
                    break;
                case "case":
                    // Validate cases
                    for (var _b = 0, _c = expr.cases; _b < _c.length; _b++) {
                        var cse = _c[_b];
                        error = _this.validateExprInternal(cse.when, lodash_1.default.extend({}, options, { types: ["boolean"] }));
                        if (error) {
                            return error;
                        }
                        error = _this.validateExprInternal(cse.then, options);
                        if (error) {
                            return error;
                        }
                    }
                    error = _this.validateExprInternal(expr.else, options);
                    if (error) {
                        return error;
                    }
                    break;
                case "score":
                    error = _this.validateExprInternal(expr.input, lodash_1.default.extend({}, options, { types: ["enum", "enumset"] }));
                    if (error) {
                        return error;
                    }
                    if (expr.input) {
                        enumValueIds = lodash_1.default.pluck(_this.exprUtils.getExprEnumValues(expr.input) || [], "id");
                    }
                    else {
                        enumValueIds = null;
                    }
                    for (key in expr.scores) {
                        value = expr.scores[key];
                        if (enumValueIds && !enumValueIds.includes(key)) {
                            return "Invalid score enum";
                        }
                        error = _this.validateExprInternal(value, lodash_1.default.extend({}, options, { types: ["number"] }));
                        if (error) {
                            return error;
                        }
                    }
                    break;
                case "build enumset":
                    for (key in expr.values) {
                        value = expr.values[key];
                        if (options.enumValueIds && !options.enumValueIds.includes(key)) {
                            return "Invalid score enum";
                        }
                        error = _this.validateExprInternal(value, lodash_1.default.extend({}, options, { types: ["boolean"] }));
                        if (error) {
                            return error;
                        }
                    }
                    break;
                case "variable":
                    // Get variable
                    var variable = lodash_1.default.findWhere(_this.variables, { id: expr.variableId });
                    if (!variable) {
                        return "Missing variable " + expr.variableId;
                    }
                    break;
                case "extension":
                    var err = extensions_1.getExprExtension(expr.extension).validateExpr(expr, options, _this.schema, _this.variables);
                    if (err) {
                        return err;
                    }
            }
            // Validate table
            if (options.idTable && _this.exprUtils.getExprIdTable(expr) && (_this.exprUtils.getExprIdTable(expr) !== options.idTable)) {
                return "Wrong idTable";
            }
            // Validate type if present
            if (options.types && !options.types.includes(_this.exprUtils.getExprType(expr))) {
                return "Invalid type";
            }
            // Validate aggregate
            var aggrStatus = _this.exprUtils.getExprAggrStatus(expr);
            if (aggrStatuses && aggrStatus) {
                if (!aggrStatuses.includes(aggrStatus)) {
                    return "Invalid aggregation " + aggrStatus + " expected " + aggrStatuses.join(", ");
                }
            }
            // Validate enums
            if (options.enumValueIds && (_this.exprUtils.getExprType(expr) == "enum" || _this.exprUtils.getExprType(expr) == "enumset")) {
                if (lodash_1.default.difference(lodash_1.default.pluck(_this.exprUtils.getExprEnumValues(expr) || [], "id"), options.enumValueIds).length > 0) {
                    return "Invalid enum";
                }
            }
            return null;
        };
        this.schema = schema;
        this.variables = variables || [];
        this.exprUtils = new ExprUtils_1.default(schema, variables);
    }
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
    ExprValidator.prototype.validateExpr = function (expr, options) {
        var _this = this;
        options = options || {};
        if (!expr) {
            return null;
        }
        if (!this.schema) {
            return weakCache.cacheFunction([expr], [this.variables, options], function () {
                return _this.validateExprInternal(expr, options);
            });
        }
        return weakCache.cacheFunction([this.schema, expr], [this.variables, options], function () {
            return _this.validateExprInternal(expr, options);
        });
    };
    return ExprValidator;
}());
exports.default = ExprValidator;
