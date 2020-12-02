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
var ExprValidator_1 = __importDefault(require("./ExprValidator"));
var immer_1 = __importDefault(require("immer"));
// Cleans expressions. Cleaning means nulling invalid (not just incomplete) expressions if they cannot be auto-fixed.
var ExprCleaner = /** @class */ (function () {
    function ExprCleaner(schema) {
        this.schema = schema;
        this.exprUtils = new ExprUtils_1.default(schema);
    }
    // Clean an expression, returning null if completely invalid, otherwise removing
    // invalid parts. Attempts to correct invalid types by wrapping in other expressions.
    // e.g. if an enum is chosen when a boolean is required, it will be wrapped in "= any" op
    // options are:
    //   table: optional current table. expression must be related to this table or will be stripped
    //   types: optional types to limit to
    //   enumValueIds: ids of enum values that are valid if type is enum
    //   idTable: table that type of id must be from
    //   aggrStatuses: statuses of aggregation to allow. list of "individual", "literal", "aggregate". Default: ["individual", "literal"]
    ExprCleaner.prototype.cleanExpr = function (expr, options) {
        if (options === void 0) { options = {}; }
        var aggrStatuses = options.aggrStatuses || ["individual", "literal"];
        // Null handling
        if (!expr) {
            return null;
        }
        // Allow {} placeholder TODO is this still needed?
        if (lodash_1.default.isEmpty(expr)) {
            return expr;
        }
        // Handle upgrades from old version
        if (expr.type == "comparison")
            return this.cleanComparisonExpr(expr, __assign(__assign({}, options), { aggrStatuses: aggrStatuses }));
        if (expr.type == "logical")
            return this.cleanLogicalExpr(expr, __assign(__assign({}, options), { aggrStatuses: aggrStatuses }));
        if (expr.type == "count")
            return this.cleanCountExpr(expr, __assign(__assign({}, options), { aggrStatuses: aggrStatuses }));
        if (expr.type == "literal" && expr.valueType == "enum[]")
            expr = { type: "literal", valueType: "enumset", value: expr.value };
        // Strip if wrong table 
        if (expr.type != "literal") {
            if (options.table && expr.table && (expr.table !== options.table)) {
                return null;
            }
        }
        // Strip if no table
        if (expr.type == "field" && !expr.table) {
            return null;
        }
        // Strip if non-existent table
        if (expr.type != "literal" && expr.table && !this.schema.getTable(expr.table)) {
            return null;
        }
        // Fix old expression type
        if ((expr.type === "literal") && (expr.valueType === "enum[]")) {
            return { type: "literal", valueType: "enumset", value: expr.value };
        }
        // Handle infinite recursion that can occur when cleaning field expressions that self-reference
        var aggrStatus = null;
        try {
            aggrStatus = this.exprUtils.getExprAggrStatus(expr);
        }
        catch (ex) {
            if (ex.message === 'Infinite recursion') {
                return null;
            }
            throw ex;
        }
        // Default aggregation if needed and not aggregated
        if (expr.type != "literal" && expr.table) {
            if (aggrStatus === "individual" && !aggrStatuses.includes("individual") && aggrStatuses.includes("aggregate")) {
                var aggrOpItems = this.exprUtils.findMatchingOpItems({
                    resultTypes: options.types,
                    lhsExpr: expr,
                    aggr: true,
                    ordered: this.schema.getTable(expr.table).ordering != null
                });
                // If aggr is required and there is at least one possible, use it
                if (aggrOpItems.length > 0) {
                    expr = { type: "op", op: aggrOpItems[0].op, table: expr.table, exprs: [expr] };
                    aggrStatus = "aggregate";
                }
            }
        }
        // Default percent where + booleanization 
        if (expr.type != "literal" && expr.table) {
            if (aggrStatus == "individual" && !aggrStatuses.includes("individual") && aggrStatuses.includes("aggregate")) {
                // Only if result types include number
                if (!options.types || options.types.includes("number")) {
                    // Find op item that matches
                    var opItem = this.exprUtils.findMatchingOpItems({ resultTypes: ["boolean"], lhsExpr: expr })[0];
                    if (opItem) {
                        // Wrap in op to make it boolean
                        var asc = void 0, end = void 0;
                        expr = { type: "op", table: expr.table, op: opItem.op, exprs: [expr] };
                        // Determine number of arguments to append
                        var args = opItem.exprTypes.length - 1;
                        // Add extra nulls for other arguments
                        for (var i = 1; i <= args; i++) {
                            expr.exprs.push(null);
                        }
                        expr = { type: "op", op: "percent where", table: expr.table, exprs: [expr] };
                        aggrStatus = "aggregate";
                    }
                }
            }
        }
        // Strip if wrong aggregation status
        if (aggrStatus && !aggrStatuses.includes(aggrStatus)) {
            return null;
        }
        // Get type
        var type = this.exprUtils.getExprType(expr);
        // Boolean-ize for easy building of filters
        // True if a boolean expression is required
        var booleanOnly = options.types && (options.types.length === 1) && (options.types[0] === "boolean");
        // If boolean and expr is not boolean, wrap with appropriate expression
        if (booleanOnly && type && (type !== "boolean")) {
            // Find op item that matches
            var opItem = this.exprUtils.findMatchingOpItems({ resultTypes: ["boolean"], lhsExpr: expr })[0];
            if (opItem) {
                // Wrap in op to make it boolean
                if (expr.type != "literal") {
                    expr = { type: "op", table: expr.table, op: opItem.op, exprs: [expr] };
                }
                else {
                    expr = { type: "op", op: opItem.op, exprs: [expr] };
                }
                // Determine number of arguments to append
                var args = opItem.exprTypes.length - 1;
                // Add extra nulls for other arguments
                for (var i = 1; i <= args; i++) {
                    expr.exprs.push(null);
                }
            }
        }
        // Get type again in case changed  
        type = this.exprUtils.getExprType(expr);
        // Strip if wrong type
        if (type && options.types && !options.types.includes(type)) {
            // case statements should be preserved as they are a variable type and they will have their then clauses cleaned
            if (expr.type !== "case") {
                return null;
            }
        }
        var cleanOptions = __assign(__assign({}, options), { aggrStatuses: aggrStatuses });
        switch (expr.type) {
            case "field":
                return this.cleanFieldExpr(expr, cleanOptions);
            case "scalar":
                return this.cleanScalarExpr(expr, cleanOptions);
            case "op":
                return this.cleanOpExpr(expr, cleanOptions);
            case "literal":
                return this.cleanLiteralExpr(expr, cleanOptions);
            case "case":
                return this.cleanCaseExpr(expr, cleanOptions);
            case "id":
                return this.cleanIdExpr(expr, cleanOptions);
            case "score":
                return this.cleanScoreExpr(expr, cleanOptions);
            case "build enumset":
                return this.cleanBuildEnumsetExpr(expr, cleanOptions);
            case "variable":
                return this.cleanVariableExpr(expr, cleanOptions);
            // default:
            //   throw new Error(`Unknown expression type ${expr.type}`)
        }
    };
    /** Removes references to non-existent tables */
    ExprCleaner.prototype.cleanFieldExpr = function (expr, options) {
        // Empty expression
        if (!expr.column || !expr.table) {
            return null;
        }
        // Missing table
        if (!this.schema.getTable(expr.table)) {
            return null;
        }
        // Missing column
        var column = this.schema.getColumn(expr.table, expr.column);
        if (!column) {
            return null;
        }
        // Invalid expr
        if (column.expr) {
            if (new ExprValidator_1.default(this.schema).validateExpr(column.expr, options)) {
                return null;
            }
        }
        // Invalid enums
        if (options.enumValueIds && (column.type === "enum")) {
            if (lodash_1.default.difference(lodash_1.default.pluck(column.enumValues, "id"), options.enumValueIds).length > 0) {
                return null;
            }
        }
        if (options.enumValueIds && column.expr) {
            if (this.exprUtils.getExprType(column.expr) === "enum") {
                if (lodash_1.default.difference(lodash_1.default.pluck(this.exprUtils.getExprEnumValues(column.expr), "id"), options.enumValueIds).length > 0) {
                    return null;
                }
            }
        }
        return expr;
    };
    ExprCleaner.prototype.cleanOpExpr = function (expr, options) {
        var _this = this;
        switch (expr.op) {
            case "and":
            case "or":
                // Simplify
                if (expr.exprs.length === 1) {
                    return this.cleanExpr(expr.exprs[0], options);
                }
                if (expr.exprs.length === 0) {
                    return null;
                }
                return immer_1.default(expr, function (draft) {
                    for (var i = 0; i < expr.exprs.length; i++) {
                        draft.exprs[i] = _this.cleanExpr(expr.exprs[i], { types: ["boolean"], aggrStatuses: options.aggrStatuses, table: expr.table });
                    }
                });
            case "+":
            case "*":
                // Simplify
                if (expr.exprs.length === 1) {
                    return this.cleanExpr(expr.exprs[0], options);
                }
                if (expr.exprs.length === 0) {
                    return null;
                }
                return immer_1.default(expr, function (draft) {
                    for (var i = 0; i < expr.exprs.length; i++) {
                        draft.exprs[i] = _this.cleanExpr(expr.exprs[i], { types: ["number"], table: expr.table, aggrStatuses: options.aggrStatuses });
                    }
                });
            default:
                // Count always takes zero parameters and is valid if number type is valid
                if ((expr.op === "count") && (!options.types || options.types.includes("number")) && options.aggrStatuses.includes("aggregate")) {
                    if (expr.exprs.length == 0) {
                        return expr;
                    }
                    else {
                        return { type: "op", op: "count", table: expr.table, exprs: [] };
                    }
                }
                // Determine aggregate type of op
                var opIsAggr = ExprUtils_1.default.isOpAggr(expr.op);
                // Aggregate ops are never allowed if aggregates are not allowed
                if (opIsAggr && !options.aggrStatuses.includes("aggregate")) {
                    return null;
                }
                // Determine aggr setting. Prevent aggr for non-aggr output
                var aggr = undefined;
                if (!options.aggrStatuses.includes("aggregate") && options.aggrStatuses.includes("individual")) {
                    aggr = false;
                }
                // Determine innerAggrStatuses (same as outer, unless aggregate expression, in which case always aggregate)
                var innerAggrStatuses_1;
                if (opIsAggr) {
                    innerAggrStatuses_1 = ["literal", "individual"];
                }
                else {
                    innerAggrStatuses_1 = options.aggrStatuses;
                }
                // First do a loose cleaning of LHS to remove obviously invalid values
                var lhsExpr = this.cleanExpr(expr.exprs[0], { table: expr.table, aggrStatuses: innerAggrStatuses_1 });
                // Now attempt to clean it restricting to the types the op allows as lhs
                if (lhsExpr) {
                    var lhsTypes = lodash_1.default.uniq(lodash_1.default.compact(lodash_1.default.map(this.exprUtils.findMatchingOpItems({ op: expr.op }), function (opItem) { return opItem.exprTypes[0]; })));
                    lhsExpr = this.cleanExpr(expr.exprs[0], { table: expr.table, aggrStatuses: innerAggrStatuses_1, types: lhsTypes });
                    // If this nulls it, don't keep as we can switch ops to preseve it
                    if ((lhsExpr == null)) {
                        lhsExpr = this.cleanExpr(expr.exprs[0], { table: expr.table, aggrStatuses: innerAggrStatuses_1 });
                    }
                }
                // Need LHS for a normal op that is not a prefix. If it is a prefix op, allow the op to stand alone without params. Allow null type (ones being built out) to stand too
                if (!lhsExpr && !ExprUtils_1.default.isOpPrefix(expr.op)) {
                    return null;
                }
                // Get opItem
                var opItems = this.exprUtils.findMatchingOpItems({
                    op: expr.op,
                    lhsExpr: lhsExpr,
                    resultTypes: options.types,
                    aggr: aggr,
                    ordered: (expr.table && this.schema.getTable(expr.table) && this.schema.getTable(expr.table).ordering) != null
                });
                // If ambiguous, just clean subexprs and return
                if (opItems.length > 1) {
                    return immer_1.default(expr, function (draft) {
                        var _loop_1 = function (i) {
                            // Determine all possible types (union of all op items types)
                            var types = lodash_1.default.uniq(lodash_1.default.compact(lodash_1.default.flatten(lodash_1.default.map(opItems, function (opItem) { return opItem.exprTypes[i]; }))));
                            draft.exprs[i] = _this.cleanExpr(expr.exprs[i], { table: expr.table, aggrStatuses: innerAggrStatuses_1, types: (types.length > 0 ? types : undefined) });
                        };
                        for (var i = 0; i < expr.exprs.length; i++) {
                            _loop_1(i);
                        }
                    });
                }
                // If not found, default opItem
                var opItem_1 = opItems[0];
                if (!opItem_1) {
                    opItem_1 = this.exprUtils.findMatchingOpItems({ lhsExpr: lhsExpr, resultTypes: options.types, aggr: aggr })[0];
                    if (!opItem_1) {
                        return null;
                    }
                    expr = { type: "op", table: expr.table, op: opItem_1.op, exprs: [lhsExpr || null] };
                }
                return immer_1.default(expr, function (draft) {
                    // Pad or trim number of expressions
                    while (draft.exprs.length < opItem_1.exprTypes.length) {
                        draft.exprs.push(null);
                    }
                    if (expr.exprs.length > opItem_1.exprTypes.length) {
                        draft.exprs.splice(opItem_1.exprTypes.length, expr.exprs.length - opItem_1.exprTypes.length);
                    }
                    // Clean all sub expressions
                    var enumValueIds = undefined;
                    if (lhsExpr) {
                        var enumValues = _this.exprUtils.getExprEnumValues(lhsExpr);
                        if (enumValues) {
                            enumValueIds = lodash_1.default.pluck(enumValues, "id");
                        }
                    }
                    for (var i = 0; i < draft.exprs.length; i++) {
                        draft.exprs[i] = _this.cleanExpr(expr.exprs[i] || null, {
                            table: expr.table,
                            types: (opItem_1.exprTypes[i] ? [opItem_1.exprTypes[i]] : undefined),
                            enumValueIds: enumValueIds,
                            idTable: _this.exprUtils.getExprIdTable(expr.exprs[0]) || undefined,
                            aggrStatuses: innerAggrStatuses_1
                        });
                    }
                });
        }
    };
    // Strips/defaults invalid aggr and where of a scalar expression
    ExprCleaner.prototype.cleanScalarExpr = function (expr, options) {
        var _this = this;
        if (expr.joins.length === 0) {
            return this.cleanExpr(expr.expr, options);
        }
        // Fix legacy entity joins (accidentally had entities.<tablename>. prepended)
        var joins = lodash_1.default.map(expr.joins, function (j) {
            if (j.match(/^entities\.[a-z_0-9]+\./)) {
                return j.split(".")[2];
            }
            return j;
        });
        expr = lodash_1.default.extend({}, expr, { joins: joins });
        if (!this.exprUtils.areJoinsValid(expr.table, expr.joins)) {
            return null;
        }
        var innerTable = this.exprUtils.followJoins(expr.table, expr.joins);
        // LEGACY
        // Move aggr to inner expression
        if (expr.aggr) {
            expr = lodash_1.default.extend({}, lodash_1.default.omit(expr, "aggr"), { expr: { type: "op", table: innerTable, op: expr.aggr, exprs: [expr.expr] } });
        }
        // // Clean where
        // if (expr.where) {
        //   expr.where = this.cleanExpr(expr.where, {table: innerTable})
        // }
        // Simplify to join column
        if ((expr.joins.length === 1) && expr.expr && expr.expr.type === "id") {
            return { type: "field", table: expr.table, column: expr.joins[0] };
        }
        // Get inner expression type (must match unless is count which can count anything)
        if (expr.expr) {
            var isMultiple = this.exprUtils.isMultipleJoins(expr.table, expr.joins);
            var aggrStatuses_1 = isMultiple ? ["literal", "aggregate"] : ["literal", "individual"];
            return immer_1.default(expr, function (draft) {
                draft.expr = _this.cleanExpr(expr.expr, __assign(__assign({}, options), { table: innerTable, aggrStatuses: aggrStatuses_1 }));
            });
        }
        return expr;
    };
    ExprCleaner.prototype.cleanLiteralExpr = function (expr, options) {
        // Convert old types
        if (['decimal', 'integer'].includes(expr.valueType)) {
            expr = lodash_1.default.extend({}, expr, { valueType: "number" });
        }
        // TODO strip if no value?
        // Remove if enum type is wrong
        if ((expr.valueType === "enum") && options.enumValueIds && expr.value && !options.enumValueIds.includes(expr.value)) {
            return null;
        }
        // Remove invalid enum types
        if ((expr.valueType === "enumset") && options.enumValueIds && expr.value) {
            if (lodash_1.default.difference(expr.value, options.enumValueIds).length > 0) {
                return __assign(__assign({}, expr), { value: lodash_1.default.intersection(options.enumValueIds, expr.value) });
            }
        }
        // Null if wrong table
        if ((expr.valueType === "id") && options.idTable && (expr.idTable !== options.idTable)) {
            return null;
        }
        return expr;
    };
    ExprCleaner.prototype.cleanCaseExpr = function (expr, options) {
        var _this = this;
        // Simplify if no cases
        if (expr.cases.length === 0) {
            return expr.else || null;
        }
        // Clean whens as boolean
        return immer_1.default(expr, function (draft) {
            for (var c = 0; c < expr.cases.length; c++) {
                draft.cases[c].when = _this.cleanExpr(expr.cases[c].when, { types: ["boolean"], table: expr.table });
                draft.cases[c].then = _this.cleanExpr(expr.cases[c].then, options);
            }
            draft.else = _this.cleanExpr(expr.else, options);
        });
    };
    ExprCleaner.prototype.cleanIdExpr = function (expr, options) {
        // Null if wrong table
        if (options.idTable && (expr.table !== options.idTable)) {
            return null;
        }
        return expr;
    };
    ExprCleaner.prototype.cleanScoreExpr = function (expr, options) {
        var _this = this;
        return immer_1.default(expr, function (draft) {
            draft.input = _this.cleanExpr(expr.input, { types: ['enum', 'enumset'] });
            // Remove scores if no input
            if (!draft.input) {
                draft.scores = {};
                return;
            }
            var enumValues = _this.exprUtils.getExprEnumValues(expr.input);
            if (!enumValues) {
                draft.scores = {};
                return;
            }
            var enumValueIds = enumValues.map(function (ev) { return ev.id; });
            // Clean score values
            for (var key in expr.scores) {
                // Remove unknown enum values 
                if (!enumValueIds.includes(key)) {
                    delete draft.scores[key];
                }
                else {
                    draft.scores[key] = _this.cleanExpr(expr.scores[key], { table: expr.table, types: ['number'] });
                    if (!draft.scores[key]) {
                        delete draft.scores[key];
                    }
                }
            }
        });
    };
    ExprCleaner.prototype.cleanBuildEnumsetExpr = function (expr, options) {
        var _this = this;
        return immer_1.default(expr, function (draft) {
            var enumValueIds = options.enumValueIds;
            // Clean values
            for (var key in expr.values) {
                if (enumValueIds && !enumValueIds.includes(key)) {
                    delete draft.values[key];
                }
                else {
                    draft.values[key] = _this.cleanExpr(expr.values[key], { table: expr.table, types: ['boolean'] });
                    if (!draft.values[key]) {
                        delete draft.values[key];
                    }
                }
            }
        });
    };
    ExprCleaner.prototype.cleanVariableExpr = function (expr, options) {
        // Get variable
        var variable = this.schema.getVariable(expr.variableId);
        if (!variable) {
            return null;
        }
        // Check id table
        if (options.idTable && (variable.type === "id") && (variable.idTable !== options.idTable)) {
            return null;
        }
        return expr;
    };
    ExprCleaner.prototype.cleanComparisonExpr = function (expr, options) {
        var _this = this;
        // Upgrade to op
        var newExpr = { type: "op", table: expr.table, op: expr.op, exprs: [expr.lhs] };
        if (expr.rhs) {
            newExpr.exprs.push(expr.rhs);
        }
        // Clean sub-expressions to handle legacy literals
        newExpr.exprs = lodash_1.default.map(newExpr.exprs, function (e) { return _this.cleanExpr(e); });
        // If = true
        if (expr.op === "= true") {
            newExpr = expr.lhs;
        }
        if (expr.op === "= false") {
            newExpr = { type: "op", op: "not", table: expr.table, exprs: [expr.lhs] };
        }
        if ((expr.op === "between") && expr.rhs && (expr.rhs.type === "literal") && (expr.rhs.valueType === "daterange")) {
            newExpr.exprs = [expr.lhs, { type: "literal", valueType: "date", value: expr.rhs.value[0] }, { type: "literal", valueType: "date", value: expr.rhs.value[1] }];
        }
        if ((expr.op === "between") && expr.rhs && (expr.rhs.type === "literal") && (expr.rhs.valueType === "datetimerange")) {
            // If date, convert datetime to date
            if (this.exprUtils.getExprType(expr.lhs) === "date") {
                newExpr.exprs = [expr.lhs, { type: "literal", valueType: "date", value: expr.rhs.value[0].substr(0, 10) }, { type: "literal", valueType: "date", value: expr.rhs.value[1].substr(0, 10) }];
            }
            else {
                newExpr.exprs = [expr.lhs, { type: "literal", valueType: "datetime", value: expr.rhs.value[0] }, { type: "literal", valueType: "datetime", value: expr.rhs.value[1] }];
            }
        }
        return this.cleanExpr(newExpr, options);
    };
    ExprCleaner.prototype.cleanLogicalExpr = function (expr, options) {
        var newExpr = { type: "op", op: expr.op, table: expr.table, exprs: expr.exprs };
        return this.cleanExpr(newExpr, options);
    };
    ExprCleaner.prototype.cleanCountExpr = function (expr, options) {
        var newExpr = { type: "id", table: expr.table };
        return this.cleanExpr(newExpr, options);
    };
    return ExprCleaner;
}());
exports.default = ExprCleaner;
