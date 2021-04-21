"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.PromiseExprEvaluator = void 0;
var lodash_1 = __importDefault(require("lodash"));
var ExprUtils_1 = __importDefault(require("./ExprUtils"));
var moment_1 = __importDefault(require("moment"));
var extensions_1 = require("./extensions");
/** Expression evaluator that is promise-based */
var PromiseExprEvaluator = /** @class */ (function () {
    /** variableValues are the expressions which the variable contains */
    function PromiseExprEvaluator(options) {
        this.schema = options.schema;
        this.locale = options.locale;
        this.variables = options.variables;
        this.variableValues = options.variableValues;
    }
    /** Evaluate an expression given the context */
    PromiseExprEvaluator.prototype.evaluate = function (expr, context) {
        return __awaiter(this, void 0, void 0, function () {
            var _a, value;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        if (!expr) {
                            return [2 /*return*/, null];
                        }
                        _a = expr.type;
                        switch (_a) {
                            case "field": return [3 /*break*/, 1];
                            case "literal": return [3 /*break*/, 5];
                            case "op": return [3 /*break*/, 6];
                            case "id": return [3 /*break*/, 8];
                            case "case": return [3 /*break*/, 9];
                            case "scalar": return [3 /*break*/, 11];
                            case "score": return [3 /*break*/, 13];
                            case "build enumset": return [3 /*break*/, 15];
                            case "variable": return [3 /*break*/, 17];
                            case "extension": return [3 /*break*/, 19];
                        }
                        return [3 /*break*/, 21];
                    case 1:
                        if (!(this.schema && this.schema.getColumn(expr.table, expr.column) && this.schema.getColumn(expr.table, expr.column).expr)) return [3 /*break*/, 3];
                        return [4 /*yield*/, this.evaluate(this.schema.getColumn(expr.table, expr.column).expr, context)];
                    case 2: return [2 /*return*/, _b.sent()];
                    case 3:
                        if (!context.row) {
                            return [2 /*return*/, null];
                        }
                        return [4 /*yield*/, context.row.getField(expr.column)];
                    case 4:
                        value = _b.sent();
                        return [2 /*return*/, value];
                    case 5: return [2 /*return*/, expr.value];
                    case 6: return [4 /*yield*/, this.evaluateOp(expr.table, expr.op, expr.exprs, context)];
                    case 7: return [2 /*return*/, _b.sent()];
                    case 8:
                        if (!context.row) {
                            return [2 /*return*/, null];
                        }
                        return [2 /*return*/, context.row.getPrimaryKey()];
                    case 9: return [4 /*yield*/, this.evaluateCase(expr, context)];
                    case 10: return [2 /*return*/, _b.sent()];
                    case 11: return [4 /*yield*/, this.evaluateScalar(expr, context)];
                    case 12: return [2 /*return*/, _b.sent()];
                    case 13: return [4 /*yield*/, this.evaluateScore(expr, context)];
                    case 14: return [2 /*return*/, _b.sent()];
                    case 15: return [4 /*yield*/, this.evaluateBuildEnumset(expr, context)];
                    case 16: return [2 /*return*/, _b.sent()];
                    case 17: return [4 /*yield*/, this.evaluateVariable(expr, context)];
                    case 18: return [2 /*return*/, _b.sent()];
                    case 19: return [4 /*yield*/, extensions_1.getExprExtension(expr.extension).evaluate(expr, context, this.schema, this.locale, this.variables, this.variableValues)];
                    case 20: return [2 /*return*/, _b.sent()];
                    case 21: throw new Error("Unsupported expression type " + expr.type);
                }
            });
        });
    };
    /** Evaluate an expression synchronously */
    PromiseExprEvaluator.prototype.evaluateSync = function (expr) {
        var _this = this;
        if (!expr) {
            return null;
        }
        switch (expr.type) {
            case "literal":
                return expr.value;
            case "op":
                return this.evaluateOpValues(expr.op, expr.exprs, expr.exprs.map(function (e) { return _this.evaluateSync(e); }));
            case "case":
                // TODO
                throw new Error("Synchronous case not supported");
            case "score":
                // TODO
                throw new Error("Synchronous score not supported");
            case "build enumset":
                // TODO
                throw new Error("Synchronous build enumset not supported");
            case "variable":
                if (expr.table) {
                    throw new Error("Synchronous table variables not supported");
                }
                // Get variable
                var variable = lodash_1.default.findWhere(this.variables || [], {
                    id: expr.variableId
                });
                if (!variable) {
                    throw new Error("Variable " + expr.variableId + " not found");
                }
                // Get value
                var value = this.variableValues[variable.id];
                if (value == null) {
                    return null;
                }
                // Evaluate variable
                return this.evaluateSync(value);
            case "extension":
                return extensions_1.getExprExtension(expr.extension).evaluateSync(expr, this.schema, this.locale, this.variables, this.variableValues);
            default:
                throw new Error("Unsupported expression type " + expr.type);
        }
    };
    PromiseExprEvaluator.prototype.evaluateBuildEnumset = function (expr, context) {
        return __awaiter(this, void 0, void 0, function () {
            var result, _a, _b, _i, key, val;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0:
                        result = [];
                        _a = [];
                        for (_b in expr.values)
                            _a.push(_b);
                        _i = 0;
                        _c.label = 1;
                    case 1:
                        if (!(_i < _a.length)) return [3 /*break*/, 4];
                        key = _a[_i];
                        return [4 /*yield*/, this.evaluate(expr.values[key], context)];
                    case 2:
                        val = _c.sent();
                        if (val) {
                            result.push(key);
                        }
                        _c.label = 3;
                    case 3:
                        _i++;
                        return [3 /*break*/, 1];
                    case 4: return [2 /*return*/, result];
                }
            });
        });
    };
    PromiseExprEvaluator.prototype.evaluateScore = function (expr, context) {
        return __awaiter(this, void 0, void 0, function () {
            var input, sum, _i, input_1, inputVal, _a;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0: return [4 /*yield*/, this.evaluate(expr.input, context)];
                    case 1:
                        input = _b.sent();
                        if (!input) {
                            return [2 /*return*/, null];
                        }
                        if (!lodash_1.default.isArray(input)) return [3 /*break*/, 6];
                        sum = 0;
                        _i = 0, input_1 = input;
                        _b.label = 2;
                    case 2:
                        if (!(_i < input_1.length)) return [3 /*break*/, 5];
                        inputVal = input_1[_i];
                        if (!expr.scores[inputVal]) return [3 /*break*/, 4];
                        _a = sum;
                        return [4 /*yield*/, this.evaluate(expr.scores[inputVal], context)];
                    case 3:
                        sum = _a + _b.sent();
                        _b.label = 4;
                    case 4:
                        _i++;
                        return [3 /*break*/, 2];
                    case 5: return [2 /*return*/, sum];
                    case 6:
                        if (!expr.scores[input]) return [3 /*break*/, 8];
                        return [4 /*yield*/, this.evaluate(expr.scores[input], context)];
                    case 7: return [2 /*return*/, _b.sent()];
                    case 8: return [2 /*return*/, 0];
                }
            });
        });
    };
    PromiseExprEvaluator.prototype.evaluateCase = function (expr, context) {
        return __awaiter(this, void 0, void 0, function () {
            var _i, _a, cs, when;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        _i = 0, _a = expr.cases;
                        _b.label = 1;
                    case 1:
                        if (!(_i < _a.length)) return [3 /*break*/, 5];
                        cs = _a[_i];
                        return [4 /*yield*/, this.evaluate(cs.when, context)];
                    case 2:
                        when = _b.sent();
                        if (!when) return [3 /*break*/, 4];
                        return [4 /*yield*/, this.evaluate(cs.then, context)];
                    case 3: return [2 /*return*/, _b.sent()];
                    case 4:
                        _i++;
                        return [3 /*break*/, 1];
                    case 5: return [4 /*yield*/, this.evaluate(expr.else, context)];
                    case 6: return [2 /*return*/, _b.sent()];
                }
            });
        });
    };
    PromiseExprEvaluator.prototype.evaluateScalar = function (expr, context) {
        return __awaiter(this, void 0, void 0, function () {
            var state, _loop_1, _i, _a, join, state_1;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        if (!context.row) {
                            return [2 /*return*/, null];
                        }
                        state = context.row;
                        _loop_1 = function (join) {
                            var temp;
                            return __generator(this, function (_a) {
                                switch (_a.label) {
                                    case 0:
                                        // Null or [] is null
                                        if (!state || (lodash_1.default.isArray(state) && state.length == 0)) {
                                            return [2 /*return*/, { value: null }];
                                        }
                                        if (!lodash_1.default.isArray(state)) return [3 /*break*/, 2];
                                        return [4 /*yield*/, Promise.all(state.map(function (st) { return st.followJoin(join); }))];
                                    case 1:
                                        temp = _a.sent();
                                        state = lodash_1.default.compact(lodash_1.default.flattenDeep(temp));
                                        return [3 /*break*/, 4];
                                    case 2: return [4 /*yield*/, state.followJoin(join)];
                                    case 3:
                                        // State is a single row. Follow
                                        state = _a.sent();
                                        _a.label = 4;
                                    case 4: return [2 /*return*/];
                                }
                            });
                        };
                        _i = 0, _a = expr.joins;
                        _b.label = 1;
                    case 1:
                        if (!(_i < _a.length)) return [3 /*break*/, 4];
                        join = _a[_i];
                        return [5 /*yield**/, _loop_1(join)];
                    case 2:
                        state_1 = _b.sent();
                        if (typeof state_1 === "object")
                            return [2 /*return*/, state_1.value];
                        _b.label = 3;
                    case 3:
                        _i++;
                        return [3 /*break*/, 1];
                    case 4:
                        if (!lodash_1.default.isArray(state)) return [3 /*break*/, 6];
                        return [4 /*yield*/, this.evaluate(expr.expr, { rows: state })];
                    case 5: return [2 /*return*/, _b.sent()];
                    case 6: return [4 /*yield*/, this.evaluate(expr.expr, { row: state || undefined })];
                    case 7: return [2 /*return*/, _b.sent()];
                }
            });
        });
    };
    PromiseExprEvaluator.prototype.evaluateOp = function (table, op, exprs, context) {
        return __awaiter(this, void 0, void 0, function () {
            var values;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        // If aggregate op
                        if (ExprUtils_1.default.isOpAggr(op)) {
                            return [2 /*return*/, this.evaluteAggrOp(table, op, exprs, context)];
                        }
                        if (!(op == "is latest")) return [3 /*break*/, 2];
                        return [4 /*yield*/, this.evaluateIsLatest(table, exprs, context)];
                    case 1: return [2 /*return*/, _a.sent()];
                    case 2: return [4 /*yield*/, Promise.all(exprs.map(function (expr) { return _this.evaluate(expr, context); }))];
                    case 3:
                        values = _a.sent();
                        return [2 /*return*/, this.evaluateOpValues(op, exprs, values)];
                }
            });
        });
    };
    /** NOTE: This is not technically correct. It's not a window function (as window
     * functions can't be used in where clauses) but rather a special query */
    PromiseExprEvaluator.prototype.evaluateIsLatest = function (table, exprs, context) {
        return __awaiter(this, void 0, void 0, function () {
            var lhss, pks, orderValues, filters, items, groups, latests, lhs, items_1, pk;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        // Fail quietly if no ordering or no schema
                        if (!this.schema || !this.schema.getTable(table) || !this.schema.getTable(table).ordering) {
                            console.warn("evaluateIsLatest does not work without schema and ordering");
                            return [2 /*return*/, false];
                        }
                        // Fail quietly if no rows
                        if (!context.rows) {
                            console.warn("evaluateIsLatest does not work without rows context");
                            return [2 /*return*/, false];
                        }
                        // Null if no row
                        if (!context.row) {
                            return [2 /*return*/, null];
                        }
                        return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[0], { row: row }); }))
                            // Evaluate pk for all rows
                        ];
                    case 1:
                        lhss = _a.sent();
                        return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return row.getPrimaryKey(); }))
                            // Evaluate all rows by ordering
                        ];
                    case 2:
                        pks = _a.sent();
                        return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return row.getField(_this.schema.getTable(table).ordering); }))
                            // Evaluate filter value for all rows if present
                        ];
                    case 3:
                        orderValues = _a.sent();
                        return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[0], { row: row }); }))];
                    case 4:
                        filters = _a.sent();
                        items = lodash_1.default.map(lhss, function (lhs, index) { return ({ lhs: lhs, pk: pks[index], ordering: orderValues[index], filter: filters[index] }); });
                        // Filter
                        if (exprs[1]) {
                            items = lodash_1.default.filter(items, function (item) { return item.filter; });
                        }
                        groups = lodash_1.default.groupBy(items, "lhs");
                        latests = [];
                        for (lhs in groups) {
                            items_1 = groups[lhs];
                            latests.push(lodash_1.default.max(items_1, "ordering"));
                        }
                        return [4 /*yield*/, context.row.getPrimaryKey()
                            // See if match
                        ];
                    case 5:
                        pk = _a.sent();
                        // See if match
                        return [2 /*return*/, lodash_1.default.contains(lodash_1.default.pluck(latests, "pk"), pk)];
                }
            });
        });
    };
    PromiseExprEvaluator.prototype.evaluteAggrOp = function (table, op, exprs, context) {
        return __awaiter(this, void 0, void 0, function () {
            var values, orderValues, wheres, zipped, sum, ofs, count, items, value, _a, i, index, largest, i, i, smallest, i, i, i, i, i;
            var _this = this;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        if (!context.rows) {
                            return [2 /*return*/, null];
                        }
                        _a = op;
                        switch (_a) {
                            case "count": return [3 /*break*/, 1];
                            case "sum": return [3 /*break*/, 2];
                            case "avg": return [3 /*break*/, 4];
                            case "percent": return [3 /*break*/, 6];
                            case "min": return [3 /*break*/, 7];
                            case "max": return [3 /*break*/, 9];
                            case "last": return [3 /*break*/, 11];
                            case "last where": return [3 /*break*/, 14];
                            case "previous": return [3 /*break*/, 18];
                            case "first": return [3 /*break*/, 21];
                            case "first where": return [3 /*break*/, 24];
                            case "count where": return [3 /*break*/, 28];
                            case "sum where": return [3 /*break*/, 30];
                            case "percent where": return [3 /*break*/, 33];
                            case "min where": return [3 /*break*/, 36];
                            case "max where": return [3 /*break*/, 39];
                            case "count distinct": return [3 /*break*/, 42];
                            case "array_agg": return [3 /*break*/, 44];
                        }
                        return [3 /*break*/, 46];
                    case 1: return [2 /*return*/, context.rows.length];
                    case 2: return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[0], { row: row }); }))];
                    case 3:
                        // Evaluate all rows
                        values = _b.sent();
                        return [2 /*return*/, lodash_1.default.sum(values)];
                    case 4: return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[0], { row: row }); }))];
                    case 5:
                        // Evaluate all rows
                        values = _b.sent();
                        return [2 /*return*/, lodash_1.default.sum(values) / values.length
                            // TODO. Uses window functions, so returning 100 for now
                        ];
                    case 6: return [2 /*return*/, 100];
                    case 7: return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[0], { row: row }); }))];
                    case 8:
                        // Evaluate all rows
                        values = _b.sent();
                        return [2 /*return*/, lodash_1.default.min(values)];
                    case 9: return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[0], { row: row }); }))];
                    case 10:
                        // Evaluate all rows
                        values = _b.sent();
                        return [2 /*return*/, lodash_1.default.max(values)];
                    case 11:
                        // Fail quietly if no ordering or no schema
                        if (!this.schema || !this.schema.getTable(table) || !this.schema.getTable(table).ordering) {
                            console.warn("last does not work without schema and ordering");
                            return [2 /*return*/, null];
                        }
                        return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return row.getField(_this.schema.getTable(table).ordering); }))
                            // Evaluate all rows
                        ];
                    case 12:
                        // Evaluate all rows by ordering
                        orderValues = _b.sent();
                        return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[0], { row: row }); }))];
                    case 13:
                        // Evaluate all rows
                        values = _b.sent();
                        zipped = lodash_1.default.zip(values, orderValues);
                        // Sort by ordering reverse
                        zipped = lodash_1.default.sortByOrder(zipped, [function (entry) { return entry[1]; }], ["desc"]);
                        values = lodash_1.default.map(zipped, function (entry) { return entry[0]; });
                        // Take first non-null
                        for (i = 0; i < values.length; i++) {
                            if (values[i] != null) {
                                return [2 /*return*/, values[i]];
                            }
                        }
                        return [2 /*return*/, null];
                    case 14:
                        // Fail quietly if no ordering or no schema
                        if (!this.schema || !this.schema.getTable(table) || !this.schema.getTable(table).ordering) {
                            console.warn("last where does not work without schema and ordering");
                            return [2 /*return*/, null];
                        }
                        return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return row.getField(_this.schema.getTable(table).ordering); }))
                            // Evaluate all rows
                        ];
                    case 15:
                        // Evaluate all rows by ordering
                        orderValues = _b.sent();
                        return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[0], { row: row }); }))
                            // Evaluate all rows by where
                        ];
                    case 16:
                        // Evaluate all rows
                        values = _b.sent();
                        return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[1], { row: row }); }))
                            // Find largest
                        ];
                    case 17:
                        // Evaluate all rows by where
                        wheres = _b.sent();
                        // Find largest
                        if (orderValues.length == 0)
                            return [2 /*return*/, null];
                        index = -1;
                        largest = null;
                        for (i = 0; i < context.rows.length; i++) {
                            if ((wheres[i] || !exprs[1]) && (index == -1 || orderValues[i] > largest) && values[i] != null) {
                                index = i;
                                largest = orderValues[i];
                            }
                        }
                        if (index >= 0) {
                            return [2 /*return*/, values[index]];
                        }
                        else {
                            return [2 /*return*/, null];
                        }
                        _b.label = 18;
                    case 18:
                        // Fail quietly if no ordering or no schema
                        if (!this.schema || !this.schema.getTable(table) || !this.schema.getTable(table).ordering) {
                            console.warn("last where does not work without schema and ordering");
                            return [2 /*return*/, null];
                        }
                        return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return row.getField(_this.schema.getTable(table).ordering); }))
                            // Evaluate all rows
                        ];
                    case 19:
                        // Evaluate all rows by ordering
                        orderValues = _b.sent();
                        return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[0], { row: row }); }))];
                    case 20:
                        // Evaluate all rows
                        values = _b.sent();
                        zipped = lodash_1.default.zip(values, orderValues);
                        // Sort by ordering reverse
                        zipped = lodash_1.default.sortByOrder(zipped, [function (entry) { return entry[1]; }], ["desc"]);
                        values = lodash_1.default.map(zipped, function (entry) { return entry[0]; });
                        // Take second non-null
                        values = lodash_1.default.filter(values, function (v) { return v != null; });
                        if (values[1] != null) {
                            return [2 /*return*/, values[1]];
                        }
                        return [2 /*return*/, null];
                    case 21:
                        // Fail quietly if no ordering or no schema
                        if (!this.schema || !this.schema.getTable(table) || !this.schema.getTable(table).ordering) {
                            console.warn("first does not work without schema and ordering");
                            return [2 /*return*/, null];
                        }
                        return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return row.getField(_this.schema.getTable(table).ordering); }))
                            // Evaluate all rows
                        ];
                    case 22:
                        // Evaluate all rows by ordering
                        orderValues = _b.sent();
                        return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[0], { row: row }); }))];
                    case 23:
                        // Evaluate all rows
                        values = _b.sent();
                        zipped = lodash_1.default.zip(values, orderValues);
                        // Sort by ordering asc
                        zipped = lodash_1.default.sortByOrder(zipped, [function (entry) { return entry[1]; }], ["asc"]);
                        values = lodash_1.default.map(zipped, function (entry) { return entry[0]; });
                        // Take first non-null
                        for (i = 0; i < values.length; i++) {
                            if (values[i] != null) {
                                return [2 /*return*/, values[i]];
                            }
                        }
                        return [2 /*return*/, null];
                    case 24:
                        // Fail quietly if no ordering or no schema
                        if (!this.schema || !this.schema.getTable(table) || !this.schema.getTable(table).ordering) {
                            console.warn("first where does not work without schema and ordering");
                            return [2 /*return*/, null];
                        }
                        return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return row.getField(_this.schema.getTable(table).ordering); }))
                            // Evaluate all rows
                        ];
                    case 25:
                        // Evaluate all rows by ordering
                        orderValues = _b.sent();
                        return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[0], { row: row }); }))
                            // Evaluate all rows by where
                        ];
                    case 26:
                        // Evaluate all rows
                        values = _b.sent();
                        return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[1], { row: row }); }))
                            // Find smallest
                        ];
                    case 27:
                        // Evaluate all rows by where
                        wheres = _b.sent();
                        // Find smallest
                        if (orderValues.length == 0)
                            return [2 /*return*/, null];
                        index = -1;
                        smallest = null;
                        for (i = 0; i < context.rows.length; i++) {
                            if ((wheres[i] || !exprs[1]) && (index == -1 || orderValues[i] < smallest) && values[i] != null) {
                                index = i;
                                smallest = orderValues[i];
                            }
                        }
                        if (index >= 0) {
                            return [2 /*return*/, values[index]];
                        }
                        else {
                            return [2 /*return*/, null];
                        }
                        _b.label = 28;
                    case 28: return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[0], { row: row }); }))];
                    case 29:
                        // Evaluate all rows by where
                        wheres = _b.sent();
                        return [2 /*return*/, wheres.filter(function (w) { return w === true; }).length];
                    case 30: return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[0], { row: row }); }))
                        // Evaluate all rows by where
                    ];
                    case 31:
                        // Evaluate all rows
                        values = _b.sent();
                        return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[1], { row: row }); }))];
                    case 32:
                        // Evaluate all rows by where
                        wheres = _b.sent();
                        sum = 0;
                        for (i = 0; i < context.rows.length; i++) {
                            if (wheres[i] === true) {
                                sum += values[i];
                            }
                        }
                        return [2 /*return*/, sum];
                    case 33: return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[0], { row: row }); }))
                        // Evaluate all rows by where
                    ];
                    case 34:
                        // Evaluate all rows
                        wheres = _b.sent();
                        return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[1], { row: row }); }))];
                    case 35:
                        // Evaluate all rows by where
                        ofs = _b.sent();
                        sum = 0;
                        count = 0;
                        for (i = 0; i < context.rows.length; i++) {
                            if (!exprs[1] || ofs[i] == true) {
                                count++;
                                if (wheres[i] === true) {
                                    sum += 1;
                                }
                            }
                        }
                        if (count === 0) {
                            return [2 /*return*/, null];
                        }
                        else {
                            return [2 /*return*/, sum / count * 100];
                        }
                        _b.label = 36;
                    case 36: return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[0], { row: row }); }))
                        // Evaluate all rows by where
                    ];
                    case 37:
                        // Evaluate all rows
                        values = _b.sent();
                        return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[1], { row: row }); }))];
                    case 38:
                        // Evaluate all rows by where
                        wheres = _b.sent();
                        items = [];
                        for (i = 0; i < context.rows.length; i++) {
                            if (wheres[i] === true) {
                                items.push(values[i]);
                            }
                        }
                        value = lodash_1.default.min(items);
                        return [2 /*return*/, value != null ? value : null];
                    case 39: return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[0], { row: row }); }))
                        // Evaluate all rows by where
                    ];
                    case 40:
                        // Evaluate all rows
                        values = _b.sent();
                        return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[1], { row: row }); }))];
                    case 41:
                        // Evaluate all rows by where
                        wheres = _b.sent();
                        items = [];
                        for (i = 0; i < context.rows.length; i++) {
                            if (wheres[i] === true) {
                                items.push(values[i]);
                            }
                        }
                        value = lodash_1.default.max(items);
                        return [2 /*return*/, value != null ? value : null];
                    case 42: return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[0], { row: row }); }))];
                    case 43:
                        // Evaluate all rows
                        values = _b.sent();
                        return [2 /*return*/, lodash_1.default.uniq(values).length];
                    case 44: return [4 /*yield*/, Promise.all(context.rows.map(function (row) { return _this.evaluate(exprs[0], { row: row }); }))];
                    case 45:
                        // Evaluate all rows
                        values = _b.sent();
                        return [2 /*return*/, values];
                    case 46: throw new Error("Unknown op " + op);
                }
            });
        });
    };
    /** Synchronously evaluate an op when the values are already known */
    PromiseExprEvaluator.prototype.evaluateOpValues = function (op, exprs, values) {
        var date, point, point1, point2, v0, v1;
        // Check if has null argument
        var hasNull = lodash_1.default.any(values, function (v) { return v == null; });
        switch (op) {
            case "+":
                return lodash_1.default.reduce(values, function (acc, value) {
                    return acc + (value != null ? value : 0);
                });
            case "*":
                if (hasNull) {
                    return null;
                }
                return lodash_1.default.reduce(values, function (acc, value) {
                    return acc * value;
                });
            case "-":
                if (hasNull) {
                    return null;
                }
                return values[0] - values[1];
            case "/":
                if (hasNull) {
                    return null;
                }
                if (values[1] === 0) {
                    return null;
                }
                return values[0] / values[1];
            case "and":
                if (values.length === 0) {
                    return null;
                }
                return lodash_1.default.reduce(values, function (acc, value) {
                    return acc && value;
                });
            case "or":
                if (values.length === 0) {
                    return null;
                }
                return lodash_1.default.reduce(values, function (acc, value) {
                    return acc || value;
                });
            case "not":
                if (hasNull) {
                    return true;
                }
                return !values[0];
            case "=":
                if (hasNull) {
                    return null;
                }
                return values[0] === values[1];
            case "<>":
                if (hasNull) {
                    return null;
                }
                return values[0] !== values[1];
            case ">":
                if (hasNull) {
                    return null;
                }
                return values[0] > values[1];
            case ">=":
                if (hasNull) {
                    return null;
                }
                return values[0] >= values[1];
            case "<":
                if (hasNull) {
                    return null;
                }
                return values[0] < values[1];
            case "<=":
                if (hasNull) {
                    return null;
                }
                return values[0] <= values[1];
            case "= false":
                if (hasNull) {
                    return null;
                }
                return values[0] === false;
            case "is null":
                return values[0] == null;
            case "is not null":
                return values[0] != null;
            case "~*":
                if (hasNull) {
                    return null;
                }
                return values[0].match(new RegExp(values[1], "i")) != null;
            case "= any":
                if (hasNull) {
                    return null;
                }
                return lodash_1.default.contains(values[1], values[0]);
            case "contains":
                if (hasNull) {
                    return null;
                }
                return lodash_1.default.difference(values[1], values[0]).length === 0;
            case "intersects":
                if (hasNull) {
                    return null;
                }
                return lodash_1.default.intersection(values[0], values[1]).length > 0;
            case "includes":
                if (hasNull) {
                    return null;
                }
                return lodash_1.default.includes(values[0], values[1]);
            case "length":
                if (hasNull) {
                    return 0;
                }
                return values[0].length;
            case "between":
                if (hasNull) {
                    return null;
                }
                return values[0] >= values[1] && values[0] <= values[2];
            case "round":
                if (hasNull) {
                    return null;
                }
                return Math.round(values[0]);
            case "floor":
                if (hasNull) {
                    return null;
                }
                return Math.floor(values[0]);
            case "ceiling":
                if (hasNull) {
                    return null;
                }
                return Math.ceil(values[0]);
            case "least":
                var least = null;
                for (var _i = 0, values_1 = values; _i < values_1.length; _i++) {
                    var value = values_1[_i];
                    if (value != null && (least == null || value < least)) {
                        least = value;
                    }
                }
                return least;
            case "greatest":
                var greatest = null;
                for (var _a = 0, values_2 = values; _a < values_2.length; _a++) {
                    var value = values_2[_a];
                    if (value != null && (greatest == null || value > greatest)) {
                        greatest = value;
                    }
                }
                return greatest;
            case "days difference":
                if (hasNull) {
                    return null;
                }
                // Pad to datetime (to allow date/datetime comparisons)
                v0 = values[0].length == 10 ? values[0] + "T00:00:00Z" : values[0];
                v1 = values[1].length == 10 ? values[1] + "T00:00:00Z" : values[1];
                return moment_1.default(v0, moment_1.default.ISO_8601).diff(moment_1.default(v1, moment_1.default.ISO_8601)) / 24 / 3600 / 1000;
            case "months difference":
                if (hasNull) {
                    return null;
                }
                // Pad to datetime (to allow date/datetime comparisons)
                v0 = values[0].length == 10 ? values[0] + "T00:00:00Z" : values[0];
                v1 = values[1].length == 10 ? values[1] + "T00:00:00Z" : values[1];
                return moment_1.default(v0, moment_1.default.ISO_8601).diff(moment_1.default(v1, moment_1.default.ISO_8601)) / 24 / 3600 / 1000 / 30.5;
            case "years difference":
                if (hasNull) {
                    return null;
                }
                // Pad to datetime (to allow date/datetime comparisons)
                v0 = values[0].length == 10 ? values[0] + "T00:00:00Z" : values[0];
                v1 = values[1].length == 10 ? values[1] + "T00:00:00Z" : values[1];
                return moment_1.default(v0, moment_1.default.ISO_8601).diff(moment_1.default(v1, moment_1.default.ISO_8601)) / 24 / 3600 / 1000 / 365;
            case "days since":
                if (hasNull) {
                    return null;
                }
                return moment_1.default().diff(moment_1.default(values[0], moment_1.default.ISO_8601)) / 24 / 3600 / 1000;
            case "weekofmonth":
                if (hasNull) {
                    return null;
                }
                return (Math.floor((moment_1.default(values[0], moment_1.default.ISO_8601).date() - 1) / 7) + 1) + ""; // Make string
            case "dayofmonth":
                if (hasNull) {
                    return null;
                }
                return moment_1.default(values[0], moment_1.default.ISO_8601).format("DD");
            case "month":
                if (hasNull) {
                    return null;
                }
                return values[0].substr(5, 2);
            case "yearmonth":
                if (hasNull) {
                    return null;
                }
                return values[0].substr(0, 7) + "-01";
            case "yearquarter":
                if (hasNull) {
                    return null;
                }
                return values[0].substr(0, 4) + "-" + moment_1.default(values[0].substr(0, 10), 'YYYY-MM-DD').quarter();
            case "yearweek":
                if (hasNull) {
                    return null;
                }
                var isoWeek = moment_1.default(values[0].substr(0, 10), 'YYYY-MM-DD').isoWeek();
                return values[0].substr(0, 4) + "-" + (isoWeek < 10 ? "0" + isoWeek : isoWeek);
            case "weekofyear":
                if (hasNull) {
                    return null;
                }
                var isoWeek2 = moment_1.default(values[0].substr(0, 10), 'YYYY-MM-DD').isoWeek();
                return isoWeek2 < 10 ? "0" + isoWeek2 : isoWeek2;
            case "to date":
                if (hasNull) {
                    return null;
                }
                return values[0].substr(0, 10);
            case "year":
                if (hasNull) {
                    return null;
                }
                return values[0].substr(0, 4) + "-01-01";
            case "today":
                if (hasNull) {
                    return null;
                }
                date = values[0];
                return moment_1.default(date, moment_1.default.ISO_8601).format("YYYY-MM-DD") === moment_1.default().format("YYYY-MM-DD");
            case "yesterday":
                if (hasNull) {
                    return null;
                }
                date = values[0];
                return moment_1.default(date, moment_1.default.ISO_8601).add(1, "days").format("YYYY-MM-DD") === moment_1.default().format("YYYY-MM-DD");
            case "thismonth":
                if (hasNull) {
                    return null;
                }
                date = values[0];
                return moment_1.default(date, moment_1.default.ISO_8601).format("YYYY-MM") === moment_1.default().format("YYYY-MM");
            case "lastmonth":
                if (hasNull) {
                    return null;
                }
                date = values[0];
                return moment_1.default(date, moment_1.default.ISO_8601).add(1, "months").format("YYYY-MM") === moment_1.default().format("YYYY-MM");
            case "thisyear":
                if (hasNull) {
                    return null;
                }
                date = values[0];
                return moment_1.default(date, moment_1.default.ISO_8601).format("YYYY") === moment_1.default().format("YYYY");
            case "lastyear":
                if (hasNull) {
                    return null;
                }
                date = values[0];
                return moment_1.default(date, moment_1.default.ISO_8601).add(1, "years").format("YYYY") === moment_1.default().format("YYYY");
            case "last24hours":
                if (hasNull) {
                    return null;
                }
                date = values[0];
                return moment_1.default(date, moment_1.default.ISO_8601).isSameOrBefore(moment_1.default()) && moment_1.default(date, moment_1.default.ISO_8601).isAfter(moment_1.default().subtract(24, "hours"));
            case "last7days":
                if (hasNull) {
                    return null;
                }
                date = values[0];
                return moment_1.default(date, moment_1.default.ISO_8601).isBefore(moment_1.default().add(1, "days")) && moment_1.default(date, moment_1.default.ISO_8601).isAfter(moment_1.default().subtract(7, "days"));
            case "last30days":
                if (hasNull) {
                    return null;
                }
                date = values[0];
                return moment_1.default(date, moment_1.default.ISO_8601).isBefore(moment_1.default().add(1, "days")) && moment_1.default(date, moment_1.default.ISO_8601).isAfter(moment_1.default().subtract(30, "days"));
            case "last365days":
                if (hasNull) {
                    return null;
                }
                date = values[0];
                return moment_1.default(date, moment_1.default.ISO_8601).isBefore(moment_1.default().add(1, "days")) && moment_1.default(date, moment_1.default.ISO_8601).isAfter(moment_1.default().subtract(365, "days"));
            case "last12months":
                if (hasNull) {
                    return null;
                }
                date = values[0];
                return moment_1.default(date, moment_1.default.ISO_8601).isBefore(moment_1.default().add(1, "days")) && moment_1.default(date, moment_1.default.ISO_8601).isAfter(moment_1.default().subtract(11, "months").startOf('month'));
            case "last6months":
                if (hasNull) {
                    return null;
                }
                date = values[0];
                return moment_1.default(date, moment_1.default.ISO_8601).isBefore(moment_1.default().add(1, "days")) && moment_1.default(date, moment_1.default.ISO_8601).isAfter(moment_1.default().subtract(5, "months").startOf('month'));
            case "last3months":
                if (hasNull) {
                    return null;
                }
                date = values[0];
                return moment_1.default(date, moment_1.default.ISO_8601).isBefore(moment_1.default().add(1, "days")) && moment_1.default(date, moment_1.default.ISO_8601).isAfter(moment_1.default().subtract(2, "months").startOf('month'));
            case "future":
                if (hasNull) {
                    return null;
                }
                date = values[0];
                return moment_1.default(date, moment_1.default.ISO_8601).isAfter(moment_1.default());
            case "notfuture":
                if (hasNull) {
                    return null;
                }
                date = values[0];
                return !moment_1.default(date, moment_1.default.ISO_8601).isAfter(moment_1.default());
            case "current date":
                return moment_1.default().format("YYYY-MM-DD");
            case "current datetime":
                return moment_1.default().toISOString();
            case "latitude":
                if (hasNull) {
                    return null;
                }
                point = values[0];
                if ((point != null ? point.type : void 0) === "Point") {
                    return point.coordinates[1];
                }
                break;
            case "longitude":
                if (hasNull) {
                    return null;
                }
                point = values[0];
                if ((point != null ? point.type : void 0) === "Point") {
                    return point.coordinates[0];
                }
                break;
            case "distance":
                if (hasNull) {
                    return null;
                }
                point1 = values[0];
                point2 = values[1];
                if ((point1 != null ? point1.type : void 0) === "Point" && (point2 != null ? point2.type : void 0) === "Point") {
                    return getDistanceFromLatLngInM(point1.coordinates[1], point1.coordinates[0], point2.coordinates[1], point2.coordinates[0]);
                }
                break;
            case "line length":
                if (hasNull) {
                    return null;
                }
                if (values[0].type !== "LineString") {
                    return 0;
                }
                var total = 0;
                var coords = values[0].coordinates;
                for (var i = 0; i < coords.length - 1; i++) {
                    total += getDistanceFromLatLngInM(coords[i][1], coords[i][0], coords[i + 1][1], coords[i + 1][0]);
                }
                return total;
            case "to text":
                if (hasNull) {
                    return null;
                }
                if (this.schema) {
                    var exprUtils = new ExprUtils_1.default(this.schema);
                    return exprUtils.stringifyExprLiteral(exprs[0], values[0], this.locale);
                }
                else {
                    return values[0] + "";
                }
            default:
                throw new Error("Unknown op " + op);
        }
    };
    PromiseExprEvaluator.prototype.evaluateVariable = function (expr, context) {
        return __awaiter(this, void 0, void 0, function () {
            var variable, value;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        variable = lodash_1.default.findWhere(this.variables || [], {
                            id: expr.variableId
                        });
                        if (!variable) {
                            throw new Error("Variable " + expr.variableId + " not found");
                        }
                        value = this.variableValues[variable.id];
                        if (value == null) {
                            return [2 /*return*/, null];
                        }
                        return [4 /*yield*/, this.evaluate(value, context)];
                    case 1: 
                    // Evaluate
                    return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    return PromiseExprEvaluator;
}());
exports.PromiseExprEvaluator = PromiseExprEvaluator;
function getDistanceFromLatLngInM(lat1, lng1, lat2, lng2) {
    var R, a, c, d, dLat, dLng;
    R = 6370986; // Radius of the earth in m
    dLat = deg2rad(lat2 - lat1); // deg2rad below
    dLng = deg2rad(lng2 - lng1);
    a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
    c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    d = R * c; // Distance in m
    return d;
}
function deg2rad(deg) {
    return deg * (Math.PI / 180);
}
