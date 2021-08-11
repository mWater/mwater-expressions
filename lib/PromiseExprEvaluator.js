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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.PromiseExprEvaluator = void 0;
const lodash_1 = __importDefault(require("lodash"));
const ExprUtils_1 = __importDefault(require("./ExprUtils"));
const moment_1 = __importDefault(require("moment"));
const extensions_1 = require("./extensions");
/** Expression evaluator that is promise-based */
class PromiseExprEvaluator {
    /** variableValues are the expressions which the variable contains */
    constructor(options) {
        this.schema = options.schema;
        this.locale = options.locale;
        this.variables = options.variables;
        this.variableValues = options.variableValues;
    }
    /** Evaluate an expression given the context */
    evaluate(expr, context) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!expr) {
                return null;
            }
            switch (expr.type) {
                case "field":
                    // If schema is present and column is an expression column, use that
                    if (this.schema &&
                        this.schema.getColumn(expr.table, expr.column) &&
                        this.schema.getColumn(expr.table, expr.column).expr) {
                        return yield this.evaluate(this.schema.getColumn(expr.table, expr.column).expr, context);
                    }
                    if (!context.row) {
                        return null;
                    }
                    // Get field from row
                    const value = yield context.row.getField(expr.column);
                    return value;
                case "literal":
                    return expr.value;
                case "op":
                    return yield this.evaluateOp(expr.table, expr.op, expr.exprs, context);
                case "id":
                    if (!context.row) {
                        return null;
                    }
                    return context.row.getPrimaryKey();
                case "case":
                    return yield this.evaluateCase(expr, context);
                case "scalar":
                    return yield this.evaluateScalar(expr, context);
                case "score":
                    return yield this.evaluateScore(expr, context);
                case "build enumset":
                    return yield this.evaluateBuildEnumset(expr, context);
                case "variable":
                    return yield this.evaluateVariable(expr, context);
                case "extension":
                    return yield extensions_1.getExprExtension(expr.extension).evaluate(expr, context, this.schema, this.locale, this.variables, this.variableValues);
                default:
                    throw new Error(`Unsupported expression type ${expr.type}`);
            }
        });
    }
    /** Evaluate an expression synchronously */
    evaluateSync(expr) {
        if (!expr) {
            return null;
        }
        switch (expr.type) {
            case "literal":
                return expr.value;
            case "op":
                return this.evaluateOpValues(expr.op, expr.exprs, expr.exprs.map((e) => this.evaluateSync(e)));
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
                    throw new Error(`Synchronous table variables not supported`);
                }
                // Get variable
                const variable = lodash_1.default.findWhere(this.variables || [], {
                    id: expr.variableId
                });
                if (!variable) {
                    throw new Error(`Variable ${expr.variableId} not found`);
                }
                // Get value
                const value = this.variableValues[variable.id];
                if (value == null) {
                    return null;
                }
                // Evaluate variable
                return this.evaluateSync(value);
            case "extension":
                return extensions_1.getExprExtension(expr.extension).evaluateSync(expr, this.schema, this.locale, this.variables, this.variableValues);
            default:
                throw new Error(`Unsupported expression type ${expr.type}`);
        }
    }
    evaluateBuildEnumset(expr, context) {
        return __awaiter(this, void 0, void 0, function* () {
            // Evaluate each boolean
            const result = [];
            for (const key in expr.values) {
                const val = yield this.evaluate(expr.values[key], context);
                if (val) {
                    result.push(key);
                }
            }
            return result;
        });
    }
    evaluateScore(expr, context) {
        return __awaiter(this, void 0, void 0, function* () {
            // Get input value
            const input = yield this.evaluate(expr.input, context);
            if (!input) {
                return null;
            }
            if (lodash_1.default.isArray(input)) {
                let sum = 0;
                for (const inputVal of input) {
                    if (expr.scores[inputVal]) {
                        sum += yield this.evaluate(expr.scores[inputVal], context);
                    }
                }
                return sum;
            }
            else if (expr.scores[input]) {
                return yield this.evaluate(expr.scores[input], context);
            }
            else {
                return 0;
            }
        });
    }
    evaluateCase(expr, context) {
        return __awaiter(this, void 0, void 0, function* () {
            for (const cs of expr.cases) {
                const when = yield this.evaluate(cs.when, context);
                if (when) {
                    return yield this.evaluate(cs.then, context);
                }
            }
            return yield this.evaluate(expr.else, context);
        });
    }
    evaluateScalar(expr, context) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!context.row) {
                return null;
            }
            // Follow each join, either expanding into array if returns multiple, or single row if one row
            let state = context.row;
            for (const join of expr.joins) {
                // Null or [] is null
                if (!state || (lodash_1.default.isArray(state) && state.length == 0)) {
                    return null;
                }
                if (lodash_1.default.isArray(state)) {
                    // State is an array of rows. Follow joins and flatten to rows
                    const temp = yield Promise.all(state.map((st) => st.followJoin(join)));
                    state = lodash_1.default.compact(lodash_1.default.flattenDeep(temp));
                }
                else {
                    // State is a single row. Follow
                    state = yield state.followJoin(join);
                }
            }
            // Evaluate expression on new context
            if (lodash_1.default.isArray(state)) {
                return yield this.evaluate(expr.expr, { rows: state });
            }
            else {
                return yield this.evaluate(expr.expr, { row: state || undefined });
            }
        });
    }
    evaluateOp(table, op, exprs, context) {
        return __awaiter(this, void 0, void 0, function* () {
            // If aggregate op
            if (ExprUtils_1.default.isOpAggr(op)) {
                return this.evaluteAggrOp(table, op, exprs, context);
            }
            // is latest is special case for window-like function
            if (op == "is latest") {
                return yield this.evaluateIsLatest(table, exprs, context);
            }
            // Evaluate exprs
            const values = yield Promise.all(exprs.map((expr) => this.evaluate(expr, context)));
            return this.evaluateOpValues(op, exprs, values);
        });
    }
    /** NOTE: This is not technically correct. It's not a window function (as window
     * functions can't be used in where clauses) but rather a special query */
    evaluateIsLatest(table, exprs, context) {
        return __awaiter(this, void 0, void 0, function* () {
            // Fail quietly if no ordering or no schema
            if (!this.schema || !this.schema.getTable(table) || !this.schema.getTable(table).ordering) {
                console.warn("evaluateIsLatest does not work without schema and ordering");
                return false;
            }
            // Fail quietly if no rows
            if (!context.rows) {
                console.warn("evaluateIsLatest does not work without rows context");
                return false;
            }
            // Null if no row
            if (!context.row) {
                return null;
            }
            // Evaluate lhs (value to group by) for all rows
            const lhss = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[0], { row: row })));
            // Evaluate pk for all rows
            const pks = yield Promise.all(context.rows.map((row) => row.getPrimaryKey()));
            // Evaluate all rows by ordering
            const orderValues = yield Promise.all(context.rows.map((row) => row.getField(this.schema.getTable(table).ordering)));
            // Evaluate filter value for all rows if present
            const filters = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[0], { row: row })));
            let items = lodash_1.default.map(lhss, (lhs, index) => ({
                lhs: lhs,
                pk: pks[index],
                ordering: orderValues[index],
                filter: filters[index]
            }));
            // Filter
            if (exprs[1]) {
                items = lodash_1.default.filter(items, (item) => item.filter);
            }
            // Group by lhs
            const groups = lodash_1.default.groupBy(items, "lhs");
            // Keep latest of each group
            let latests = [];
            for (const lhs in groups) {
                const items = groups[lhs];
                latests.push(lodash_1.default.max(items, "ordering"));
            }
            // Get pk of row
            const pk = yield context.row.getPrimaryKey();
            // See if match
            return lodash_1.default.contains(lodash_1.default.pluck(latests, "pk"), pk);
        });
    }
    evaluteAggrOp(table, op, exprs, context) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!context.rows) {
                return null;
            }
            let values, orderValues, wheres, zipped, sum, ofs, count, items, value, index;
            switch (op) {
                case "count":
                    return context.rows.length;
                case "sum":
                    // Evaluate all rows
                    values = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[0], { row })));
                    return lodash_1.default.sum(values);
                case "avg":
                    // Evaluate all rows
                    values = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[0], { row })));
                    return lodash_1.default.sum(values) / values.length;
                // TODO. Uses window functions, so returning 100 for now
                case "percent":
                    return 100;
                case "min":
                    // Evaluate all rows
                    values = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[0], { row })));
                    return lodash_1.default.min(values);
                case "max":
                    // Evaluate all rows
                    values = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[0], { row })));
                    return lodash_1.default.max(values);
                case "last":
                    // Fail quietly if no ordering or no schema
                    if (!this.schema || !this.schema.getTable(table) || !this.schema.getTable(table).ordering) {
                        console.warn("last does not work without schema and ordering");
                        return null;
                    }
                    // Evaluate all rows by ordering
                    orderValues = yield Promise.all(context.rows.map((row) => row.getField(this.schema.getTable(table).ordering)));
                    // Evaluate all rows
                    values = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[0], { row })));
                    zipped = lodash_1.default.zip(values, orderValues);
                    // Sort by ordering reverse
                    zipped = lodash_1.default.sortByOrder(zipped, [(entry) => entry[1]], ["desc"]);
                    values = lodash_1.default.map(zipped, (entry) => entry[0]);
                    // Take first non-null
                    for (let i = 0; i < values.length; i++) {
                        if (values[i] != null) {
                            return values[i];
                        }
                    }
                    return null;
                case "last where":
                    // Fail quietly if no ordering or no schema
                    if (!this.schema || !this.schema.getTable(table) || !this.schema.getTable(table).ordering) {
                        console.warn("last where does not work without schema and ordering");
                        return null;
                    }
                    // Evaluate all rows by ordering
                    orderValues = yield Promise.all(context.rows.map((row) => row.getField(this.schema.getTable(table).ordering)));
                    // Evaluate all rows
                    values = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[0], { row })));
                    // Evaluate all rows by where
                    wheres = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[1], { row })));
                    // Find largest
                    if (orderValues.length == 0)
                        return null;
                    index = -1;
                    let largest = null;
                    for (let i = 0; i < context.rows.length; i++) {
                        if ((wheres[i] || !exprs[1]) && (index == -1 || orderValues[i] > largest) && values[i] != null) {
                            index = i;
                            largest = orderValues[i];
                        }
                    }
                    if (index >= 0) {
                        return values[index];
                    }
                    else {
                        return null;
                    }
                case "previous":
                    // Fail quietly if no ordering or no schema
                    if (!this.schema || !this.schema.getTable(table) || !this.schema.getTable(table).ordering) {
                        console.warn("last where does not work without schema and ordering");
                        return null;
                    }
                    // Evaluate all rows by ordering
                    orderValues = yield Promise.all(context.rows.map((row) => row.getField(this.schema.getTable(table).ordering)));
                    // Evaluate all rows
                    values = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[0], { row })));
                    zipped = lodash_1.default.zip(values, orderValues);
                    // Sort by ordering reverse
                    zipped = lodash_1.default.sortByOrder(zipped, [(entry) => entry[1]], ["desc"]);
                    values = lodash_1.default.map(zipped, (entry) => entry[0]);
                    // Take second non-null
                    values = lodash_1.default.filter(values, (v) => v != null);
                    if (values[1] != null) {
                        return values[1];
                    }
                    return null;
                case "first":
                    // Fail quietly if no ordering or no schema
                    if (!this.schema || !this.schema.getTable(table) || !this.schema.getTable(table).ordering) {
                        console.warn("first does not work without schema and ordering");
                        return null;
                    }
                    // Evaluate all rows by ordering
                    orderValues = yield Promise.all(context.rows.map((row) => row.getField(this.schema.getTable(table).ordering)));
                    // Evaluate all rows
                    values = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[0], { row })));
                    zipped = lodash_1.default.zip(values, orderValues);
                    // Sort by ordering asc
                    zipped = lodash_1.default.sortByOrder(zipped, [(entry) => entry[1]], ["asc"]);
                    values = lodash_1.default.map(zipped, (entry) => entry[0]);
                    // Take first non-null
                    for (let i = 0; i < values.length; i++) {
                        if (values[i] != null) {
                            return values[i];
                        }
                    }
                    return null;
                case "first where":
                    // Fail quietly if no ordering or no schema
                    if (!this.schema || !this.schema.getTable(table) || !this.schema.getTable(table).ordering) {
                        console.warn("first where does not work without schema and ordering");
                        return null;
                    }
                    // Evaluate all rows by ordering
                    orderValues = yield Promise.all(context.rows.map((row) => row.getField(this.schema.getTable(table).ordering)));
                    // Evaluate all rows
                    values = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[0], { row })));
                    // Evaluate all rows by where
                    wheres = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[1], { row })));
                    // Find smallest
                    if (orderValues.length == 0)
                        return null;
                    index = -1;
                    let smallest = null;
                    for (let i = 0; i < context.rows.length; i++) {
                        if ((wheres[i] || !exprs[1]) && (index == -1 || orderValues[i] < smallest) && values[i] != null) {
                            index = i;
                            smallest = orderValues[i];
                        }
                    }
                    if (index >= 0) {
                        return values[index];
                    }
                    else {
                        return null;
                    }
                case "count where":
                    // Evaluate all rows by where
                    wheres = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[0], { row })));
                    return wheres.filter((w) => w === true).length;
                case "sum where":
                    // Evaluate all rows
                    values = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[0], { row })));
                    // Evaluate all rows by where
                    wheres = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[1], { row })));
                    sum = 0;
                    for (let i = 0; i < context.rows.length; i++) {
                        if (wheres[i] === true) {
                            sum += values[i];
                        }
                    }
                    return sum;
                case "percent where":
                    // Evaluate all rows
                    wheres = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[0], { row })));
                    // Evaluate all rows by where
                    ofs = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[1], { row })));
                    sum = 0;
                    count = 0;
                    for (let i = 0; i < context.rows.length; i++) {
                        if (!exprs[1] || ofs[i] == true) {
                            count++;
                            if (wheres[i] === true) {
                                sum += 1;
                            }
                        }
                    }
                    if (count === 0) {
                        return null;
                    }
                    else {
                        return (sum / count) * 100;
                    }
                case "min where":
                    // Evaluate all rows
                    values = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[0], { row })));
                    // Evaluate all rows by where
                    wheres = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[1], { row })));
                    items = [];
                    for (let i = 0; i < context.rows.length; i++) {
                        if (wheres[i] === true) {
                            items.push(values[i]);
                        }
                    }
                    value = lodash_1.default.min(items);
                    return value != null ? value : null;
                case "max where":
                    // Evaluate all rows
                    values = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[0], { row })));
                    // Evaluate all rows by where
                    wheres = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[1], { row })));
                    items = [];
                    for (let i = 0; i < context.rows.length; i++) {
                        if (wheres[i] === true) {
                            items.push(values[i]);
                        }
                    }
                    value = lodash_1.default.max(items);
                    return value != null ? value : null;
                case "count distinct":
                    // Evaluate all rows
                    values = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[0], { row })));
                    return lodash_1.default.uniq(values).length;
                case "array_agg":
                    // Evaluate all rows
                    values = yield Promise.all(context.rows.map((row) => this.evaluate(exprs[0], { row })));
                    return values;
                default:
                    throw new Error(`Unknown op ${op}`);
            }
        });
    }
    /** Synchronously evaluate an op when the values are already known */
    evaluateOpValues(op, exprs, values) {
        let date, point, point1, point2, v0, v1;
        // Check if has null argument
        const hasNull = lodash_1.default.any(values, (v) => v == null);
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
                let least = null;
                for (const value of values) {
                    if (value != null && (least == null || value < least)) {
                        least = value;
                    }
                }
                return least;
            case "greatest":
                let greatest = null;
                for (const value of values) {
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
                return Math.floor((moment_1.default(values[0], moment_1.default.ISO_8601).date() - 1) / 7) + 1 + ""; // Make string
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
                return values[0].substr(0, 4) + "-" + moment_1.default(values[0].substr(0, 10), "YYYY-MM-DD").quarter();
            case "yearweek":
                if (hasNull) {
                    return null;
                }
                const isoWeek = moment_1.default(values[0].substr(0, 10), "YYYY-MM-DD").isoWeek();
                return values[0].substr(0, 4) + "-" + (isoWeek < 10 ? "0" + isoWeek : isoWeek);
            case "weekofyear":
                if (hasNull) {
                    return null;
                }
                const isoWeek2 = moment_1.default(values[0].substr(0, 10), "YYYY-MM-DD").isoWeek();
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
                return (moment_1.default(date, moment_1.default.ISO_8601).isSameOrBefore(moment_1.default()) &&
                    moment_1.default(date, moment_1.default.ISO_8601).isAfter(moment_1.default().subtract(24, "hours")));
            case "last7days":
                if (hasNull) {
                    return null;
                }
                date = values[0];
                return (moment_1.default(date, moment_1.default.ISO_8601).isBefore(moment_1.default().add(1, "days")) &&
                    moment_1.default(date, moment_1.default.ISO_8601).isAfter(moment_1.default().subtract(7, "days")));
            case "last30days":
                if (hasNull) {
                    return null;
                }
                date = values[0];
                return (moment_1.default(date, moment_1.default.ISO_8601).isBefore(moment_1.default().add(1, "days")) &&
                    moment_1.default(date, moment_1.default.ISO_8601).isAfter(moment_1.default().subtract(30, "days")));
            case "last365days":
                if (hasNull) {
                    return null;
                }
                date = values[0];
                return (moment_1.default(date, moment_1.default.ISO_8601).isBefore(moment_1.default().add(1, "days")) &&
                    moment_1.default(date, moment_1.default.ISO_8601).isAfter(moment_1.default().subtract(365, "days")));
            case "last12months":
                if (hasNull) {
                    return null;
                }
                date = values[0];
                return (moment_1.default(date, moment_1.default.ISO_8601).isBefore(moment_1.default().add(1, "days")) &&
                    moment_1.default(date, moment_1.default.ISO_8601).isAfter(moment_1.default().subtract(11, "months").startOf("month")));
            case "last6months":
                if (hasNull) {
                    return null;
                }
                date = values[0];
                return (moment_1.default(date, moment_1.default.ISO_8601).isBefore(moment_1.default().add(1, "days")) &&
                    moment_1.default(date, moment_1.default.ISO_8601).isAfter(moment_1.default().subtract(5, "months").startOf("month")));
            case "last3months":
                if (hasNull) {
                    return null;
                }
                date = values[0];
                return (moment_1.default(date, moment_1.default.ISO_8601).isBefore(moment_1.default().add(1, "days")) &&
                    moment_1.default(date, moment_1.default.ISO_8601).isAfter(moment_1.default().subtract(2, "months").startOf("month")));
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
                if ((point1 != null ? point1.type : void 0) === "Point" &&
                    (point2 != null ? point2.type : void 0) === "Point") {
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
                let total = 0;
                const coords = values[0].coordinates;
                for (let i = 0; i < coords.length - 1; i++) {
                    total += getDistanceFromLatLngInM(coords[i][1], coords[i][0], coords[i + 1][1], coords[i + 1][0]);
                }
                return total;
            case "to text":
                if (hasNull) {
                    return null;
                }
                if (this.schema) {
                    const exprUtils = new ExprUtils_1.default(this.schema);
                    const type = exprUtils.getExprType(exprs[0]);
                    return exprUtils.stringifyExprLiteral(exprs[0], values[0], this.locale);
                }
                else {
                    return values[0] + "";
                }
            case "to number":
                if (hasNull) {
                    return null;
                }
                if (lodash_1.default.isString(values[0]) && values[0].match(/^([0-9]+[.]?[0-9]*|[.][0-9]+)$/)) {
                    return parseFloat(values[0]);
                }
                return null;
            default:
                throw new Error(`Unknown op ${op}`);
        }
    }
    evaluateVariable(expr, context) {
        return __awaiter(this, void 0, void 0, function* () {
            // Get variable
            const variable = lodash_1.default.findWhere(this.variables || [], {
                id: expr.variableId
            });
            if (!variable) {
                throw new Error(`Variable ${expr.variableId} not found`);
            }
            // Get value
            const value = this.variableValues[variable.id];
            if (value == null) {
                return null;
            }
            // Evaluate
            return yield this.evaluate(value, context);
        });
    }
}
exports.PromiseExprEvaluator = PromiseExprEvaluator;
function getDistanceFromLatLngInM(lat1, lng1, lat2, lng2) {
    var R, a, c, d, dLat, dLng;
    R = 6370986; // Radius of the earth in m
    dLat = deg2rad(lat2 - lat1); // deg2rad below
    dLng = deg2rad(lng2 - lng1);
    a =
        Math.sin(dLat / 2) * Math.sin(dLat / 2) +
            Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
    c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    d = R * c; // Distance in m
    return d;
}
function deg2rad(deg) {
    return deg * (Math.PI / 180);
}
