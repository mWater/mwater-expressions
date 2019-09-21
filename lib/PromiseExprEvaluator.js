"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
var lodash_1 = __importDefault(require("lodash"));
/** Expression evaluator that is promise-based */
var PromiseExprEvaluator = /** @class */ (function () {
    function PromiseExprEvaluator(exprEvaluator) {
        this.exprEvaluator = exprEvaluator;
    }
    PromiseExprEvaluator.prototype.evaluate = function (expr, context) {
        var _this = this;
        var innerContext = {};
        var callbackifyRow = function (row) {
            return {
                getPrimaryKey: function (callback) {
                    row.getPrimaryKey().then(function (value) { return callback(null, value); }, function (error) { return callback(error); });
                },
                getField: function (columnId, callback) {
                    row.getField(columnId).then(function (value) {
                        // If value is row, callbackify
                        if (value && value.getPrimaryKey) {
                            value = callbackifyRow(value);
                        }
                        else if (lodash_1.default.isArray(value) && value.length > 0 && value[0].getPrimaryKey) {
                            value = value.map(function (r) { return callbackifyRow(r); });
                        }
                        callback(null, value);
                    }, function (error) { return callback(error); });
                }
            };
        };
        if (context.row) {
            innerContext.row = callbackifyRow(context.row);
        }
        if (context.rows) {
            innerContext.rows = context.rows.map(function (r) { return callbackifyRow(r); });
        }
        return new Promise(function (resolve, reject) {
            _this.exprEvaluator.evaluate(expr, innerContext, function (error, value) {
                if (error) {
                    reject(error);
                    return;
                }
                resolve(value);
            });
        });
    };
    return PromiseExprEvaluator;
}());
exports.PromiseExprEvaluator = PromiseExprEvaluator;
