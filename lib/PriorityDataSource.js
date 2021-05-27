"use strict";
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (Object.prototype.hasOwnProperty.call(b, p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
var DataSource_1 = __importDefault(require("./DataSource"));
// Forwards performQuery call to the PriorityDataQueue that will forward them to the DataSource
var PriorityDataSource = /** @class */ (function (_super) {
    __extends(PriorityDataSource, _super);
    function PriorityDataSource(priorityDataQueue, priority) {
        var _this = _super.call(this) || this;
        _this.priorityDataQueue = priorityDataQueue;
        _this.priority = priority;
        return _this;
    }
    PriorityDataSource.prototype.performQuery = function (query, cb) {
        var _this = this;
        if (cb) {
            this.priorityDataQueue.performQuery(query, cb, this.priority);
            return;
        }
        return new Promise(function (resolve, reject) {
            _this.priorityDataQueue.performQuery(query, function (err, results) {
                if (err) {
                    reject(err);
                }
                else {
                    resolve(results);
                }
            }, _this.priority);
        });
    };
    PriorityDataSource.prototype.getImageUrl = function (imageId, height) {
        return this.priorityDataQueue.getImageUrl(imageId, height);
    };
    // Clears the cache if possible with this data source
    PriorityDataSource.prototype.clearCache = function () {
        return this.priorityDataQueue.clearCache();
    };
    // Get the cache expiry time in ms from epoch. No cached items before this time will be used
    PriorityDataSource.prototype.getCacheExpiry = function () {
        return this.priorityDataQueue.getCacheExpiry();
    };
    return PriorityDataSource;
}(DataSource_1.default));
exports.default = PriorityDataSource;
