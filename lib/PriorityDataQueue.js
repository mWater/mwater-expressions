"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
var async_1 = require("async");
var lodash_1 = __importDefault(require("lodash"));
var PriorityDataSource_1 = __importDefault(require("./PriorityDataSource"));
// Creates PriorityDataSource from DataSource
var PriorityDataQueue = /** @class */ (function () {
    function PriorityDataQueue(dataSource, concurrency) {
        var worker;
        this.dataSource = dataSource;
        // Creates a priorityQueue that calls performQuery
        worker = function (query, callback) {
            // Defer to prevent too-deep recursion
            return lodash_1.default.defer(function () {
                return dataSource.performQuery(query, callback);
            });
        };
        this.performQueryPriorityQueue = async_1.priorityQueue(worker, concurrency);
    }
    // Creates a PriorityDataSource that will then be used like a DataSource but with a priority
    PriorityDataQueue.prototype.createPriorityDataSource = function (priority) {
        return new PriorityDataSource_1.default(this, priority);
    };
    // Designed to be called by PriorityDataSource
    PriorityDataQueue.prototype.performQuery = function (query, cb, priority) {
        // Push to the priorityQueue
        return this.performQueryPriorityQueue.push(query, priority, cb);
    };
    // Clears the cache if possible with this data source
    PriorityDataQueue.prototype.clearCache = function () {
        return this.dataSource.clearCache();
    };
    // Get the cache expiry time in ms from epoch. No cached items before this time will be used
    PriorityDataQueue.prototype.getCacheExpiry = function () {
        return this.dataSource.getCacheExpiry();
    };
    // Simply call the dataSource since this is not an async function
    PriorityDataQueue.prototype.getImageUrl = function (imageId, height) {
        return this.dataSource.getImageUrl(imageId, height);
    };
    PriorityDataQueue.prototype.kill = function () {
        if (this.performQueryPriorityQueue != null) {
            return this.performQueryPriorityQueue.kill();
        }
    };
    return PriorityDataQueue;
}());
exports.default = PriorityDataQueue;
