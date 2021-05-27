"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const async_1 = require("async");
const lodash_1 = __importDefault(require("lodash"));
const PriorityDataSource_1 = __importDefault(require("./PriorityDataSource"));
// Creates PriorityDataSource from DataSource
class PriorityDataQueue {
    constructor(dataSource, concurrency) {
        var worker;
        this.dataSource = dataSource;
        // Creates a priorityQueue that calls performQuery
        worker = function (query, callback) {
            // Defer to prevent too-deep recursion
            return lodash_1.default.defer(() => {
                return dataSource.performQuery(query, callback);
            });
        };
        this.performQueryPriorityQueue = async_1.priorityQueue(worker, concurrency);
    }
    // Creates a PriorityDataSource that will then be used like a DataSource but with a priority
    createPriorityDataSource(priority) {
        return new PriorityDataSource_1.default(this, priority);
    }
    // Designed to be called by PriorityDataSource
    performQuery(query, cb, priority) {
        // Push to the priorityQueue
        this.performQueryPriorityQueue.push(query, priority, cb);
    }
    // Clears the cache if possible with this data source
    clearCache() {
        this.dataSource.clearCache();
    }
    // Get the cache expiry time in ms from epoch. No cached items before this time will be used
    getCacheExpiry() {
        return this.dataSource.getCacheExpiry();
    }
    // Simply call the dataSource since this is not an async function
    getImageUrl(imageId, height) {
        return this.dataSource.getImageUrl(imageId, height);
    }
    kill() {
        if (this.performQueryPriorityQueue != null) {
            this.performQueryPriorityQueue.kill();
        }
    }
}
exports.default = PriorityDataQueue;
