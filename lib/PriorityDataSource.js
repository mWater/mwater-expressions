"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
// Forwards performQuery call to the PriorityDataQueue that will forward them to the DataSource
var PriorityDataSource = /** @class */ (function () {
    function PriorityDataSource(priorityDataQueue, priority) {
        this.priorityDataQueue = priorityDataQueue;
        this.priority = priority;
    }
    PriorityDataSource.prototype.performQuery = function (query, cb) {
        return this.priorityDataQueue.performQuery(query, cb, this.priority);
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
}());
exports.default = PriorityDataSource;
