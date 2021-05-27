"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const DataSource_1 = __importDefault(require("./DataSource"));
// Forwards performQuery call to the PriorityDataQueue that will forward them to the DataSource
class PriorityDataSource extends DataSource_1.default {
    constructor(priorityDataQueue, priority) {
        super();
        this.priorityDataQueue = priorityDataQueue;
        this.priority = priority;
    }
    performQuery(query, cb) {
        if (cb) {
            this.priorityDataQueue.performQuery(query, cb, this.priority);
            return;
        }
        return new Promise((resolve, reject) => {
            this.priorityDataQueue.performQuery(query, (err, results) => {
                if (err) {
                    reject(err);
                }
                else {
                    resolve(results);
                }
            }, this.priority);
        });
    }
    getImageUrl(imageId, height) {
        return this.priorityDataQueue.getImageUrl(imageId, height);
    }
    // Clears the cache if possible with this data source
    clearCache() {
        return this.priorityDataQueue.clearCache();
    }
    // Get the cache expiry time in ms from epoch. No cached items before this time will be used
    getCacheExpiry() {
        return this.priorityDataQueue.getCacheExpiry();
    }
}
exports.default = PriorityDataSource;
