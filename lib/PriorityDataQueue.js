var PriorityDataQueue, PriorityDataSource, _, async;

async = require('async');

_ = require('lodash');

PriorityDataSource = require('./PriorityDataSource');

// Creates PriorityDataSource from DataSource
module.exports = PriorityDataQueue = class PriorityDataQueue {
  constructor(dataSource, concurrency) {
    var worker;
    this.dataSource = dataSource;
    // Creates a priorityQueue that calls performQuery
    worker = function(query, callback) {
      // Defer to prevent too-deep recursion
      return _.defer(() => {
        return dataSource.performQuery(query, callback);
      });
    };
    this.performQueryPriorityQueue = new async.priorityQueue(worker, concurrency);
  }

  // Creates a PriorityDataSource that will then be used like a DataSource but with a priority
  createPriorityDataSource(priority) {
    return new PriorityDataSource(this, priority);
  }

  // Designed to be called by PriorityDataSource
  performQuery(query, cb, priority) {
    // Push to the priorityQueue
    return this.performQueryPriorityQueue.push(query, priority, cb);
  }

  // Clears the cache if possible with this data source
  clearCache() {
    return this.dataSource.clearCache();
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
      return this.performQueryPriorityQueue.kill();
    }
  }

};
