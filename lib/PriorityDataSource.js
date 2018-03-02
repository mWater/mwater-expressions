// Behaves like a DataSource
// Created by a PriorityDataQueue
// Forwards performQuery call to the PriorityDataQueue that will forward them to the DataSource
var PriorityDataSource;

module.exports = PriorityDataSource = class PriorityDataSource {
  constructor(priorityDataQueue, priority) {
    this.priorityDataQueue = priorityDataQueue;
    this.priority = priority;
  }

  performQuery(query, cb) {
    return this.priorityDataQueue.performQuery(query, cb, this.priority);
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

};
