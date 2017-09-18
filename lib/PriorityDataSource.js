var PriorityDataSource;

module.exports = PriorityDataSource = (function() {
  function PriorityDataSource(priorityDataQueue, priority) {
    this.priorityDataQueue = priorityDataQueue;
    this.priority = priority;
  }

  PriorityDataSource.prototype.performQuery = function(query, cb) {
    return this.priorityDataQueue.performQuery(query, cb, this.priority);
  };

  PriorityDataSource.prototype.getImageUrl = function(imageId, height) {
    return this.priorityDataQueue.getImageUrl(imageId, height);
  };

  PriorityDataSource.prototype.clearCache = function() {};

  return PriorityDataSource;

})();
