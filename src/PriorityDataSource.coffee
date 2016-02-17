
# Behaves like a DataSource
# Created by a PriorityDataQueue
# Forwards performQuery call to the PriorityDataQueue that will forward them to the DataSource
module.exports = class PriorityDataSource

  constructor: (priorityDataQueue, priority) ->
    @priorityDataQueue = priorityDataQueue
    @priority = priority

  performQuery: (query, cb) ->
    @priorityDataQueue.performQuery(query, cb, @priority)

  getImageUrl: (imageId, height) ->
    @priorityDataQueue.getImageUrl(imageId, height)