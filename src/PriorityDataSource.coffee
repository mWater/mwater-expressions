

# Behaves like a DataSource
# Created by a PriorityDataQueue
# Forwards performQuery/performQueries call to the PriorityDataQueue that will forward them to the DataSource
module.exports = class PriorityDataSource

  constructor: (priorityDataQueue, priority) ->
    @priorityDataQueue = priorityDataQueue
    @priority = priority

  performQueries: (queries, cb) ->
    @priorityDataQueue.performQueries(queries, cb, @priority)

  performQueries: (query, cb) ->
    @priorityDataQueue.performQuery(query, cb, @priority)