async = require 'async'

# Creates PriorityDataSource from DataSource
module.exports = class PriorityDataQueue

  constructor: (dataSource, concurrency) ->
    @dataSource = dataSource
    worker = (task, callback) ->
      console.log task
      callback()
    @priorityQueue = new async.priorityQueue(worker, concurrency)

  createPriorityDataSource : (priority) ->
    return new PriorityDataSource(@dataSource, priority)

  performQueries: (queries, cb, priority) ->
    @priorityQueue.push queries, priority, cb

  performQuery: (query, cb, priority) ->
    @priorityQueue.push query, priority, cb
