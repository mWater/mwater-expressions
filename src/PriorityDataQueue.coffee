async = require 'async'

PriorityDataSource = require './PriorityDataSource'

# Creates PriorityDataSource from DataSource
module.exports = class PriorityDataQueue

  constructor: (dataSource, concurrency) ->
    @dataSource = dataSource
    # Creates a priorityQueue that calls performQuery
    worker = (query, callback) ->
      dataSource.performQuery(query, callback)
    @performQueryPriorityQueue = new async.priorityQueue(worker, concurrency)

  # Creates a PriorityDataSource that will then be used like a DataSource but with a priority
  createPriorityDataSource : (priority) ->
    return new PriorityDataSource(this, priority)

  # Designed to be called by PriorityDataSource
  performQuery: (query, cb, priority) ->
    # Push to the priorityQueue
    @performQueryPriorityQueue.push query, priority, cb

  # Simply call the dataSource since this is not an async function
  getImageUrl: (imageId, height) ->
    @dataSource.getImageUrl(imageId, height)

  kill: () ->
    if @performQueryPriorityQueue?
      @performQueryPriorityQueue.kill()
