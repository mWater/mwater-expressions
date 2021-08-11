assert = require('chai').assert

PriorityDataQueue = require('../src/PriorityDataQueue').default
DataSource = require '../src/DataSource'

# Very simple DataSource implementation used for testing
class TestDataSource extends DataSource
  performQuery: (query, cb) ->
    # Simply does an async callback passing back the query
    call = () ->
      cb(query)
    setTimeout(call, 1)

describe "PriorityDataQueue", ->
  beforeEach ->
    testDataSource = new TestDataSource()
    @priorityDataQueue = new PriorityDataQueue(testDataSource, 1)

  it "calling performQuery reaches the DataSource", (testCallback) ->
    priorityDataSource = @priorityDataQueue.createPriorityDataSource(1)
    priorityDataSource.performQuery('test', (data) ->
      # Make sure that TestDataSource called back with the right data
      assert.equal('test', data)
      testCallback()
    )

  it "priorityDataQueue properly prioritize the calls", (testCallback) ->
    counter = 0
    callDone = (priority) ->
      counter++
      # Make sure that the priority is called in the right order
      assert.equal priority, counter
      if counter == 4
        testCallback()

    priorityDataSource1 = @priorityDataQueue.createPriorityDataSource(1)
    priorityDataSource2 = @priorityDataQueue.createPriorityDataSource(2)
    priorityDataSource3 = @priorityDataQueue.createPriorityDataSource(3)
    priorityDataSource4 = @priorityDataQueue.createPriorityDataSource(4)
    # First one needs to be done first since the query is empty and the first call will have best priority
    priorityDataSource1.performQuery(1, (data) ->
      callDone(1)
    )
    # Then call with 3
    priorityDataSource3.performQuery(3, (data) ->
      callDone(3)
    )
    # Then call with 4
    priorityDataSource4.performQuery(4, (data) ->
      callDone(4)
    )
    # Then with 2
    priorityDataSource2.performQuery(2, (data) ->
      callDone(2)
    )
