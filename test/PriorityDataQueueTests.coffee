assert = require('chai').assert

PriorityDataQueue = require '../src/PriorityDataQueue'

describe "PriorityDataQueue", ->
  beforeEach ->
    # Nothing to do yet
    null

  it "tests something", ->
    testDataSource = new TestDataSource()
    new PriorityDataQueue(testDataSource, )