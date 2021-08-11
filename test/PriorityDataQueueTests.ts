// TODO: This file was created by bulk-decaffeinate.
// Sanity-check the conversion and remove this comment.
import { assert } from "chai"
import { default as PriorityDataQueue } from "../src/PriorityDataQueue"
import DataSource from "../src/DataSource"

// Very simple DataSource implementation used for testing
class TestDataSource extends DataSource {
  performQuery(query: any, cb: any) {
    // Simply does an async callback passing back the query
    const call = () => cb(query)
    return setTimeout(call, 1)
  }
}

describe("PriorityDataQueue", function () {
  beforeEach(function () {
    const testDataSource = new TestDataSource()
    return (this.priorityDataQueue = new PriorityDataQueue(testDataSource, 1))
  })

  it("calling performQuery reaches the DataSource", function (testCallback) {
    const priorityDataSource = this.priorityDataQueue.createPriorityDataSource(1)
    return priorityDataSource.performQuery("test", function (data: any) {
      // Make sure that TestDataSource called back with the right data
      assert.equal("test", data)
      return testCallback()
    });
  })

  return it("priorityDataQueue properly prioritize the calls", function (testCallback) {
    let counter = 0

    function callDone(priority: any) {
      counter++
      // Make sure that the priority is called in the right order
      assert.equal(priority, counter)
      if (counter === 4) {
        return testCallback()
      }
    }

    const priorityDataSource1 = this.priorityDataQueue.createPriorityDataSource(1)
    const priorityDataSource2 = this.priorityDataQueue.createPriorityDataSource(2)
    const priorityDataSource3 = this.priorityDataQueue.createPriorityDataSource(3)
    const priorityDataSource4 = this.priorityDataQueue.createPriorityDataSource(4)
    // First one needs to be done first since the query is empty and the first call will have best priority
    priorityDataSource1.performQuery(1, (data: any) => callDone(1))
    // Then call with 3
    priorityDataSource3.performQuery(3, (data: any) => callDone(3))
    // Then call with 4
    priorityDataSource4.performQuery(4, (data: any) => callDone(4))
    // Then with 2
    return priorityDataSource2.performQuery(2, (data: any) => callDone(2));
  });
})
