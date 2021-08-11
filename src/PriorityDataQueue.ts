import { AsyncPriorityQueue, priorityQueue } from "async"
import { JsonQLQuery } from "jsonql"
import _ from "lodash"
import DataSource from "./DataSource"

import PriorityDataSource from "./PriorityDataSource"
import { Row } from "./types"

// Creates PriorityDataSource from DataSource
export default class PriorityDataQueue {
  dataSource: DataSource
  performQueryPriorityQueue: AsyncPriorityQueue<JsonQLQuery>

  constructor(dataSource: DataSource, concurrency: number) {
    var worker
    this.dataSource = dataSource

    // Creates a priorityQueue that calls performQuery
    worker = function (query: JsonQLQuery, callback: (err: any, results: Row[]) => void) {
      // Defer to prevent too-deep recursion
      return _.defer(() => {
        return dataSource.performQuery(query, callback)
      })
    }
    this.performQueryPriorityQueue = priorityQueue(worker, concurrency)
  }

  // Creates a PriorityDataSource that will then be used like a DataSource but with a priority
  createPriorityDataSource(priority: number) {
    return new PriorityDataSource(this, priority)
  }

  // Designed to be called by PriorityDataSource
  performQuery(query: JsonQLQuery, cb: (error: any, rows: Row[]) => void, priority: number) {
    // Push to the priorityQueue
    this.performQueryPriorityQueue.push(query, priority, cb)
  }

  // Clears the cache if possible with this data source
  clearCache() {
    this.dataSource.clearCache()
  }

  // Get the cache expiry time in ms from epoch. No cached items before this time will be used
  getCacheExpiry() {
    return this.dataSource.getCacheExpiry()
  }

  // Simply call the dataSource since this is not an async function
  getImageUrl(imageId: string, height?: number) {
    return this.dataSource.getImageUrl(imageId, height)
  }

  kill() {
    if (this.performQueryPriorityQueue != null) {
      this.performQueryPriorityQueue.kill()
    }
  }
}
