import { JsonQLQuery } from "jsonql"
import DataSource from "./DataSource"
// Behaves like a DataSource
// Created by a PriorityDataQueue

import PriorityDataQueue from "./PriorityDataQueue"
import { Row } from "./types"

// Forwards performQuery call to the PriorityDataQueue that will forward them to the DataSource
export default class PriorityDataSource extends DataSource {
  priorityDataQueue: PriorityDataQueue
  priority: number

  constructor(priorityDataQueue: PriorityDataQueue, priority: number) {
    super()
    this.priorityDataQueue = priorityDataQueue
    this.priority = priority
  }

  performQuery(query: JsonQLQuery): Promise<Row[]>;
  performQuery(query: JsonQLQuery, cb: (error: any, rows: Row[]) => void): void;
  performQuery(query: JsonQLQuery, cb?: (err: any, results: Row[]) => void): Promise<Row[]> | void {
    if (cb) {
      this.priorityDataQueue.performQuery(query, cb, this.priority)
      return
    }

    return new Promise<Row[]>((resolve, reject) => {
      this.priorityDataQueue.performQuery(query, (err, results) => {
        if (err) {
          reject(err)
        }
        else {
          resolve(results)
        }
      }, this.priority)
    })
  }

  getImageUrl(imageId: string, height?: number) {
    return this.priorityDataQueue.getImageUrl(imageId, height)
  }

  // Clears the cache if possible with this data source
  clearCache() {
    return this.priorityDataQueue.clearCache()
  }

  // Get the cache expiry time in ms from epoch. No cached items before this time will be used
  getCacheExpiry() {
    return this.priorityDataQueue.getCacheExpiry()
  }
}
