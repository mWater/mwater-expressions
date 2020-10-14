import { JsonQLQuery } from "jsonql"
// Behaves like a DataSource
// Created by a PriorityDataQueue

import PriorityDataQueue from "./PriorityDataQueue"
import { Row } from "./types"

// Forwards performQuery call to the PriorityDataQueue that will forward them to the DataSource
export default class PriorityDataSource {
  priorityDataQueue: PriorityDataQueue
  priority: number

  constructor(priorityDataQueue: PriorityDataQueue, priority: number) {
    this.priorityDataQueue = priorityDataQueue
    this.priority = priority
  }

  performQuery(query: JsonQLQuery, cb: (err: any, results: Row[]) => void) {
    return this.priorityDataQueue.performQuery(query, cb, this.priority)
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
