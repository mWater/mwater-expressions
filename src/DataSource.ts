import { JsonQLQuery } from "jsonql"
import _ from "lodash"
import { Row } from "./types"

// Fetches data for queries
export default class DataSource {
  /** Performs a single query. Calls cb with (error, rows) or uses promise if no callback */
  performQuery(query: JsonQLQuery): Promise<Row[]>
  performQuery(query: JsonQLQuery, cb: (error: any, rows: Row[]) => void): void
  performQuery(query: any, cb?: any): any {
    throw new Error("Not implemented")
  }

  /** Get the url to download an image (by id from an image or imagelist column)
    Height, if specified, is minimum height needed. May return larger image
    Can be used to upload by posting to this url
  */
  getImageUrl(imageId: string, height?: number): string {
    throw new Error("Not implemented")
  }

  // Clears the cache if possible with this data source
  clearCache(): void {
    throw new Error("Not implemented")
  }

  // Get the cache expiry time in ms from epoch. No cached items before this time will be used. 0 for no cache limit.
  // Useful for knowing when cache has been cleared, as it will be set to time of clearing.
  getCacheExpiry(): number {
    throw new Error("Not implemented")
  }
}
