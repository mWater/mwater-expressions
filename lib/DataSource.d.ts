import { JsonQLQuery } from "jsonql";
import { Row } from "./types";

export default class DataSource {
  /** Performs a single query. Calls cb with (error, rows) */
  performQuery(query: JsonQLQuery, cb: (error: any, rows: Row[]) => void): void

  /** Get the url to download an image (by id from an image or imagelist column)
    Height, if specified, is minimum height needed. May return larger image
    Can be used to upload by posting to this url
  */
  getImageUrl(imageId: string, height?: number): string

  // Clears the cache if possible with this data source
  clearCache(): void

  // Get the cache expiry time in ms from epoch. No cached items before this time will be used. 0 for no cache limit.
  // Useful for knowing when cache has been cleared, as it will be set to time of clearing.
  getCacheExpiry(): number
}
