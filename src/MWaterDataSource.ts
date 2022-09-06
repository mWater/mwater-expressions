import _ from "lodash"
import DataSource from "./DataSource"
import LRU from "lru-cache"
import querystring from "querystring"
import $ from "jquery"
import { JsonQLQuery } from "jsonql"
import { Row } from "./types"

/** Caching data source for mWater. Requires jQuery. require explicitly: require('mwater-expressions/lib/MWaterDataSource') */
export default class MWaterDataSource extends DataSource {
  apiUrl: string
  client: string | null | undefined
  cacheExpiry: number
  options: { serverCaching?: boolean; localCaching?: boolean; imageApiUrl?: string }
  cache: LRU<string, Row[]>
  
  /**
   * @param apiUrl
   * @param options serverCaching: allows server to send cached results. default true
   * localCaching allows local MRU cache. default true
   * imageApiUrl: overrides apiUrl for images
   */
   constructor(
    apiUrl: string,
    client?: string | null,
    options: { serverCaching?: boolean; localCaching?: boolean; imageApiUrl?: string } = {}
  ) {
    super()
    this.apiUrl = apiUrl
    this.client = client

    // cacheExpiry is time in ms from epoch that is oldest data that can be accepted. 0 = any (if serverCaching is true)
    this.cacheExpiry = 0

    _.defaults(options, { serverCaching: true, localCaching: true })
    this.options = options

    if (this.options.localCaching) {
      this.cache = new LRU({ max: 500, maxAge: 1000 * 15 * 60 })
    }
  }

  performQuery(query: JsonQLQuery): Promise<Row[]>
  performQuery(query: JsonQLQuery, cb: (error: any, rows: Row[]) => void): void
  performQuery(query: any, cb?: any): any {
    // If no callback, use promise
    let cacheKey: any, method
    if (!cb) {
      return new Promise((resolve, reject) => {
        return this.performQuery(query, (error: any, rows: any) => {
          if (error) {
            return reject(error)
          } else {
            return resolve(rows)
          }
        })
      })
    }

    if (this.options.localCaching) {
      cacheKey = JSON.stringify(query)
      const cachedRows = this.cache.get(cacheKey)
      if (cachedRows) {
        cb(null, cachedRows)
        return
      }
    }

    const queryParams: any = {}
    if (this.client) {
      queryParams.client = this.client
    }

    const jsonqlStr = JSON.stringify(query)

    // Add as GET if short, POST otherwise
    if (jsonqlStr.length < 2000) {
      queryParams.jsonql = jsonqlStr
      method = "GET"
    } else {
      method = "POST"
    }

    // Setup caching
    const headers = {}
    if (method === "GET") {
      if (!this.options.serverCaching) {
        // Using headers forces OPTIONS call, so use timestamp to disable caching
        // headers['Cache-Control'] = "no-cache"
        queryParams.ts = Date.now()
      } else if (this.cacheExpiry) {
        const seconds = Math.floor((new Date().getTime() - this.cacheExpiry) / 1000)
        headers["Cache-Control"] = `max-age=${seconds}`
      }
    }

    // Create URL
    const url = this.apiUrl + "jsonql?" + querystring.stringify(queryParams)

    $.ajax({
      dataType: "json",
      method,
      url,
      headers,
      data: method === "POST" ? { jsonql: jsonqlStr } : undefined
    })
      .done((rows: any) => {
        if (this.options.localCaching) {
          // Cache rows
          this.cache.set(cacheKey, rows)
        }

        cb(null, rows)
      })
      .fail((xhr: any) => {
        cb(new Error(xhr.responseText))
      })
  }

  // Get the cache expiry time in ms from epoch. No cached items before this time will be used
  getCacheExpiry() {
    return this.cacheExpiry
  }

  // Clears the local cache
  clearCache() {
    this.cache?.reset()

    // Set new cache expiry
    this.cacheExpiry = new Date().getTime()
  }

  // Get the url to download an image (by id from an image or imagelist column)
  // Height, if specified, is minimum height needed. May return larger image
  // Can be used to upload by posting to this url
  getImageUrl(imageId: any, height: any) {
    const apiUrl = this.options.imageApiUrl || this.apiUrl

    let url = apiUrl + `images/${imageId}`
    const query: any = {}
    if (height) {
      query.h = height
    }
    if (this.client) {
      query.client = this.client
    }

    if (!_.isEmpty(query)) {
      url += "?" + querystring.stringify(query)
    }

    return url
  }
}
