var DataSource, LRU, MWaterDataSource, _, querystring;

_ = require('lodash');

DataSource = require('./DataSource');

LRU = require("lru-cache");

querystring = require('querystring');

// Caching data source for mWater. Requires jQuery. require explicitly: require('mwater-expressions/lib/MWaterDataSource')
module.exports = MWaterDataSource = class MWaterDataSource extends DataSource {
  // options:
  // serverCaching: allows server to send cached results. default true
  // localCaching allows local MRU cache. default true
  // imageApiUrl: overrides apiUrl for images
  constructor(apiUrl, client, options = {}) {
    super();
    this.apiUrl = apiUrl;
    this.client = client;
    // cacheExpiry is time in ms from epoch that is oldest data that can be accepted. 0 = any (if serverCaching is true)
    this.cacheExpiry = 0;
    _.defaults(options, {
      serverCaching: true,
      localCaching: true
    });
    this.options = options;
    if (this.options.localCaching) {
      this.cache = LRU({
        max: 500,
        maxAge: 1000 * 15 * 60
      });
    }
  }

  performQuery(jsonql, cb) {
    var cacheKey, cachedRows, headers, jsonqlStr, method, queryParams, seconds, url;
    if (this.options.localCaching) {
      cacheKey = JSON.stringify(jsonql);
      cachedRows = this.cache.get(cacheKey);
      if (cachedRows) {
        return cb(null, cachedRows);
      }
    }
    queryParams = {};
    if (this.client) {
      queryParams.client = this.client;
    }
    jsonqlStr = JSON.stringify(jsonql);
    // Add as GET if short, POST otherwise
    if (jsonqlStr.length < 10000) {
      queryParams.jsonql = jsonqlStr;
      method = "GET";
    } else {
      method = "POST";
    }
    url = this.apiUrl + "jsonql?" + querystring.stringify(queryParams);
    // Setup caching
    headers = {};
    if (method === "GET") {
      if (!this.options.serverCaching) {
        headers['Cache-Control'] = "no-cache";
      } else if (this.cacheExpiry) {
        seconds = Math.floor((new Date().getTime() - this.cacheExpiry) / 1000);
        headers['Cache-Control'] = `max-age=${seconds}`;
      }
    }
    return $.ajax({
      dataType: "json",
      method: method,
      url: url,
      headers: headers,
      data: method === "POST" ? {
        jsonql: jsonqlStr
      } : void 0
    }).done((rows) => {
      if (this.options.localCaching) {
        // Cache rows
        this.cache.set(cacheKey, rows);
      }
      return cb(null, rows);
    }).fail((xhr) => {
      return cb(new Error(xhr.responseText));
    });
  }

  // Get the cache expiry time in ms from epoch. No cached items before this time will be used
  getCacheExpiry() {
    return this.cacheExpiry;
  }

  // Clears the local cache 
  clearCache() {
    var ref;
    if ((ref = this.cache) != null) {
      ref.reset();
    }
    // Set new cache expiry
    return this.cacheExpiry = new Date().getTime();
  }

  // Get the url to download an image (by id from an image or imagelist column)
  // Height, if specified, is minimum height needed. May return larger image
  // Can be used to upload by posting to this url
  getImageUrl(imageId, height) {
    var apiUrl, query, url;
    apiUrl = this.options.imageApiUrl || this.apiUrl;
    url = apiUrl + `images/${imageId}`;
    query = {};
    if (height) {
      query.h = height;
    }
    if (this.client) {
      query.client = this.client;
    }
    if (!_.isEmpty(query)) {
      url += "?" + querystring.stringify(query);
    }
    return url;
  }

};
