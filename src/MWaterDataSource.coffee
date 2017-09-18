_ = require 'lodash'
DataSource = require './DataSource'
LRU = require("lru-cache")
querystring = require 'querystring'

# Caching data source for mWater. Requires jQuery. require explicitly: require('mwater-expressions/lib/MWaterDataSource')
module.exports = class MWaterDataSource extends DataSource
  # options:
  # serverCaching: allows server to send cached results. default true
  # localCaching allows local MRU cache. default true
  # imageApiUrl: overrides apiUrl for images
  constructor: (apiUrl, client, options = {}) ->
    @apiUrl = apiUrl
    @client = client

    # cacheExpiry is time in ms from epoch that is oldest data that can be accepted. 0 = any (if serverCaching is true)
    @cacheExpiry = 0

    _.defaults(options, { serverCaching: true, localCaching: true })
    @options = options

    if @options.localCaching
      @cache = LRU({ max: 500, maxAge: 1000 * 15 * 60 })

  performQuery: (jsonql, cb) ->
    if @options.localCaching
      cacheKey = JSON.stringify(jsonql)
      cachedRows = @cache.get(cacheKey)
      if cachedRows
        return cb(null, cachedRows)

    queryParams = {}
    if @client
      queryParams.client = @client

    jsonqlStr = JSON.stringify(jsonql)

    # Add as GET if short, POST otherwise
    if jsonqlStr.length < 10000
      queryParams.jsonql = jsonqlStr
      method = "GET"
    else
      method = "POST"

    url = @apiUrl + "jsonql?" + querystring.stringify(queryParams)

    # Setup caching
    headers = {}
    if method == "GET"
      if not @options.serverCaching
        headers['Cache-Control'] = "no-cache"
      else if @cacheExpiry
        seconds = Math.floor((new Date().getTime() - @cacheExpiry) / 1000)
        headers['Cache-Control'] = "max-age=#{seconds}"

    $.ajax({ 
      dataType: "json"
      method: method
      url: url
      headers: headers
      data: if method == "POST" then { jsonql: jsonqlStr }
    }).done (rows) =>
      if @options.localCaching
        # Cache rows
        @cache.set(cacheKey, rows)
        
      cb(null, rows)
    .fail (xhr) =>
      cb(new Error(xhr.responseText))

  # Get the cache expiry time in ms from epoch. No cached items before this time will be used
  getCacheExpiry: -> @cacheExpiry

  # Clears the local cache 
  clearCache: ->
    @cache?.reset()

    # Set new cache expiry
    @cacheExpiry = new Date().getTime()

  # Get the url to download an image (by id from an image or imagelist column)
  # Height, if specified, is minimum height needed. May return larger image
  # Can be used to upload by posting to this url
  getImageUrl: (imageId, height) ->
    apiUrl = @options.imageApiUrl or @apiUrl 

    url = apiUrl + "images/#{imageId}"
    query = {}
    if height
      query.h = height
    if @client
      query.client = @client

    if not _.isEmpty(query)
      url += "?" + querystring.stringify(query)

    return url