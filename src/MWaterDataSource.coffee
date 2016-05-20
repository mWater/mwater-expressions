_ = require 'lodash'
DataSource = require './DataSource'
LRU = require("lru-cache")

# Caching data source for mWater. Requires jQuery. require explicitly: require('mwater-expressions/lib/MWaterDataSource')
module.exports = class MWaterDataSource extends DataSource
  # options:
  # serverCaching: allows server to send cached results. default true
  # localCaching allows local MRU cache. default true
  constructor: (apiUrl, client, options = {}) ->
    @apiUrl = apiUrl
    @client = client

    _.defaults(options, { serverCaching: true, localCaching: true })
    @options = options

    if @options.localCaching
      @cache = LRU({ max: 500, maxAge: 1000 * 15 * 60 })

  performQuery: (query, cb) ->
    if @options.localCaching
      cacheKey = JSON.stringify(query)
      cachedRows = @cache.get(cacheKey)
      if cachedRows
        return cb(null, cachedRows)

    url = @apiUrl + "jsonql?jsonql=" + encodeURIComponent(JSON.stringify(query))
    if @client
      url += "&client=#{@client}"

    # Setup caching
    headers = {}
    if not @options.serverCaching
      headers['Cache-Control'] = "no-cache"

    $.ajax({ dataType: "json", url: url, headers: headers })
      .done (rows) =>
        if @options.localCaching
          # Cache rows
          @cache.set(cacheKey, rows)
          
        cb(null, rows)
      .fail (xhr) =>
        cb(new Error(xhr.responseText))

  # Clears the local cache 
  clearCache: ->
    @cache?.reset()

  # Get the url to download an image (by id from an image or imagelist column)
  # Height, if specified, is minimum height needed. May return larger image
  getImageUrl: (imageId, height) ->
    url = @apiUrl + "images/#{imageId}"
    if height
      url += "?h=#{height}"

    return url