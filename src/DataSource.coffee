_ = require 'lodash'
async = require 'async'

# Fetches data for queries
module.exports = class DataSource
  # Performs a single query. Calls cb with (error, rows)
  performQuery: (query, cb) ->
    throw new Error("Not implemented")

  # Get the url to download an image (by id from an image or imagelist column)
  # Height, if specified, is minimum height needed. May return larger image
  getImageUrl: (imageId, height) ->
    throw new Error("Not implemented")

  # Clears the cache if possible with this data source
  clearCache: ->
    # Do nothing by default
    return
