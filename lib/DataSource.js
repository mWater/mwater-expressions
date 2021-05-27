"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var DataSource, _;

_ = require('lodash'); // Fetches data for queries

module.exports = DataSource = /*#__PURE__*/function () {
  function DataSource() {
    (0, _classCallCheck2["default"])(this, DataSource);
  }

  (0, _createClass2["default"])(DataSource, [{
    key: "performQuery",
    // Performs a single query. Calls cb with (error, rows) or uses promise if no callback
    value: function performQuery(query, cb) {
      throw new Error("Not implemented");
    } // Get the url to download an image (by id from an image or imagelist column)
    // Height, if specified, is minimum height needed. May return larger image
    // Can be used to upload by posting to this url

  }, {
    key: "getImageUrl",
    value: function getImageUrl(imageId, height) {
      throw new Error("Not implemented");
    } // Clears the cache if possible with this data source

  }, {
    key: "clearCache",
    value: function clearCache() {
      throw new Error("Not implemented");
    } // Get the cache expiry time in ms from epoch. No cached items before this time will be used. 0 for no cache limit.
    // Useful for knowing when cache has been cleared, as it will be set to time of clearing.

  }, {
    key: "getCacheExpiry",
    value: function getCacheExpiry() {
      throw new Error("Not implemented");
    }
  }]);
  return DataSource;
}();