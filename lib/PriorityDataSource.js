"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

// Behaves like a DataSource
// Created by a PriorityDataQueue
// Forwards performQuery call to the PriorityDataQueue that will forward them to the DataSource
var PriorityDataSource;

module.exports = PriorityDataSource =
/*#__PURE__*/
function () {
  function PriorityDataSource(priorityDataQueue, priority) {
    (0, _classCallCheck2["default"])(this, PriorityDataSource);
    this.priorityDataQueue = priorityDataQueue;
    this.priority = priority;
  }

  (0, _createClass2["default"])(PriorityDataSource, [{
    key: "performQuery",
    value: function performQuery(query, cb) {
      return this.priorityDataQueue.performQuery(query, cb, this.priority);
    }
  }, {
    key: "getImageUrl",
    value: function getImageUrl(imageId, height) {
      return this.priorityDataQueue.getImageUrl(imageId, height);
    } // Clears the cache if possible with this data source

  }, {
    key: "clearCache",
    value: function clearCache() {
      return this.priorityDataQueue.clearCache();
    } // Get the cache expiry time in ms from epoch. No cached items before this time will be used

  }, {
    key: "getCacheExpiry",
    value: function getCacheExpiry() {
      return this.priorityDataQueue.getCacheExpiry();
    }
  }]);
  return PriorityDataSource;
}();