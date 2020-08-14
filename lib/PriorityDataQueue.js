"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var PriorityDataQueue, PriorityDataSource, _, async;

async = require('async');
_ = require('lodash');
PriorityDataSource = require('./PriorityDataSource'); // Creates PriorityDataSource from DataSource

module.exports = PriorityDataQueue = /*#__PURE__*/function () {
  function PriorityDataQueue(dataSource, concurrency) {
    (0, _classCallCheck2["default"])(this, PriorityDataQueue);
    var worker;
    this.dataSource = dataSource; // Creates a priorityQueue that calls performQuery

    worker = function worker(query, callback) {
      // Defer to prevent too-deep recursion
      return _.defer(function () {
        return dataSource.performQuery(query, callback);
      });
    };

    this.performQueryPriorityQueue = new async.priorityQueue(worker, concurrency);
  } // Creates a PriorityDataSource that will then be used like a DataSource but with a priority


  (0, _createClass2["default"])(PriorityDataQueue, [{
    key: "createPriorityDataSource",
    value: function createPriorityDataSource(priority) {
      return new PriorityDataSource(this, priority);
    } // Designed to be called by PriorityDataSource

  }, {
    key: "performQuery",
    value: function performQuery(query, cb, priority) {
      // Push to the priorityQueue
      return this.performQueryPriorityQueue.push(query, priority, cb);
    } // Clears the cache if possible with this data source

  }, {
    key: "clearCache",
    value: function clearCache() {
      return this.dataSource.clearCache();
    } // Get the cache expiry time in ms from epoch. No cached items before this time will be used

  }, {
    key: "getCacheExpiry",
    value: function getCacheExpiry() {
      return this.dataSource.getCacheExpiry();
    } // Simply call the dataSource since this is not an async function

  }, {
    key: "getImageUrl",
    value: function getImageUrl(imageId, height) {
      return this.dataSource.getImageUrl(imageId, height);
    }
  }, {
    key: "kill",
    value: function kill() {
      if (this.performQueryPriorityQueue != null) {
        return this.performQueryPriorityQueue.kill();
      }
    }
  }]);
  return PriorityDataQueue;
}();