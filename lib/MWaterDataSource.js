"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var _inherits2 = _interopRequireDefault(require("@babel/runtime/helpers/inherits"));

var _possibleConstructorReturn2 = _interopRequireDefault(require("@babel/runtime/helpers/possibleConstructorReturn"));

var _getPrototypeOf2 = _interopRequireDefault(require("@babel/runtime/helpers/getPrototypeOf"));

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = (0, _getPrototypeOf2["default"])(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = (0, _getPrototypeOf2["default"])(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return (0, _possibleConstructorReturn2["default"])(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

var $, DataSource, LRU, MWaterDataSource, _, querystring;

_ = require('lodash');
DataSource = require('./DataSource');
LRU = require("lru-cache");
querystring = require('querystring');
$ = require('jquery'); // Caching data source for mWater. Requires jQuery. require explicitly: require('mwater-expressions/lib/MWaterDataSource')

module.exports = MWaterDataSource = /*#__PURE__*/function (_DataSource) {
  (0, _inherits2["default"])(MWaterDataSource, _DataSource);

  var _super = _createSuper(MWaterDataSource);

  // options:
  // serverCaching: allows server to send cached results. default true
  // localCaching allows local MRU cache. default true
  // imageApiUrl: overrides apiUrl for images
  function MWaterDataSource(apiUrl, client) {
    var _this;

    var options = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : {};
    (0, _classCallCheck2["default"])(this, MWaterDataSource);
    _this = _super.call(this);
    _this.apiUrl = apiUrl;
    _this.client = client; // cacheExpiry is time in ms from epoch that is oldest data that can be accepted. 0 = any (if serverCaching is true)

    _this.cacheExpiry = 0;

    _.defaults(options, {
      serverCaching: true,
      localCaching: true
    });

    _this.options = options;

    if (_this.options.localCaching) {
      _this.cache = LRU({
        max: 500,
        maxAge: 1000 * 15 * 60
      });
    }

    return _this;
  }

  (0, _createClass2["default"])(MWaterDataSource, [{
    key: "performQuery",
    value: function performQuery(jsonql, cb) {
      var _this2 = this;

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

      jsonqlStr = JSON.stringify(jsonql); // Add as GET if short, POST otherwise

      if (jsonqlStr.length < 2000) {
        queryParams.jsonql = jsonqlStr;
        method = "GET";
      } else {
        method = "POST";
      } // Setup caching


      headers = {};

      if (method === "GET") {
        if (!this.options.serverCaching) {
          // Using headers forces OPTIONS call, so use timestamp to disable caching
          // headers['Cache-Control'] = "no-cache"
          queryParams.ts = Date.now();
        } else if (this.cacheExpiry) {
          seconds = Math.floor((new Date().getTime() - this.cacheExpiry) / 1000);
          headers['Cache-Control'] = "max-age=".concat(seconds);
        }
      } // Create URL


      url = this.apiUrl + "jsonql?" + querystring.stringify(queryParams);
      return $.ajax({
        dataType: "json",
        method: method,
        url: url,
        headers: headers,
        data: method === "POST" ? {
          jsonql: jsonqlStr
        } : void 0
      }).done(function (rows) {
        if (_this2.options.localCaching) {
          // Cache rows
          _this2.cache.set(cacheKey, rows);
        }

        return cb(null, rows);
      }).fail(function (xhr) {
        return cb(new Error(xhr.responseText));
      });
    } // Get the cache expiry time in ms from epoch. No cached items before this time will be used

  }, {
    key: "getCacheExpiry",
    value: function getCacheExpiry() {
      return this.cacheExpiry;
    } // Clears the local cache 

  }, {
    key: "clearCache",
    value: function clearCache() {
      var ref;

      if ((ref = this.cache) != null) {
        ref.reset();
      } // Set new cache expiry


      return this.cacheExpiry = new Date().getTime();
    } // Get the url to download an image (by id from an image or imagelist column)
    // Height, if specified, is minimum height needed. May return larger image
    // Can be used to upload by posting to this url

  }, {
    key: "getImageUrl",
    value: function getImageUrl(imageId, height) {
      var apiUrl, query, url;
      apiUrl = this.options.imageApiUrl || this.apiUrl;
      url = apiUrl + "images/".concat(imageId);
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
  }]);
  return MWaterDataSource;
}(DataSource); // Make ES6 compatible


MWaterDataSource["default"] = MWaterDataSource;