var $, DataSource, LRU, MWaterDataSource, _, querystring,
  extend = function(child, parent) { for (var key in parent) { if (hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; },
  hasProp = {}.hasOwnProperty;

_ = require('lodash');

DataSource = require('./DataSource');

LRU = require("lru-cache");

querystring = require('querystring');

$ = require('jquery');

module.exports = MWaterDataSource = (function(superClass) {
  extend(MWaterDataSource, superClass);

  function MWaterDataSource(apiUrl, client, options) {
    if (options == null) {
      options = {};
    }
    MWaterDataSource.__super__.constructor.call(this);
    this.apiUrl = apiUrl;
    this.client = client;
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

  MWaterDataSource.prototype.performQuery = function(jsonql, cb) {
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
    if (jsonqlStr.length < 2000) {
      queryParams.jsonql = jsonqlStr;
      method = "GET";
    } else {
      method = "POST";
    }
    url = this.apiUrl + "jsonql?" + querystring.stringify(queryParams);
    headers = {};
    if (method === "GET") {
      if (!this.options.serverCaching) {
        headers['Cache-Control'] = "no-cache";
      } else if (this.cacheExpiry) {
        seconds = Math.floor((new Date().getTime() - this.cacheExpiry) / 1000);
        headers['Cache-Control'] = "max-age=" + seconds;
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
    }).done((function(_this) {
      return function(rows) {
        if (_this.options.localCaching) {
          _this.cache.set(cacheKey, rows);
        }
        return cb(null, rows);
      };
    })(this)).fail((function(_this) {
      return function(xhr) {
        return cb(new Error(xhr.responseText));
      };
    })(this));
  };

  MWaterDataSource.prototype.getCacheExpiry = function() {
    return this.cacheExpiry;
  };

  MWaterDataSource.prototype.clearCache = function() {
    var ref;
    if ((ref = this.cache) != null) {
      ref.reset();
    }
    return this.cacheExpiry = new Date().getTime();
  };

  MWaterDataSource.prototype.getImageUrl = function(imageId, height) {
    var apiUrl, query, url;
    apiUrl = this.options.imageApiUrl || this.apiUrl;
    url = apiUrl + ("images/" + imageId);
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
  };

  return MWaterDataSource;

})(DataSource);

MWaterDataSource["default"] = MWaterDataSource;
