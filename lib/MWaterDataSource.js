var DataSource, LRU, MWaterDataSource, _,
  extend = function(child, parent) { for (var key in parent) { if (hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; },
  hasProp = {}.hasOwnProperty;

_ = require('lodash');

DataSource = require('./DataSource');

LRU = require("lru-cache");

module.exports = MWaterDataSource = (function(superClass) {
  extend(MWaterDataSource, superClass);

  function MWaterDataSource(apiUrl, client, options) {
    if (options == null) {
      options = {};
    }
    this.apiUrl = apiUrl;
    this.client = client;
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

  MWaterDataSource.prototype.performQuery = function(query, cb) {
    var cacheKey, cachedRows, headers, url;
    if (this.options.localCaching) {
      cacheKey = JSON.stringify(query);
      cachedRows = this.cache.get(cacheKey);
      if (cachedRows) {
        return cb(null, cachedRows);
      }
    }
    url = this.apiUrl + "jsonql?jsonql=" + encodeURIComponent(JSON.stringify(query));
    if (this.client) {
      url += "&client=" + this.client;
    }
    headers = {};
    if (!this.options.serverCaching) {
      headers['Cache-Control'] = "no-cache";
    }
    return $.ajax({
      dataType: "json",
      url: url,
      headers: headers
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

  MWaterDataSource.prototype.getImageUrl = function(imageId, height) {
    var url;
    url = this.apiUrl + ("images/" + imageId);
    if (height) {
      url += "?h=" + height;
    }
    return url;
  };

  return MWaterDataSource;

})(DataSource);
