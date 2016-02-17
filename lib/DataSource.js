var DataSource, _, async;

_ = require('lodash');

async = require('async');

module.exports = DataSource = (function() {
  function DataSource() {}

  DataSource.prototype.performQuery = function(query, cb) {
    throw new Error("Not implemented");
  };

  DataSource.prototype.getImageUrl = function(imageId, height) {
    throw new Error("Not implemented");
  };

  return DataSource;

})();
