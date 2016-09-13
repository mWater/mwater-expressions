var DataSource, _;

_ = require('lodash');

module.exports = DataSource = (function() {
  function DataSource() {}

  DataSource.prototype.performQuery = function(query, cb) {
    throw new Error("Not implemented");
  };

  DataSource.prototype.getImageUrl = function(imageId, height) {
    throw new Error("Not implemented");
  };

  DataSource.prototype.clearCache = function() {};

  return DataSource;

})();
