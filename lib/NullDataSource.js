var DataSource, NullDataSource,
  extend = function(child, parent) { for (var key in parent) { if (hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; },
  hasProp = {}.hasOwnProperty;

DataSource = require('./DataSource');

module.exports = NullDataSource = (function(superClass) {
  extend(NullDataSource, superClass);

  function NullDataSource() {
    return NullDataSource.__super__.constructor.apply(this, arguments);
  }

  NullDataSource.prototype.performQuery = function(query, cb) {
    return cb(null, []);
  };

  return NullDataSource;

})(DataSource);
