var DataSource, NullDataSource;

DataSource = require('./DataSource');

// Data source which always returns empty queries
module.exports = NullDataSource = class NullDataSource extends DataSource {
  // Performs a single query. Calls cb with rows
  performQuery(query, cb) {
    return cb(null, []);
  }

};
