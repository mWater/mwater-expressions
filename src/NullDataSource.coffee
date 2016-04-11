DataSource = require './DataSource'

# Data source which always returns empty queries
module.exports = class NullDataSource extends DataSource
  # Performs a single query. Calls cb with rows
  performQuery: (query, cb) -> cb([])

