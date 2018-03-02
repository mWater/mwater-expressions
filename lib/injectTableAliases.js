var _, injectTableAliases;

_ = require('lodash');

// Recursively inject table aliases 
// aliases is map of replacement to table aliases. For example, { "{a}": b } will replace "{a}" with "b"
injectTableAliases = function(jsonql, aliases) {
  // Handle empty
  if (!jsonql) {
    return jsonql;
  }
  // Handle arrays
  if (_.isArray(jsonql)) {
    return _.map(jsonql, (item) => {
      return injectTableAliases(item, aliases);
    });
  }
  // Handle non-objects by leaving alone
  if (!_.isObject(jsonql)) {
    return jsonql;
  }
  // Handle field
  if (jsonql.type === "field" && aliases[jsonql.tableAlias]) {
    return _.extend({}, jsonql, {
      tableAlias: aliases[jsonql.tableAlias]
    });
  }
  // Recurse object keys
  return _.mapValues(jsonql, (value) => {
    return injectTableAliases(value, aliases);
  });
};

module.exports = injectTableAliases;
