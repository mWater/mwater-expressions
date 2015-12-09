var _, injectTableAliases;

_ = require('lodash');

injectTableAliases = function(jsonql, aliases) {
  if (!jsonql) {
    return jsonql;
  }
  if (_.isArray(jsonql)) {
    return _.map(jsonql, (function(_this) {
      return function(item) {
        return injectTableAliases(item, aliases);
      };
    })(this));
  }
  if (!_.isObject(jsonql)) {
    return jsonql;
  }
  if (jsonql.type === "field" && aliases[jsonql.tableAlias]) {
    return _.extend({}, jsonql, {
      tableAlias: aliases[jsonql.tableAlias]
    });
  }
  return _.mapValues(jsonql, (function(_this) {
    return function(value) {
      return injectTableAliases(value, aliases);
    };
  })(this));
};

module.exports = injectTableAliases;
