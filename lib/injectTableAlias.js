var _, injectTableAlias, injectTableAliases;

_ = require('lodash');

injectTableAliases = require('./injectTableAliases');

injectTableAlias = function(jsonql, tableAlias) {
  return injectTableAliases(jsonql, {
    "{alias}": tableAlias
  });
};

module.exports = injectTableAlias;
