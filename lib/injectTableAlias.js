var _, injectTableAlias, injectTableAliases;

_ = require('lodash');

injectTableAliases = require('./injectTableAliases');

// Recursively inject table alias tableAlias for `{alias}` 
injectTableAlias = function(jsonql, tableAlias) {
  return injectTableAliases(jsonql, {
    "{alias}": tableAlias
  });
};

module.exports = injectTableAlias;
