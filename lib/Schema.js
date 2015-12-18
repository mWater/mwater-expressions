var H, React, Schema, _;

_ = require('lodash');

React = require('react');

H = React.DOM;

module.exports = Schema = (function() {
  function Schema(json) {
    this.tables = [];
    this.tableMap = {};
    this.columnMap = {};
    if (json) {
      this.tables = _.cloneDeep(json.tables);
      this.tables = _.map(this.tables, this.stripIdColumns);
      this._reindex();
    }
  }

  Schema.prototype._reindex = function() {
    var i, item, len, mapContent, ref, results, table;
    this.tableMap = {};
    this.columnMap = {};
    mapContent = (function(_this) {
      return function(table, item) {
        var i, item2, len, ref, results;
        if (item.type === "section") {
          ref = item.contents;
          results = [];
          for (i = 0, len = ref.length; i < len; i++) {
            item2 = ref[i];
            results.push(mapContent(table, item2));
          }
          return results;
        } else {
          return _this.columnMap[table.id + "::" + item.id] = item;
        }
      };
    })(this);
    ref = this.tables;
    results = [];
    for (i = 0, len = ref.length; i < len; i++) {
      table = ref[i];
      this.tableMap[table.id] = table;
      results.push((function() {
        var j, len1, ref1, results1;
        ref1 = table.contents;
        results1 = [];
        for (j = 0, len1 = ref1.length; j < len1; j++) {
          item = ref1[j];
          results1.push(mapContent(table, item));
        }
        return results1;
      })());
    }
    return results;
  };

  Schema.prototype.getTables = function() {
    return this.tables;
  };

  Schema.prototype.getTable = function(tableId) {
    return this.tableMap[tableId];
  };

  Schema.prototype.getColumn = function(tableId, columnId) {
    return this.columnMap[tableId + "::" + columnId];
  };

  Schema.prototype.getColumns = function(tableId) {
    var columns, i, item, len, ref, searchContent, table;
    columns = [];
    searchContent = (function(_this) {
      return function(item) {
        var i, item2, len, ref, results;
        if (item.type === "section") {
          ref = item.contents;
          results = [];
          for (i = 0, len = ref.length; i < len; i++) {
            item2 = ref[i];
            results.push(searchContent(item2));
          }
          return results;
        } else {
          return columns.push(item);
        }
      };
    })(this);
    table = this.getTable(tableId);
    ref = table.contents;
    for (i = 0, len = ref.length; i < len; i++) {
      item = ref[i];
      searchContent(item);
    }
    return columns;
  };

  Schema.prototype.addTable = function(table) {
    var tables;
    tables = _.filter(this.tables, function(t) {
      return t.id !== table.id;
    });
    tables.push(table);
    return new Schema({
      tables: tables
    });
  };

  Schema.prototype.getNamedExprs = function(tableId) {
    return [];
  };

  Schema.prototype.stripIdColumns = function(table) {
    var stripIdColumnsFromContents;
    stripIdColumnsFromContents = function(contents) {
      var i, item, len, output;
      output = [];
      for (i = 0, len = contents.length; i < len; i++) {
        item = contents[i];
        if (item.type !== "section" && item.type !== "id") {
          output.push(item);
        } else if (item.type === "section") {
          output.push(_.extend(item, {
            contents: stripIdColumnsFromContents(item.contents)
          }));
        }
      }
      return output;
    };
    return _.extend(table, {
      contents: stripIdColumnsFromContents(table.contents)
    });
  };

  return Schema;

})();
