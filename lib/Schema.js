var H, React, Schema, _;

_ = require('lodash');

React = require('react');

H = React.DOM;

module.exports = Schema = (function() {
  function Schema(json) {
    var i, len, ref, table;
    this.tables = [];
    this.tableMap = {};
    this.columnMap = {};
    if (json) {
      this.tables = _.cloneDeep(json.tables);
      this.tables = _.map(this.tables, this.stripIdColumns);
      ref = this.tables;
      for (i = 0, len = ref.length; i < len; i++) {
        table = ref[i];
        this.tableMap[table.id] = table;
        this.columnMap[table.id] = this._indexTable(table);
      }
    }
  }

  Schema.prototype._indexTable = function(table) {
    var i, item, len, map, mapContent, ref;
    mapContent = (function(_this) {
      return function(t, map, item) {
        var i, item2, len, ref, results;
        if (item.type === "section") {
          ref = item.contents;
          results = [];
          for (i = 0, len = ref.length; i < len; i++) {
            item2 = ref[i];
            results.push(mapContent(t, map, item2));
          }
          return results;
        } else {
          return map[item.id] = item;
        }
      };
    })(this);
    map = {};
    ref = table.contents;
    for (i = 0, len = ref.length; i < len; i++) {
      item = ref[i];
      mapContent(table, map, item);
    }
    return map;
  };

  Schema.prototype.getTables = function() {
    return this.tables;
  };

  Schema.prototype.getTable = function(tableId) {
    return this.tableMap[tableId];
  };

  Schema.prototype.getColumn = function(tableId, columnId) {
    var map;
    map = this.columnMap[tableId];
    if (!map) {
      return null;
    }
    return map[columnId];
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
    var columnMap, schema, tableMap, tables;
    tables = _.filter(this.tables, function(t) {
      return t.id !== table.id;
    });
    tables.push(table);
    tableMap = _.clone(this.tableMap);
    tableMap[table.id] = table;
    columnMap = _.clone(this.columnMap);
    columnMap[table.id] = this._indexTable(table);
    schema = new Schema();
    schema.tables = tables;
    schema.tableMap = tableMap;
    schema.columnMap = columnMap;
    return schema;
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
