var Schema, _;

_ = require('lodash');

// Schema for a database. Immutable.
// Stores tables with columns (possibly in nested sections).
// See docs/Schema-and-Types for details of format
module.exports = Schema = class Schema {
  constructor(json) {
    var i, len, ref, table;
    this.tables = [];
    // Map of table.id to table
    this.tableMap = {};
    
    // Map of "<tableid>" to map of { "<columnid>" to column }
    this.columnMap = {};
    if (json) {
      this.tables = json.tables;
      ref = this.tables;
      // Setup maps
      for (i = 0, len = ref.length; i < len; i++) {
        table = ref[i];
        this.tableMap[table.id] = table;
        this.columnMap[table.id] = this._indexTable(table);
      }
    }
  }

  _indexTable(table) {
    var i, item, len, map, mapContent, ref;
    mapContent = (t, map, item) => {
      var i, item2, len, ref, results;
      // Recurse for sections
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
    map = {};
    ref = table.contents;
    for (i = 0, len = ref.length; i < len; i++) {
      item = ref[i];
      mapContent(table, map, item);
    }
    return map;
  }

  getTables() {
    return this.tables;
  }

  getTable(tableId) {
    return this.tableMap[tableId];
  }

  getColumn(tableId, columnId) {
    var map;
    map = this.columnMap[tableId];
    if (!map) {
      return null;
    }
    return map[columnId];
  }

  // Gets the columns in order, flattened out from sections
  getColumns(tableId) {
    var columns, i, item, len, ref, searchContent, table;
    columns = [];
    searchContent = (item) => {
      var i, item2, len, ref, results;
      // Recurse for sections
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
    table = this.getTable(tableId);
    ref = table.contents;
    for (i = 0, len = ref.length; i < len; i++) {
      item = ref[i];
      searchContent(item);
    }
    return columns;
  }

  // Add table with id, name, desc, primaryKey, ordering (column with natural order) and contents (array of columns/sections)
  // Will replace table if already exists. 
  // schemas are immutable, so returns a fresh copy
  addTable(table) {
    var columnMap, schema, tableMap, tables;
    // Remove existing and add new
    tables = _.filter(this.tables, function(t) {
      return t.id !== table.id;
    });
    tables.push(table);
    // Update table map
    tableMap = _.clone(this.tableMap);
    tableMap[table.id] = table;
    // Update column map
    columnMap = _.clone(this.columnMap);
    columnMap[table.id] = this._indexTable(table);
    schema = new Schema();
    schema.tables = tables;
    schema.tableMap = tableMap;
    schema.columnMap = columnMap;
    return schema;
  }

  // Convert to a JSON 
  toJSON() {
    return {
      tables: this.tables
    };
  }

};
