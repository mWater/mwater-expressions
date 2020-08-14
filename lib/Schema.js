"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var Schema, _;

_ = require('lodash'); // Schema for a database. Immutable.
// Stores tables with columns (possibly in nested sections).
// See docs/Schema-and-Types for details of format

module.exports = Schema = /*#__PURE__*/function () {
  function Schema(json) {
    (0, _classCallCheck2["default"])(this, Schema);
    var i, len, ref, table;
    this.tables = []; // Map of table.id to table

    this.tableMap = {}; // Map of "<tableid>" to map of { "<columnid>" to column }

    this.columnMap = {};

    if (json) {
      this.tables = json.tables;
      ref = this.tables; // Setup maps

      for (i = 0, len = ref.length; i < len; i++) {
        table = ref[i];
        this.tableMap[table.id] = table;
        this.columnMap[table.id] = this._indexTable(table);
      }
    }
  }

  (0, _createClass2["default"])(Schema, [{
    key: "_indexTable",
    value: function _indexTable(table) {
      var i, item, len, map, _mapContent, ref;

      _mapContent = function mapContent(t, map, item) {
        var i, item2, len, ref, results; // Recurse for sections

        if (item.type === "section") {
          ref = item.contents;
          results = [];

          for (i = 0, len = ref.length; i < len; i++) {
            item2 = ref[i];
            results.push(_mapContent(t, map, item2));
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

        _mapContent(table, map, item);
      }

      return map;
    }
  }, {
    key: "getTables",
    value: function getTables() {
      return this.tables;
    }
  }, {
    key: "getTable",
    value: function getTable(tableId) {
      return this.tableMap[tableId];
    }
  }, {
    key: "getColumn",
    value: function getColumn(tableId, columnId) {
      var map;
      map = this.columnMap[tableId];

      if (!map) {
        return null;
      }

      return map[columnId];
    } // Gets the columns in order, flattened out from sections

  }, {
    key: "getColumns",
    value: function getColumns(tableId) {
      var columns, i, item, len, ref, _searchContent, table;

      columns = [];

      _searchContent = function searchContent(item) {
        var i, item2, len, ref, results; // Recurse for sections

        if (item.type === "section") {
          ref = item.contents;
          results = [];

          for (i = 0, len = ref.length; i < len; i++) {
            item2 = ref[i];
            results.push(_searchContent(item2));
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

        _searchContent(item);
      }

      return columns;
    } // Add table with id, name, desc, primaryKey, ordering (column with natural order) and contents (array of columns/sections)
    // Will replace table if already exists. 
    // schemas are immutable, so returns a fresh copy

  }, {
    key: "addTable",
    value: function addTable(table) {
      var columnMap, schema, tableMap, tables; // Remove existing and add new

      tables = _.filter(this.tables, function (t) {
        return t.id !== table.id;
      });
      tables.push(table); // Update table map

      tableMap = _.clone(this.tableMap);
      tableMap[table.id] = table; // Update column map

      columnMap = _.clone(this.columnMap);
      columnMap[table.id] = this._indexTable(table);
      schema = new Schema();
      schema.tables = tables;
      schema.tableMap = tableMap;
      schema.columnMap = columnMap;
      return schema;
    } // Convert to a JSON 

  }, {
    key: "toJSON",
    value: function toJSON() {
      return {
        tables: this.tables
      };
    }
  }]);
  return Schema;
}();