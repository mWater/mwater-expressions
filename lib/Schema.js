"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
var lodash_1 = __importDefault(require("lodash"));
var _1 = require(".");
/** Schema for a database. Immutable.
 * Stores tables with columns (possibly in nested sections).
 * See docs/Schema-and-Types for details of format
 */
var Schema = /** @class */ (function () {
    function Schema(schemaJson) {
        this.tables = [];
        this.tableMap = {};
        this.columnMaps = {};
        if (schemaJson) {
            this.tables = schemaJson.tables;
            // Setup maps
            for (var _i = 0, _a = this.tables; _i < _a.length; _i++) {
                var table = _a[_i];
                this.tableMap[table.id] = table;
                this.columnMaps[table.id] = this.indexTable(table);
            }
        }
    }
    Schema.prototype.indexTable = function (table) {
        return lodash_1.default.indexBy(_1.flattenContents(table.contents), function (c) { return c.id; });
    };
    Schema.prototype.getTables = function () { return this.tables; };
    Schema.prototype.getTable = function (tableId) {
        return this.tableMap[tableId] || null;
    };
    Schema.prototype.getColumn = function (tableId, columnId) {
        var map = this.columnMaps[tableId];
        if (!map) {
            return null;
        }
        return map[columnId] || null;
    };
    /** Gets the columns in order, flattened out from sections */
    Schema.prototype.getColumns = function (tableId) {
        return _1.flattenContents(this.getTable(tableId).contents);
    };
    /** Add table with id, name, desc, primaryKey, ordering (column with natural order) and contents (array of columns/sections)
     * Will replace table if already exists.
     * schemas are immutable, so returns a fresh copy */
    Schema.prototype.addTable = function (table) {
        // Remove existing and add new
        var tables = lodash_1.default.filter(this.tables, function (t) { return t.id !== table.id; });
        tables.push(table);
        // Update table map
        var tableMap = lodash_1.default.clone(this.tableMap);
        tableMap[table.id] = table;
        // Update column map
        var columnMaps = lodash_1.default.clone(this.columnMaps);
        columnMaps[table.id] = this.indexTable(table);
        var schema = new Schema();
        schema.tables = tables;
        schema.tableMap = tableMap;
        schema.columnMaps = columnMaps;
        return schema;
    };
    // Convert to a JSON 
    Schema.prototype.toJSON = function () {
        return { tables: this.tables };
    };
    return Schema;
}());
exports.default = Schema;
