"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const lodash_1 = __importDefault(require("lodash"));
const _1 = require(".");
/** Schema for a database. Immutable.
 * Stores tables with columns (possibly in nested sections).
 * See docs/Schema-and-Types for details of format
 */
class Schema {
    constructor(schemaJson) {
        this.tables = [];
        this.tableMap = {};
        this.columnMaps = {};
        if (schemaJson) {
            this.tables = schemaJson.tables;
            // Setup maps
            for (let table of this.tables) {
                this.tableMap[table.id] = table;
                this.columnMaps[table.id] = this.indexTable(table);
            }
        }
    }
    indexTable(table) {
        return lodash_1.default.indexBy((0, _1.flattenContents)(table.contents), (c) => c.id);
    }
    getTables() {
        return this.tables;
    }
    getTable(tableId) {
        return this.tableMap[tableId] || null;
    }
    getColumn(tableId, columnId) {
        const map = this.columnMaps[tableId];
        if (!map) {
            return null;
        }
        return map[columnId] || null;
    }
    /** Gets the columns in order, flattened out from sections */
    getColumns(tableId) {
        return (0, _1.flattenContents)(this.getTable(tableId).contents);
    }
    /** Add table with id, name, desc, primaryKey, ordering (column with natural order) and contents (array of columns/sections)
     * Will replace table if already exists.
     * schemas are immutable, so returns a fresh copy */
    addTable(table) {
        // Remove existing and add new
        const tables = lodash_1.default.filter(this.tables, (t) => t.id !== table.id);
        tables.push(table);
        // Update table map
        const tableMap = lodash_1.default.clone(this.tableMap);
        tableMap[table.id] = table;
        // Update column map
        const columnMaps = lodash_1.default.clone(this.columnMaps);
        columnMaps[table.id] = this.indexTable(table);
        const schema = new Schema();
        schema.tables = tables;
        schema.tableMap = tableMap;
        schema.columnMaps = columnMaps;
        return schema;
    }
    // Convert to a JSON
    toJSON() {
        return { tables: this.tables };
    }
}
exports.default = Schema;
