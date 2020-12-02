import { SchemaJson, Table, Column } from "./types";
declare type ColumnMap = {
    [columnId: string]: Column;
};
/** Schema for a database. Immutable.
 * Stores tables with columns (possibly in nested sections).
 * See docs/Schema-and-Types for details of format
 */
export default class Schema {
    tables: Table[];
    /** Map of table.id to table */
    tableMap: {
        [tableId: string]: Table;
    };
    /** Map of "<tableid>" to map of { "<columnid>" to column } */
    columnMaps: {
        [tableId: string]: ColumnMap;
    };
    constructor(schemaJson?: SchemaJson);
    private indexTable;
    getTables(): Table[];
    getTable(tableId: string): Table | null;
    getColumn(tableId: string, columnId: string): Column | null;
    /** Gets the columns in order, flattened out from sections */
    getColumns(tableId: string): Column[];
    /** Add table with id, name, desc, primaryKey, ordering (column with natural order) and contents (array of columns/sections)
     * Will replace table if already exists.
     * schemas are immutable, so returns a fresh copy */
    addTable(table: Table): Schema;
    toJSON(): SchemaJson;
}
export {};
