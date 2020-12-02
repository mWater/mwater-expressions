import _ from 'lodash'
import { flattenContents } from '.';
import { SchemaJson, Table, Column } from "./types";

type ColumnMap = { [columnId: string] : Column }

/** Schema for a database. Immutable.
 * Stores tables with columns (possibly in nested sections).
 * See docs/Schema-and-Types for details of format
 */
export default class Schema {
  tables: Table[]

  /** Map of table.id to table */
  tableMap: { [tableId: string]: Table }

  /** Map of "<tableid>" to map of { "<columnid>" to column } */
  columnMaps: { [tableId: string]: ColumnMap }

  constructor(schemaJson?: SchemaJson) {
    this.tables = []
    this.tableMap = {}
    this.columnMaps = {}

    if (schemaJson) {
      this.tables = schemaJson.tables

      // Setup maps
      for (let table of this.tables) {
        this.tableMap[table.id] = table
        this.columnMaps[table.id] = this.indexTable(table)
      }
    }
  }

  private indexTable(table: Table): ColumnMap {
    return _.indexBy(flattenContents(table.contents), c => c.id)
  }

  getTables() { return this.tables }

  getTable(tableId: string): Table | null { 
    return this.tableMap[tableId] || null 
  }

  getColumn(tableId: string, columnId: string): Column | null {
    const map = this.columnMaps[tableId]
    if (!map) {
      return null;
    }
    return map[columnId] || null
  }

  /** Gets the columns in order, flattened out from sections */
  getColumns(tableId: string) {
    return flattenContents(this.getTable(tableId)!.contents)
  }

  /** Add table with id, name, desc, primaryKey, ordering (column with natural order) and contents (array of columns/sections)
   * Will replace table if already exists. 
   * schemas are immutable, so returns a fresh copy */
  addTable(table: Table): Schema {
    // Remove existing and add new
    const tables = _.filter(this.tables, t => t.id !== table.id)
    tables.push(table)

    // Update table map
    const tableMap = _.clone(this.tableMap)
    tableMap[table.id] = table

    // Update column map
    const columnMaps = _.clone(this.columnMaps)
    columnMaps[table.id] = this.indexTable(table)

    const schema = new Schema();
    schema.tables = tables;
    schema.tableMap = tableMap;
    schema.columnMaps = columnMaps;

    return schema
  }

  // Convert to a JSON 
  toJSON(): SchemaJson {
    return { tables: this.tables }
  }
}
