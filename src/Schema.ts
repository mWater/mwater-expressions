import _ from 'lodash'
import { flattenContents } from '.';
import { SchemaJson, Table, Column, Variable } from "./types";

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

  /** Variables of the schema */
  variables: Variable[]

  constructor(schemaJson?: SchemaJson) {
    this.tables = []
    this.tableMap = {}
    this.columnMaps = {}
    this.variables = []

    if (schemaJson) {
      this.tables = schemaJson.tables

      // Setup maps
      for (let table of this.tables) {
        this.tableMap[table.id] = table
        this.columnMaps[table.id] = this.indexTable(table)
      }

      if (schemaJson.variables) {
        this.variables = schemaJson.variables
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

  getVariable(variableId: string): Variable | null {
    return this.variables.find(v => v.id == variableId) || null
  }

  getVariables(): Variable[] {
    return this.variables
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
    schema.variables = this.variables

    return schema
  }

  /** Adds a variable to the schema 
   * schemas are immutable, so returns a fresh copy */
  addVariable(variable: Variable): Schema {
   // Remove existing and add new
   const variables = _.filter(this.variables, v => v.id !== variable.id)
   variables.push(variable)

   const schema = new Schema();
   schema.tables = this.tables;
   schema.tableMap = this.tableMap;
   schema.columnMaps = this.columnMaps;
   schema.variables = variables

   return schema
 }

  // Convert to a JSON 
  toJSON(): SchemaJson {
    return { tables: this.tables, variables: this.variables }
  }
}
