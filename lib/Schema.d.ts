import { SchemaJson, Table, Column } from "./types";

/** Schema which describes tables and columns of a database */
export default class Schema {
  constructor(schemaJson?: SchemaJson)

  addTable(table: Table): Schema

  getTables(): Table[]

  getTable(tableId: string): Table | null

  getColumn(tableId: string, columnId: string): Column | null

  /** Gets the columns in order, flattened out from sections */
  getColumns(tableId: string): Column[]
}
