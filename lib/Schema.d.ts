import { JsonQL } from "jsonql";
import { Expr } from "./Expr";
import { LocalizedString } from "./LocalizedString";

export interface Table {
  id: string

  /** localized name of table */
  name: LocalizedString

  /** localized description of table (optional) */
  desc?: LocalizedString
  
  /** non-localized short code for a table (optional) */
  code?: string
  
  /** column of database (not schema column) with primary key (optional). Can be JsonQL expression with `{alias}` for table alias  */
  primaryKey?: string | JsonQL
  
  /** column in schema with natural ordering (optional). */
  ordering?: string
  
  /** table with "ancestor" and "descendant". Faster than ancestry and ancestryText */
  ancestryTable?: string
  
  /** DEPRECATED: column with jsonb array of primary keys, including self. Makes table hierarchical. */
  ancestry?: string
  
  /** DEPRECATED: column with jsonb array of primary keys as JSON text, including self. Required if non-text primary keys for optimization purposes. */
  ancestryText?: string
  
  /** column with label when choosing a single row. Can be JsonQL expression with `{alias}` for table alias */
  label?: string | JsonQL
  
  /** array of content items (columns, sections and joins) of the table */
  contents: Array<Column | Section>
  
  /** true if table is deprecated. Do not show unless already selected */
  deprecated?: boolean
  
  /** Optional custom JsonQL expression. This allows a simple table to be translated to an arbitrarily complex JsonQL expression before being sent to the server.  */
  jsonql?: JsonQL
  
  /** sql expression that gets the table. Usually just name of the table. *Note*: this is only for when using a schema file for Water.org's visualization server */
  sql?: string
}

export interface EnumValue {
  id: string
  name: LocalizedString
  code?: string
}

/**
 * image: { id: id of image, caption: optional caption } 
 * imagelist: an array of images 
 * json: arbitrary json
 */
export type LiteralType = "text" | "number" | "enum" | "enumset" | "boolean" | "date" | "datetime" | "id" | "id[]" | "geometry" | "text[]" | "image" | "imagelist" | "json"

export interface Column {
  /** table-unique id of item */
  id: string

  /** localized name of item */
  name: LocalizedString
  
  /** localized description of item */
  desc?: LocalizedString
  
  /**  optional non-localized code of item */
  code?: string
  
  /** type of content item. Literal type or `join`, `expr`. `expr` is deprecated! */
  type: LiteralType | "join" | "expr"
  
  /**  Values for enum. Array of { id, name, code }. For type `enum` or `enumset` only. `id` is the string value of the enum. `code` is optional non-localized code for enum value */
  enumValues?: EnumValue[]
  
  /**  table for id, id[] fields */
  idTable?: string
  
  /**  Details of the join. See below. For type `join` only. */
  join?: Join
  
  /**  true if column is deprecated. Do not show unless already selected */
  deprecated?: boolean
  
  /**  set to expression if the column is an mwater-expression to be evaluated */
  expr?: Expr
  
  /**  true if column contains confidential data and should be not displayed by default */
  confidential?: boolean
  
  /**  true if column is redacted and might be blank or scrambled */
  redacted?: boolean
  
  /**  Optional custom JsonQL expression. This allows a simple column to be translated to an arbitrarily complex JsonQL expresion before being sent to the server. It will have any fields with tableAlias = `{alias}` replaced by the appropriate alias. For all except `join`, `section` and `expr` */
  jsonql?: JsonQL
  
  /**  sql expression that gets the column value. Uses `{alias}` which will be substituted with the table alias. Usually just `{alias}.some_column_name`. *Note*: this is only for when using a schema file for Water.org's visualization server */
  sql?: string
}

export interface Join {
  type: "1-n" | "n-1" | "n-n" | "1-1"
  /** Table which join is to */
  toTable: string
  /** Inverse join column id in the case of 1-n joins (but optionally for all joins) */
  inverse?: string
  /** jsonql expression with aliases {from} and {to} */
  jsonql?: JsonQL
  /** table column to start join from or jsonql with alias {alias} */
  fromColumn?: string | JsonQL
  /** table column to end join at or jsonql with alias {alias}.  */
  toColumn?: string | JsonQL
}

export interface Section {
  id?: string

  type: "section"

  name: LocalizedString

  contents: Array<Section | Column>
}

export interface SchemaJson {
  tables: Table[]
}

export class Schema {
  constructor(schemaJson?: SchemaJson)

  addTable(table: Table): Schema

  getTables(): Table[]

  getTable(tableId: string): Table | null

  getColumn(tableId: string, columnId: string): Column | null

  /** Gets the columns in order, flattened out from sections */
  getColumns(tableId: string): Column[]
}
