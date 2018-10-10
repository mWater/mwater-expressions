export interface LocalizedString {
  _base: string,
  [language: string]: string  // Localizations
}

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

export type LiteralType = "id" | "text" | "number" | "enum" | "enumset" | "boolean" | "date" | "datetime" | "geometry" | "text[]" | "image" | "imagelist"

export interface Column {
  /** table-unique id of item */
  id: string

  /** localized name of item */
  name: LocalizedString
  
  /** localized description of item */
  desc?: LocalizedString
  
  /**  optional non-localized code of item */
  code?: string
  
  /** type of content item. `id`, `text`, `number`, `enum`, `enumset`, `boolean`, `date`, `datetime`, `geometry`, `text[]`, `image`, `imagelist`, `join`, `section`, `expr`. */
  type: LiteralType | "join" | "section" | "expr"
  
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

/** Expression. Can be null */
export type Expr = LiteralExpr | FieldExpr | OpExpr | IdExpr | ScalarExpr | VariableExpr | null

export interface LiteralExpr {
  type: "literal"
  valueType: string
  idTable?: string
  value: any  
}

export interface IdExpr {
  type: "id"
  table: string
}

export interface FieldExpr {
  type: "field"
  table: string
  column: string
}

export interface OpExpr {
  type: "op"
  table: string
  op: string
  exprs: Expr[]
}

/** Expression that references a variable */
export interface VariableExpr {
  type: "variable"

  /** Table of expression that variable references (if relevant) */
  table?: string

  variableId: string
}

/** Variable that is referenced in an expression */
export interface Variable {
  /** Unique id of the variable */
  id: string

  /** Localized name of the variable */
  name: LocalizedString

  /** Localized description of the variable */
  desc?: LocalizedString

  /** Type of the value of the variable */
  type: LiteralType

  /** Table that variable expression is for. If present, is a non-literal variable and the value is an expression. Note: must be non-aggregate */
  table?: string

  /** For enum and enumset variables */
  enumValues?: EnumValue[]
  
  /** table for id, id[] fields */
  idTable?: string
}

export interface ScalarExpr {
  type: "scalar"

  /** Table id of start table */
  table: string

  /** Array of join columns to follow to get to table of expr. All must be `join` type */
  joins: string[]
  
  /** Expression from final table to get value */
  expr: Expr
}

export interface JsonQL {
  type: string
  [other: string]: any
}

export interface JsonQLQuery {
  type: "query"
  selects: JsonQLSelect[]
  from: JsonQLFrom
  where: JsonQLExpr
  orderBy: any // TODO
  groupBy: any // TODO
  limit?: number
}

export interface JsonQLExpr {
  // TODO
  type: string
  [other: string]: any
}

export type JsonQLFrom = JsonQLTableFrom

export interface JsonQLTableFrom {
  type: "table"
  table: string
  alias: string
}

export interface JsonQLSelect {
  expr: Expr
  alias: string
}

export interface Row {
  [alias: string]: any 
}
  
export class DataSource {
  /** Performs a single query. Calls cb with (error, rows) */
  performQuery(query: JsonQL, cb: (error: any, rows: Row[]) => void): void

  /** Get the url to download an image (by id from an image or imagelist column)
    Height, if specified, is minimum height needed. May return larger image
    Can be used to upload by posting to this url
  */
  getImageUrl(imageId: string, height: number): string

  // # Clears the cache if possible with this data source
  // clearCache: ->
  //   throw new Error("Not implemented")

  // # Get the cache expiry time in ms from epoch. No cached items before this time will be used. 0 for no cache limit.
  // # Useful for knowing when cache has been cleared, as it will be set to time of clearing.
  // getCacheExpiry: -> 
  //   throw new Error("Not implemented")
}

export type AggrStatus = "individual" | "literal" | "aggregate"

export class ExprUtils {
  constructor(schema: Schema)

  summarizeExpr(expr: Expr, locale?: string): string

  getExprType(expr: Expr): string | null

  getExprAggrStatus(expr: Expr): AggrStatus | null

  getExprEnumValues(expr: Expr): EnumValue[] | null

  /** Converts a literal value related to an expression to a string, using name of enums. preferEnumCodes tries to use code over name */
  stringifyExprLiteral(expr: Expr, literal: any, locale?: string, preferEnumCodes?: boolean): string
}

/** Validates expressions. If an expression has been cleaned, it will always be valid */
export class ExprValidator {
  constructor(schema: Schema, variables?: Variable[])

  /** Validates an expression, returning null if it is valid, otherwise return an error string
   * options are:
   *   table: optional current table. expression must be related to this table or will be stripped
   *   types: optional types to limit to
   *   enumValueIds: ids of enum values that are valid if type is enum
   *   idTable: table that type of id must be from
   *   aggrStatuses: statuses of aggregation to allow. list of "individual", "literal", "aggregate". Default: ["individual", "literal"]
   */
  validateExpr(expr: Expr, options: { table?: string, types?: string[], enumValueIds?: string[], idTable?: string, aggrStatuses?: AggrStatus[] }): string | null
}

export class ExprCompiler {
  constructor(schema: Schema, variables?: Variable[], variableValues?: { [variableId: string]: any })

  compileExpr(options: { expr: Expr, tableAlias: string }): JsonQLExpr
  compileTable(table: string, alias: string): JsonQLFrom
}

/** a row is a plain object that has the following functions as properties */
export interface ExprEvaluatorRow {
  /** gets primary key of row. callback is called with (error, value) */
  getPrimaryKey(callback: (error: any, value?: any) => void): void

  /** gets the value of a column. callback is called with (error, value) 
   * For joins, getField will get array of rows for 1-n and n-n joins and a row for n-1 and 1-1 joins
   */
  getField(columnId: string, callback: (error: any, value?: any) => void): void
}

export interface ExprEvaluatorContext {
  /** current row. Optional for aggr expressions */
  row?: ExprEvaluatorRow
  /** array of rows (for aggregate expressions) */
  rows?: ExprEvaluatorRow[]
}

export class ExprEvaluator {
  constructor(schema: Schema, locale?: string, variables?: Variable[], variableValues?: { [variableId: string]: any })
  evaluate(expr: Expr, context: ExprEvaluatorContext, callback: (error: any, value?: any) => void): void
}
