Expressions define how to get a single value from the context of the row of a table. It can be as simple as a field expression (column of the current row) to a series of joins and arithmetic to get a value.

Expressions are represented in JSON and can be compiled to [JsonQL](https://github.com/mWater/jsonql) using the `ExpressionCompiler`.

Expressions are always of the form `{ type: <type of expression>, ... }`

`null` mean no expression. `{}` is a placeholder for a mandatory expression to be filled in.

Expressions can be aggregate (e.g. sum(...)) or individual. Aggregation status is the term for one of "literal", "individual" or "aggregate".

**NOTE**: See types.ts for a more complete list of expression types

The types are:

- `field`: References a column in the current row
- `scalar`: Follows one or more joins and then applies the inner expression with an optional where clause and a aggregate function.
- `literal`: 123, "abc", etc.
- `id`: expression that represents the primary key of a table row
- `op`: Operation that takes expressions and an op.
- `case`: Series of boolean when/thens and an else

### `field` expressions 

Column of the database

- `type`: "field"
- `table`: Table id of table
- `column`: Column id of column

### `scalar` expressions

Gets a single value given a row of a table.

- `type`: "scalar"
- `table`: Table id of start table
- `joins`: Array of join columns to follow to get to table of expr. All must be `join` type
- `expr`: Expression from final table to get value
- `aggr`: Aggregation function to use if any join is multiple, null/undefined if not needed (DEPRECATED: use aggr expressions in `expr`)
- `where`: optional logical expression to filter aggregation (DEPRECATED: use aggr expressions in `expr`)

#### Aggr values

aggr: "last", "sum", "count", "max", "min", "stdev", "stdevp"

### `op` expression

- `type`: "op"
- `table`: Table id of table
- `op`: See below
- `exprs`: array of expressions to use for the op. Second and third, etc. are usually literal for all but "and" and "or"

#### ops: 

`and`, `or`, `=`, `>`, `>=`, `<`, `<=`, `<>`, `~*`, `>`, `<`, `= false`, `is null`, `is not null`, `= any`, `between`, `contains`, `thisyear`, `lastyear`, `thismonth`, `lastmonth`, `today`, `yesterday`, `last7days`, `last30days`, `last365days`, `distance` (distance between two geometries in meters), `round`, `floor`, `ceiling`, 

`to text`: Convert enum to text. Extra property `locale` is optional

Aggregate ones: 

`last`, `sum`, `count`, `max`, `min`, `stdev`, `stdevp`

`count where`: aggregate that takes a boolean condition
`percent where`: aggregate that takes two boolean condition (filter and basis. resolves to percentage where [filter] of [basis])
`last where`: aggregate that takes any type expression and a boolean filter

### `literal` expressions

- `type`: "literal"
- `valueType`: "text", "number", "boolean", "enum", "date", "enumset", "text[]", "datetime", "id"
- `idTable`: id of table that id is for if id valueType = "literal"
- `value`: value of literal. date is ISO 8601 YYYY-MM-DD. datetime is ISO 8601 ending in Z

### `case` expressions

- `type`: "case"
- `table`: Table id of table
- `cases`: array of { when: boolean expression, then: result value }
- `else`: optional else if no cases match

### `id` expressions

This gets the id of the table:

- `type`: "id"
- `table`: Table id of table

### `score` expressions

Scores an enum or enumset by assigning and summing the scores for each value.

- `type`: "score"
- `table`: Table id of table
- `input`: enum or enumset expression
- `scores`: map of enum/enumset id to score expression

### `build enumset` expressions

Creates an enumset from a set of boolean expressions for each value

- `type`: "build enumset"
- `table`: Table id of table
- `values`: map of enumset id to boolean expression. If true, will be included
