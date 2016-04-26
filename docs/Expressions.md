Expressions define how to get a single value from the context of the row of a table. It can be as simple as a field expression (column of the current row) to a series of joins and arithmetic to get a value.

Expressions are represented in JSON and can be compiled to [JsonQL](https://github.com/mWater/jsonql) using the `ExpressionCompiler`.

Expressions are always of the form `{ type: <type of expression>, ... }`

`null` mean no expression. `{}` is a placeholder for a mandatory expression to be filled in.

The types are:

- `field`: References a column in the current row
- `scalar`: Follows one or more joins and then applies the inner expression with an optional where clause and a aggregate function.
- `literal`: 123, "abc", etc.
- `id`: expression that represents the primary key of a table row
- `op`: Operation that takes expressions and an op.
- `case`: Series of boolean when/thens and an else

### field expressions 

Column of the database

- `type`: "field"
- `table`: Table id of table
- `column`: Column id of column

### scalar expressions

Gets a single value given a row of a table.

- `type`: "scalar"
- `table`: Table id of start table
- `joins`: Array of join columns to follow to get to table of expr. All must be `join` type
- `expr`: Expression from final table to get value
- `aggr`: Aggregation function to use if any join is multiple, null/undefined if not needed
- `where`: optional logical expression to filter aggregation

#### Aggr values

aggr: "last", "sum", "count", "max", "min", "stdev", "stdevp"

### op expression

- `type`: "op"
- `table`: Table id of table
- `op`: "and", "or", "=", ">", ">=", "<", "<=", "<>", "~*", ">", "<", "= false", "is null", "is not null", "= any", "between", "contains", 'thisyear', 'lastyear', 'thismonth', 'lastmonth', 'today', 'yesterday', 'last7days', 'last30days', 'last365days', 'distance' (distance between two geometries in meters), 'round', 'floor', 'ceiling'

- `exprs`: array of expressions to use for the op. Second and third, etc. are usually literal for all but "and" and "or"

### literal expressions

- `type`: "literal"
- `valueType`: "text", "number", "boolean", "enum", "date", "enumset", "text[]", "datetime", "id"
- `idTable`: id of table that id is for if id valueType = "literal"
- `value`: value of literal. date is ISO 8601 YYYY-MM-DD. datetime is ISO 8601 ending in Z

### case expressions

- `type`: "case"
- `table`: Table id of table
- `cases`: array of { when: boolean expression, then: result value }
- `else`: optional else if no cases match

### id expressions

This gets the id of the table:

- `type`: "id"
- `table`: Table id of table

It is used as the inner expression when trying to do a count(*), as it is logically equivalent to count(sometable.theprimarykey)

### score expressions

Scores an enum or enumset by assigning and summing the scores for each value.

- `type`: "score"
- `table`: Table id of table
- `input`: enum or enumset expression
- `scores`: map of enum/enumset id to score expression