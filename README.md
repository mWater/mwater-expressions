# Expressions

Defines a language for expressions that return a value for a single row of a table. Also gives a React-based UI for editing expressions.

Complex expressions involving joins, arithmetic or case statements can be created visually and then compiled to SQL or interpreted.

See the [[Wiki|Home]] for more information

## Expression types

### Scalar expressions

Gets a single value given a row of a table.

`type`: "scalar"
`table`: Table id of start table
`joins`: Array of join columns to follow to get to table of expr. All must be `join` type
`expr`: Expression from final table to get value
`aggr`: Aggregation function to use if any join is multiple, null/undefined if not needed
`where`: optional logical expression to filter aggregation

#### Aggr values

aggr: "last", "sum", "count", "max", "min", "stdev", "stdevp"

### Field expressions 

Column of the database

`type`: "field"
`table`: Table id of table
`column`: Column id of column

### Logical expression

`type`: "logical"
`table`: Table id of table
`op`: `and` or `or`
`exprs`: expressions to combine. Either `logical` for nested conditions or `comparison`

### Comparison expressions

`type`: "comparison"
`table`: Table id of table 
`lhs`: left hand side expression. `scalar` usually.
`op`: "=", ">", ">=", "<", "<=", "~*", ">", "<", "= true", "= false", "is null", "is not null", '= any', 'between'
`rhs`: right hand side expressions. `literal` usually.

### Literal expressions

`type`: "literal"
`valueType`: "text", "integer", "decimal", "boolean", "enum", "date", "enum[]", "text[]", "daterange", "datetimerange"
`value`: value of literal. date is ISO 8601 YYYY-MM-DD. daterange is `[YYYY-MM-DD, YYYY-MM-DD]` (min, max). datetimerange is `[min iso datetime, max iso datetime]`

## Count expressions

This represents a `count(sometable.*)` expression. It should be displayed as Number of tablename

`type`: "count"
`table`: Table id of table
