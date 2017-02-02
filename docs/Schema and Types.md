The schema defines the tables in the database and their columns. 
## JSON representation

The schema is serializable as JSON as follows:

`{ tables: <array of table> }`

Localized strings are stored either as raw string or as `{ "en": "English name", "fr": "French", ... "_base": "en" }`

_base is optional.

### table

`id`: unique id of table

`name`: localized name of table

`desc`: localized description of table (optional)

`code`: non-localized short code for a table (optional)

`primaryKey`: column with primary key (optional). Can be JsonQL expression with `{alias}` for table alias 

`ordering`: column with natural ordering (optional). Can be JsonQL expression with `{alias}` for table alias

`ancestry`: column with jsonb array of primary keys, including self. Makes table hierarchical.

`label`: column with label when choosing a single row. Can be JsonQL expression with `{alias}` for table alias

`contents`: array of content items (columns, sections and joins) of the table

`deprecated`: true if table is deprecated. Do not show unless already selected

`jsonql`: Optional custom JsonQL expression. This allows a simple table to be translated to an arbitrarily complex JsonQL expression before being sent to the server. 

`sql`: sql expression that gets the table. Usually just name of the table. *Note*: this is only for when sharing a schema file with [LookupSchemaMap](https://github.com/mWater/jsonql/blob/master/src/LookupSchemaMap.coffee)

### content

Either a section, join or column. 

`id`: table-unique id of item. sections do not need id (TODO good idea?)

`name`: localized name of item

`desc`: localized description of item

`code`: optional non-localized code of item

`type`: type of content item. `id`, `text`, `number`, `enum`, `enumset`, `boolean`, `date`, `datetime`, `geometry`, `text[]`, `image`, `imagelist`, `join`, `section`, `expr`.

`enumValues`: Values for enum. Array of { id, name, code }. For type `enum` or `enumset` only. `id` is the string value of the enum. `code` is optional non-localized code for enum value

`idTable`: table for id[] fields

`join`: Details of the join. See below. For type `join` only.

`deprecated`: true if column is deprecated. Do not show unless already selected

`jsonql`: Optional custom JsonQL expression. This allows a simple column to be translated to an arbitrarily complex JsonQL expresion before being sent to the server. It will have any fields with tableAlias = `{alias}` replaced by the appropriate alias. For all except `join`, `section` and `expr`

`sql`: sql expression that gets the column value. Uses `{alias}` which will be substituted with the table alias. Usually just `{alias}.some_column_name`. *Note*: this is only for when sharing a schema file with [LookupSchemaMap](https://github.com/mWater/jsonql/blob/master/src/LookupSchemaMap.coffee)

## Column types

joins and columns can be nested within sections for organizational purposes.

* `id`: an id column. Ignored. *Note*: this is only for when sharing a schema file with [LookupSchemaMap](https://github.com/mWater/jsonql/blob/master/src/LookupSchemaMap.coffee)
* `text`: strings. e.g. "apple"
* `number`: e.g 1.34, 2, 5
* `boolean`: true or false
* `enum`: fixed set of values with localized names. See values definition
* `enumset`: set (unordered) of enum values
* `geometry`: geometry column. Database-specific, but should be GeoJSON when queried as JSON.
* `date`: date stored as ISO 8601 (e.g. "2015-12-31")
* `datetime`: timestamp stored as ISO 8601 (e.g. "2015-12-31T02:04:31Z") 
* `text[]`: ordered array of text values
* `image`: TODO
* `imagelist`: TODO
* `id[]`: array of primary keys of another table. Include `idTable` field

Special column types:
* `join`: not a columns per se, but link to one or N rows in another table
* `section`: has contents which is list of other columns/sections
* `expr`: has `expr` field which is an expression to be evaluated. Can be aggregate or individual expression

### enumValues

Enum values are represented by an array of objects e.g. `{ id: some id, code: optional non-localized code, name: some name, desc: optional description localized }`. `id` should be a string. `name` is a string label of the enum value

### join

`type`: "1-n", "n-1", "n-n" or "1-1"
`toTable`: table to end join at

`jsonql`: jsonql expression with aliases {from} and {to}

`-or-`

`fromColumn`: table column to start join from or jsonql with alias {alias}
`toColumn`: table column to end join at or jsonql with alias {alias}. 


## Example format
Written in yaml:

```
tables:
  - id: tablexyz
    name: Table XYZ
    primaryKey: id  # column name of primary key
    sql: "(select * from tablexyz)" # Optional override on sql to get the column value. Most cases not needed
    contents:
      - type: section
        name: Section X
        contents: 
          - type: text
            id: program_name
            name: Program Name
          - type: integer
            id: num_enrolled
            name: Number of people enrolled in program  # Comments go here after hash
            sql: "{alias}.number_enrolled" # Optional override on sql to get the column value
          - type: enum
            id: program_country
            name: Country of Program
            enumValues:
              - { id: "india", name: "India" }
              - { id: "canada", name: "Canada" }
          - type: join
            id: division
            name: Division of Program
            join:
              type: "1-n"
              toTable: divisions
              fromColumn: division_id # This is the column in this table that it refers to
              toColumn: id  # Refered to field

      - type: section
        name: Section Y
        contents: 
          - type: text
            id: program_desc
            name: Program Desciption
          # And so on...

  - id: tableabc
    name: Table ABC
    primaryKey: id  # column name of primary key
    # And so on...

```
