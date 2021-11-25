import { default as Schema } from "../src/Schema"

export function simpleSchema() {
  let schema = new Schema()
  schema = schema.addTable({
    id: "t1",
    name: { _base: "en", en: "T1" },
    primaryKey: "primary",
    ordering: "ordering",
    contents: [
      { id: "text", name: { _base: "en", en: "Text" }, type: "text" },
      { id: "number", name: { _base: "en", en: "Number" }, type: "number" },
      {
        id: "enum",
        name: { _base: "en", en: "Enum" },
        type: "enum",
        enumValues: [
          { id: "a", name: { _base: "en", en: "A" } },
          { id: "b", name: { _base: "en", en: "B" } }
        ]
      },
      {
        id: "enumset",
        name: { _base: "en", en: "EnumSet" },
        type: "enumset",
        enumValues: [
          { id: "a", name: { _base: "en", en: "A" } },
          { id: "b", name: { _base: "en", en: "B" } }
        ]
      },
      { id: "date", name: { _base: "en", en: "Date" }, type: "date" },
      { id: "datetime", name: { _base: "en", en: "Datetime" }, type: "datetime" },
      { id: "boolean", name: { _base: "en", en: "Boolean" }, type: "boolean" },
      { id: "geometry", name: { _base: "en", en: "Geometry" }, type: "geometry" },
      { id: "text[]", name: { _base: "en", en: "Text[]" }, type: "text[]" },
      {
        id: "1-2",
        name: { _base: "en", en: "T1->T2" },
        type: "join",
        join: { type: "1-n", toTable: "t2", fromColumn: "primary", toColumn: "t1" }
      },
      { id: "id", name: { _base: "en", en: "Id" }, type: "id", idTable: "t2" },
      { id: "id[]", name: { _base: "en", en: "Id[]" }, type: "id[]", idTable: "t2" },
      { id: "ordering", name: { _base: "en", en: "Ordering" }, type: "number" },

      // Expressions
      {
        id: "expr_enum",
        name: { _base: "en", en: "Expr Enum" },
        type: "enum",
        expr: { type: "field", table: "t1", column: "enum" },
        enumValues: [
          { id: "a", name: { _base: "en", en: "A" } },
          { id: "b", name: { _base: "en", en: "B" } }
        ]
      },
      {
        id: "expr_number",
        name: { _base: "en", en: "Expr Number" },
        type: "number",
        expr: { type: "field", table: "t1", column: "number" }
      },
      { id: "expr_id", name: { _base: "en", en: "Expr Id" }, type: "id", idTable: "t1", expr: { type: "id", table: "t1" } },
      {
        id: "expr_sum",
        name: { _base: "en", en: "Expr Sum" },
        type: "number",
        expr: { type: "op", op: "sum", exprs: [{ type: "field", table: "t1", column: "number" }] }
      }
    ]
  })

  schema = schema.addTable({
    id: "t2",
    name: { _base: "en", en: "T2" },
    primaryKey: "primary",
    ordering: "number",
    contents: [
      { id: "text", name: { _base: "en", en: "Text" }, type: "text" },
      { id: "number", name: { _base: "en", en: "Number" }, type: "number" },
      { id: "geometry", name: { _base: "en", en: "Geometry" }, type: "geometry" },
      { id: "boolean", name: { _base: "en", en: "Boolean" }, type: "boolean" },
      { id: "id", name: { _base: "en", en: "Id" }, type: "id", idTable: "t1" },
      {
        id: "2-1",
        name: { _base: "en", en: "T2->T1" },
        type: "join",
        join: { type: "n-1", toTable: "t1", fromColumn: "t1", toColumn: "primary" }
      }
    ]
  })

  // Hierarchical table (since has ancestryTable)
  schema = schema.addTable({
    id: "thier",
    name: { _base: "en", en: "THier" },
    primaryKey: "primary",
    ordering: "number",
    ancestryTable: "thier_ancestry",
    contents: [
      { id: "text", name: { _base: "en", en: "Text" }, type: "text" },
      { id: "number", name: { _base: "en", en: "Number" }, type: "number" },
      {
        id: "2-1",
        name: { _base: "en", en: "T2->T1" },
        type: "join",
        join: { type: "n-1", toTable: "t1", fromColumn: "t1", toColumn: "primary" }
      }
    ]
  })

  schema = schema.addTable({
    id: "t3",
    name: { _base: "en", en: "T3" },
    primaryKey: "primary",
    contents: [
      {
        id: "3-2",
        name: { _base: "en", en: "T3->T2" },
        type: "join",
        join: { type: "n-1", toTable: "t2", fromColumn: "t2", toColumn: "primary" }
      }
    ]
  })

  return schema
}
