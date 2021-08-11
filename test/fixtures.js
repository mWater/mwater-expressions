// TODO: This file was created by bulk-decaffeinate.
// Sanity-check the conversion and remove this comment.
import { default as Schema } from '../src/Schema';

export function simpleSchema() {
  let schema = new Schema();
  schema = schema.addTable({ id: "t1", name: { en: "T1" }, primaryKey: "primary", ordering: "ordering", contents: [
    { id: "text", name: { en: "Text" }, type: "text" },
    { id: "number", name: { en: "Number" }, type: "number" },
    { id: "enum", name: { en: "Enum" }, type: "enum", enumValues: [{ id: "a", name: { en: "A" }}, { id: "b", name: { en: "B" }}] },
    { id: "enumset", name: { en: "EnumSet" }, type: "enumset", enumValues: [{ id: "a", name: { en: "A" }}, { id: "b", name: { en: "B" }}] },
    { id: "date", name: { en: "Date" }, type: "date" },
    { id: "datetime", name: { en: "Datetime" }, type: "datetime" },
    { id: "boolean", name: { en: "Boolean" }, type: "boolean" },
    { id: "geometry", name: { en: "Geometry" }, type: "geometry" },
    { id: "text[]", name: { en: "Text[]" }, type: "text[]" },
    { id: "1-2", name: { en: "T1->T2" }, type: "join", join: { type: "1-n", toTable: "t2", fromColumn: "primary", toColumn: "t1" }},
    { id: "id", name: { en: "Id" }, type: "id", idTable: "t2" },
    { id: "id[]", name: { en: "Id[]" }, type: "id[]", idTable: "t2" },
    { id: "ordering", name: { en: "Ordering"}, type: "number" },

    // Expressions
    { id: "expr_enum", name: { en: "Expr Enum"}, type: "enum", expr: { type: "field", table: "t1", column: "enum" }, enumValues: [{ id: "a", name: { en: "A" }}, { id: "b", name: { en: "B" }}] },
    { id: "expr_number", name: { en: "Expr Number"}, type: "number", expr: { type: "field", table: "t1", column: "number" } },
    { id: "expr_id", name: { en: "Expr Id"}, type: "id", idTable: "t1", expr: { type: "id", table: "t1" } },
    { id: "expr_sum", name: { en: "Expr Sum"}, type: "number", expr: { type: "op", op: "sum", exprs: [{ type: "field", table: "t1", column: "number" }] }}
  ]});

  schema = schema.addTable({ id: "t2", name: { en: "T2" }, primaryKey: "primary", ordering: "number", contents: [
    { id: "text", name: { en: "Text" }, type: "text" },
    { id: "number", name: { en: "Number" }, type: "number" },
    { id: "geometry", name: { en: "Geometry" }, type: "geometry" },
    { id: "boolean", name: { en: "Boolean" }, type: "boolean" },
    { id: "id", name: { en: "Id" }, type: "id", idTable: "t1" },
    { id: "2-1", name: { en: "T2->T1" }, type: "join", join: { type: "n-1", toTable: "t1", fromColumn: "t1", toColumn: "primary" }}
  ]});

  // Hierarchical table (since has ancestryTable)
  schema = schema.addTable({ id: "thier", name: { en: "THier" }, primaryKey: "primary", ordering: "number", ancestryTable: "thier_ancestry", contents: [
    { id: "text", name: { en: "Text" }, type: "text" },
    { id: "number", name: { en: "Number" }, type: "number" },
    { id: "2-1", name: { en: "T2->T1" }, type: "join", join: { type: "n-1", toTable: "t1", fromColumn: "t1", toColumn: "primary" }}
  ]});

  schema = schema.addTable({ id: "t3", name: { en: "T3" }, primaryKey: "primary", contents: [
    { id: "3-2", name: { en: "T3->T2" }, type: "join", join: { type: "n-1", toTable: "t2", fromColumn: "t2", toColumn: "primary" }}
  ]});

  return schema;
}