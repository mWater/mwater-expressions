Schema = require '../src/Schema'

exports.simpleSchema = ->
  schema = new Schema()
  schema = schema.addTable({ id: "t1", name: { en: "T1" }, primaryKey: "primary", contents: [
    { id: "text", name: { en: "Text" }, type: "text" }
    { id: "number", name: { en: "Number" }, type: "number" }
    { id: "enum", name: { en: "Enum" }, type: "enum", enumValues: [{ id: "a", name: "A"}, { id: "b", name: "B"}] }
    { id: "enumset", name: { en: "EnumSet" }, type: "enumset", enumValues: [{ id: "a", name: "A"}, { id: "b", name: "B"}] }
    { id: "date", name: { en: "Date" }, type: "date" }
    { id: "datetime", name: { en: "Datetime" }, type: "datetime" }
    { id: "boolean", name: { en: "Boolean" }, type: "boolean" }
    { id: "geometry", name: { en: "Geometry" }, type: "geometry" }
    { id: "text[]", name: { en: "Text[]" }, type: "text[]" }
    { id: "1-2", name: { en: "T1->T2" }, type: "join", join: { type: "1-n", toTable: "t2", fromColumn: "primary", toColumn: "t1" }}
  ]})

  schema = schema.addTable({ id: "t2", name: { en: "T2" }, primaryKey: "primary", ordering: "number", contents: [
    { id: "t1", name: { en: "T1" }, type: "uuid" }
    { id: "text", name: { en: "Text" }, type: "text" }
    { id: "number", name: { en: "Number" }, type: "number" }
    { id: "2-1", name: { en: "T2->T1" }, type: "join", join: { type: "n-1", toTable: "t1", fromColumn: "t1", toColumn: "primary" }}
  ]})

  return schema