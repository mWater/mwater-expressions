assert = require('chai').assert
_ = require 'lodash'
Schema = require('../src/Schema').default
canonical = require 'canonical-json'

compare = (actual, expected) ->
  assert.equal canonical(actual), canonical(expected)

describe "Schema", ->
  it "adds and gets tables", ->
    schema = new Schema()
    schema = schema.addTable({ id: "a", name: "A", desc: "a table", contents: [{ id: "x", name: "X", type: "text" }] })
    assert.equal schema.getTables()[0].id, "a"
    assert.equal schema.getTables()[0].name, "A"
    assert.equal schema.getTables()[0].desc, "a table"

    assert.equal schema.getTable("a").name, "A"
    assert.equal schema.getColumn("a", "x").name, "X"

  it "loads from JSON", ->
    schema = new Schema({
      tables: [{
        id: "a"
        name: "A"
        contents: [
          {
            id: "x"
            name: "X"
            type: "text"
          }
        ]
      }]
    })

    assert.equal schema.getColumn("a", "x").name, "X"

  it "saves to JSON", ->
    schema = new Schema({
      tables: [{
        id: "a"
        name: "A"
        contents: [
          {
            id: "x"
            name: "X"
            type: "text"
          }
        ]
      }], 
      variables: [
        { id: "varnumber", name: { _base: "en", en: "Varnumber" }, type: "number" }
      ]
    })

    assert.equal JSON.stringify(schema.toJSON()), JSON.stringify({
      tables: [{
        id: "a"
        name: "A"
        contents: [
          {
            id: "x"
            name: "X"
            type: "text"
          }
        ]
      }],
      variables: [
        { id: "varnumber", name: { _base: "en", en: "Varnumber" }, type: "number" }
      ]
    })

  # it "skips id types", ->
  #   schema = new Schema({
  #     tables: [{
  #       id: "a"
  #       name: "A"
  #       contents: [
  #         { 
  #           id: "id"
  #           name: "ID"
  #           type: "id"
  #         },
  #         {
  #           id: "x"
  #           name: "X"
  #           type: "text"
  #         }
  #       ]
  #     }]
  #   })

  #   assert not schema.getColumn("a", "id")

  it "loads from JSON object with sections", ->
    schema = new Schema({
      tables: [{
        id: "a"
        name: "A"
        contents: [
          { 
            type: "section"
            name: "S1"
            contents: [
              {
                id: "x"
                name: "X"
                type: "text"
              }
            ]
          }
        ]
      }]
    })

    assert.equal schema.getColumn("a", "x").name, "X"
    assert.equal schema.getColumns("a").length, 1
    assert.equal schema.getColumns("a")[0].name, "X"