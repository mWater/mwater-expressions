// TODO: This file was created by bulk-decaffeinate.
// Sanity-check the conversion and remove this comment.
import { assert } from "chai"
import _ from "lodash"
import { default as Schema } from "../src/Schema"
import canonical from "canonical-json"

function compare(actual: any, expected: any) {
  assert.equal(canonical(actual), canonical(expected))
}

describe("Schema", function () {
  it("adds and gets tables", function () {
    let schema = new Schema()
    schema = schema.addTable({ id: "a", name: { _base: "en", en: "A" }, desc: { _base: "en", en: "a table" }, contents: [{ id: "x", name: { _base: "en", en: "X" }, type: "text" }] })
    assert.equal(schema.getTables()[0].id, "a")
    assert.deepEqual(schema.getTables()[0].name, { _base: "en", en: "A" })
    assert.deepEqual(schema.getTables()[0].desc, { _base: "en", en: "a table" })

    assert.deepEqual(schema.getTable("a")!.name, { _base: "en", en: "A" })
    assert.deepEqual(schema.getColumn("a", "x")!.name, { _base: "en", en: "X" })
  })

  it("loads from JSON", function () {
    const schema = new Schema({
      tables: [
        {
          id: "a",
          name: { _base: "en", en: "A" },
          contents: [
            {
              id: "x",
              name: { _base: "en", en: "X" },
              type: "text"
            }
          ]
        }
      ]
    })

    assert.deepEqual(schema.getColumn("a", "x")!.name, { _base: "en", en: "X" })
  })

  it("saves to JSON", function () {
    const schema = new Schema({
      tables: [
        {
          id: "a",
          name: { _base: "en", en: "A" },
          contents: [
            {
              id: "x",
              name: { _base: "en", en: "X" },
              type: "text"
            }
          ]
        }
      ]
    })

    assert.equal(
      JSON.stringify(schema.toJSON()),
      JSON.stringify({
        tables: [
          {
            id: "a",
            name: { _base: "en", en: "A" },
            contents: [
              {
                id: "x",
                name: { _base: "en", en: "X" },
                type: "text"
              }
            ]
          }
        ]
      })
    )
  })

  // it "skips id types", ->
  //   schema = new Schema({
  //     tables: [{
  //       id: "a"
  //       name: "A"
  //       contents: [
  //         {
  //           id: "id"
  //           name: "ID"
  //           type: "id"
  //         },
  //         {
  //           id: "x"
  //           name: "X"
  //           type: "text"
  //         }
  //       ]
  //     }]
  //   })

  //   assert not schema.getColumn("a", "id")

  return it("loads from JSON object with sections", function () {
    const schema = new Schema({
      tables: [
        {
          id: "a",
          name: { _base: "en", en: "A" },
          contents: [
            {
              type: "section",
              name: { _base: "en", en: "S1" },
              contents: [
                {
                  id: "x",
                  name: { _base: "en", en: "X" },
                  type: "text"
                }
              ]
            }
          ]
        }
      ]
    })

    assert.deepEqual(schema.getColumn("a", "x")!.name, { _base: "en", en: "X" })
    assert.equal(schema.getColumns("a").length, 1)
    assert.deepEqual(schema.getColumns("a")[0].name, { _base: "en", en: "X" })
  })
})
