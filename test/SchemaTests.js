import { assert } from 'chai';
import _ from 'lodash';
import { default as Schema } from '../src/Schema';
import canonical from 'canonical-json';

const compare = (actual, expected) => assert.equal(canonical(actual), canonical(expected));

describe("Schema", function() {
  it("adds and gets tables", function() {
    let schema = new Schema();
    schema = schema.addTable({ id: "a", name: "A", desc: "a table", contents: [{ id: "x", name: "X", type: "text" }] });
    assert.equal(schema.getTables()[0].id, "a");
    assert.equal(schema.getTables()[0].name, "A");
    assert.equal(schema.getTables()[0].desc, "a table");

    assert.equal(schema.getTable("a").name, "A");
    return assert.equal(schema.getColumn("a", "x").name, "X");
  });

  it("loads from JSON", function() {
    const schema = new Schema({
      tables: [{
        id: "a",
        name: "A",
        contents: [
          {
            id: "x",
            name: "X",
            type: "text"
          }
        ]
      }]
    });

    return assert.equal(schema.getColumn("a", "x").name, "X");
  });

  it("saves to JSON", function() {
    const schema = new Schema({
      tables: [{
        id: "a",
        name: "A",
        contents: [
          {
            id: "x",
            name: "X",
            type: "text"
          }
        ]
      }]
    });

    return assert.equal(JSON.stringify(schema.toJSON()), JSON.stringify({
      tables: [{
        id: "a",
        name: "A",
        contents: [
          {
            id: "x",
            name: "X",
            type: "text"
          }
        ]
      }]
    })
    );
  });

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

  return it("loads from JSON object with sections", function() {
    const schema = new Schema({
      tables: [{
        id: "a",
        name: "A",
        contents: [
          { 
            type: "section",
            name: "S1",
            contents: [
              {
                id: "x",
                name: "X",
                type: "text"
              }
            ]
          }
        ]
      }]
    });

    assert.equal(schema.getColumn("a", "x").name, "X");
    assert.equal(schema.getColumns("a").length, 1);
    return assert.equal(schema.getColumns("a")[0].name, "X");
  });
});