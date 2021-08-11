// TODO: This file was created by bulk-decaffeinate.
// Sanity-check the conversion and remove this comment.
import { assert } from 'chai';
import _ from 'lodash';
import { default as Schema } from '../src/Schema';
import { default as ExprValidator } from '../src/ExprValidator';
import * as fixtures from './fixtures';
import { setupTestExtension } from './extensionSetup';
import canonical from 'canonical-json';

setupTestExtension();

const variables = [
  { id: "varenum", name: { _base: "en", en: "Varenum" }, type: "enum", enumValues: [{ id: "a", name: { en: "A" }}, { id: "b", name: { en: "B" }}] },
  { id: "varnumber", name: { _base: "en", en: "Varnumber" }, type: "number" },
  { id: "varnumberexpr", name: { _base: "en", en: "Varnumberexpr" }, type: "number", table: "t1" },
  { id: "varid", name: { _base: "en", en: "Varid" }, type: "id", idTable: "t1" }
];

describe("ExprValidator", function() {
  beforeEach(function() {
    this.schema = fixtures.simpleSchema();
    this.exprValidator = new ExprValidator(this.schema, variables);
    this.isValid = (expr, options) => {
      return assert.isNull(this.exprValidator.validateExpr(expr, options), "Expected to be valid");
    };

    return this.notValid = (expr, options) => {
      return assert(this.exprValidator.validateExpr(expr, options), "Expected to be invalid");
    };
  });

  it("invalid if wrong table", function() {
    return this.notValid({ type: "field", table: "t1", column: "text" }, {table: "t2"});
  });

  it("invalid if wrong type", function() {
    return this.notValid({ type: "field", table: "t1", column: "enum" }, {types: ["text"]});
  });

  it("invalid if wrong idTable", function() {
    const field = { type: "id", table: "t1" };
    this.isValid(field, {types: ["id"], idTable: "t1"});
    return this.notValid(field, {types: ["id"], idTable: "t2"});
  });

  it("invalid if wrong enums", function() {
    const field = { type: "field", table: "t1", column: "enum" };
    this.isValid(field, {enumValueIds: ["a", "b", "c"]});
    return this.notValid(field, {enumValueIds: ["a"]});
  });

  it("invalid if wrong enums expression", function() {
    const field = { type: "field", table: "t1", column: "expr_enum" };
    this.isValid(field, {enumValueIds: ["a", "b", "c"]});
    return this.notValid(field, {enumValueIds: ["a"]});
  });

  it("valid if ok", function() {
    return this.isValid({ type: "field", table: "t1", column: "text"});
  });

  it("invalid if missing field", function() {
    return this.notValid({ type: "field", table: "t1", column: "xyz"});
  });

  it("invalid if field expr invalid", function() {
    const table = this.schema.getTable("t1");
    table.contents.push(
      { id: "expr_invalid", name: { en: "Expr Invalid"}, type: "expr", expr: { type: "field", table: "t1", column: "xyz" }}
    );
    const schema = this.schema.addTable(table);

    const exprValidator = new ExprValidator(schema);
    return assert(exprValidator.validateExpr({ type: "field", table: "t1", column: "expr_invalid" }));
  });

  it("handles recursive field expr", function() {
    const table = this.schema.getTable("t1");
    table.contents.push(
      { id: "expr_recursive", name: { en: "Expr Recursive"}, type: "expr", expr: { type: "field", table: "t1", column: "expr_recursive" }}
    );
    const schema = this.schema.addTable(table);

    const exprValidator = new ExprValidator(schema);
    return assert(exprValidator.validateExpr({ type: "field", table: "t1", column: "expr_recursive" }));
  });

  describe("scalar", function() {
    it("valid", function() {
      const expr = { 
        type: "scalar",
        table: "t2",
        joins: ["2-1"],
        expr: { type: "field", table: "t1", column: "number" }
      };
      return this.isValid(expr);
    });

    it("bad join", function() {
      const expr = { 
        type: "scalar",
        table: "t2",
        joins: ["xyz"],
        expr: { type: "field", table: "t1", column: "number" }
      };
      return this.notValid(expr);
    });

    it("bad expr", function() {
      const expr = { 
        type: "scalar",
        table: "t2",
        joins: ["2-1"],
        expr: { type: "field", table: "t1", column: "xyz" }
      };
      return this.notValid(expr);
    });
    
    return it("valid aggr", function() {
      const expr = {
        type: "scalar",
        table: "t1",
        joins: ["1-2"],
        expr: { type: "op", table: "t2", op: "avg", exprs: [{ type: "field", table: "t2", column: "number" }] }
      };
      return this.isValid(expr);
    });
  });

  describe("op", function() {
    it("invalid if mixed aggregate and individual", function() {
      const expr = {
        type: "op",
        table: "t1",
        op: "+",
        exprs: [
          { type: "field", table: "t1", column: "number" },
          { type: "op", op: "sum", exprs: [{ type: "field", table: "t1", column: "number" }] }
        ]
      };
      return this.notValid(expr, { aggrStatuses: ["individual", "literal", "aggregate"] });
    });

    it("valid", function() {
      const expr = { 
        type: "op",
        table: "t1",
        op: "+",
        exprs: [{ type: "field", table: "t1", column: "number" }]
      };
      return this.isValid(expr);
    });

    it("invalid if expr invalid", function() {
      const expr = { 
        type: "op",
        table: "t1",
        op: "+",
        exprs: [{ type: "field", table: "t1", column: "xyz" }]
      };
      return this.notValid(expr);
    });

    return it("invalid if wrong expr types", function() {
      const expr = { 
        type: "op",
        table: "t1",
        op: "+",
        exprs: [{ type: "field", table: "t1", column: "text" }]
      };
      return this.notValid(expr);
    });
  });

  describe("case", function() {
    it("validates else", function() {
      let expr = { 
        type: "case",
        table: "t1",
        cases: [{ when: { type: "literal", valueType: "boolean", value: true }, then: { type: "literal", valueType: "number", value: 123 }}],
        else: { type: "literal", valueType: "text", value: "abc" }
      };
      this.isValid(expr);

      expr = _.cloneDeep(expr);
      expr.else = { type: "field", table: "t1", column: "xyz" };
      return this.notValid(expr);
    });

    it("validates cases whens boolean", function() {
      let expr = { 
        type: "case",
        table: "t1",
        cases: [{ when: { type: "literal", valueType: "boolean", value: true }, then: { type: "literal", valueType: "number", value: 123 }}],
        else: { type: "literal", valueType: "text", value: "abc" }
      };
      this.isValid(expr);

      expr = _.cloneDeep(expr);
      expr.cases[0].when = { type: "field", table: "t1", column: "text" };
      return this.notValid(expr);
    });

    return it("validates cases thens", function() {
      let expr = { 
        type: "case",
        table: "t1",
        cases: [{ when: { type: "literal", valueType: "boolean", value: true }, then: { type: "literal", valueType: "number", value: 123 }}],
        else: { type: "literal", valueType: "text", value: "abc" }
      };
      this.isValid(expr);

      expr = _.cloneDeep(expr);
      expr.cases[0].then = { type: "field", table: "t1", column: "xyz" };
      return this.notValid(expr);
    });
  });

  describe("score", function() {
    it("validates input", function() {
      let expr = { type: "score", table: "t1", input: { type: "field", table: "t1", column: "enum" }, scores: {} };
      this.isValid(expr);

      expr = { type: "score", table: "t1", input: { type: "field", table: "t1", column: "text" }, scores: {} };
      this.notValid(expr);

      expr = { type: "score", table: "t1", input: { type: "field", table: "t1", column: "xyz" }, scores: {} };
      return this.notValid(expr);
    });

    it("validates score keys", function() {
      let expr = { type: "score", table: "t1", input: { type: "field", table: "t1", column: "enum" }, scores: {
          a: { type: "field", table: "t1", column: "number" }
        } 
      };
      this.isValid(expr);

      expr = { type: "score", table: "t1", input: { type: "field", table: "t1", column: "enum" }, scores: {
          xyz: { type: "field", table: "t1", column: "number" }
        } 
      };
      return this.notValid(expr);
    });

    return it("validates score values", function() {
      let expr = { type: "score", table: "t1", input: { type: "field", table: "t1", column: "enum" }, scores: {
          a: { type: "field", table: "t1", column: "number" }
        } 
      };
      this.isValid(expr);

      expr = { type: "score", table: "t1", input: { type: "field", table: "t1", column: "enum" }, scores: {
          a: { type: "field", table: "t1", column: "text" }
        } 
      };
      return this.notValid(expr);
    });
  });

  describe("variable", function() { 
    it("fails if non-existent", function() {
      return this.notValid({ type: "variable", variableId: "varxyz" });
    });

    it("success if exists", function() {
      return this.isValid({ type: "variable", variableId: "varnumber" });
    });

    return it("checks idTable", function() {
      this.isValid({ type: "variable", variableId: "varid" });
      this.isValid({ type: "variable", variableId: "varid" }, { table: "t2" });
      this.isValid({ type: "variable", variableId: "varid" }, { table: "t2", idTable: "t1" });
      return this.notValid({ type: "variable", variableId: "varid" }, { table: "t2", idTable: "t2" });
    });
  });

  return it("validates extension", function() {
    const schema = fixtures.simpleSchema();
    const exprValidator = new ExprValidator(schema, variables);
    return assert.equal(exprValidator.validateExpr({ type: "extension", extension: "test" }), "test");
  });
});
