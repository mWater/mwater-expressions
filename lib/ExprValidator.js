"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var ExprUtils,
    ExprValidator,
    _,
    indexOf = [].indexOf;

_ = require('lodash');
ExprUtils = require('./ExprUtils'); // Validates expressions. If an expression has been cleaned, it will always be valid

module.exports = ExprValidator =
/*#__PURE__*/
function () {
  function ExprValidator(schema) {
    var variables = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : [];
    (0, _classCallCheck2["default"])(this, ExprValidator);
    this.schema = schema;
    this.variables = variables;
    this.exprUtils = new ExprUtils(schema, variables);
  } // Validates an expression, returning null if it is valid, otherwise return an error string
  // options are:
  //   table: optional current table. expression must be related to this table or will be stripped
  //   types: optional types to limit to
  //   enumValueIds: ids of enum values that are valid if type is enum
  //   idTable: table that type of id must be from
  //   aggrStatuses: statuses of aggregation to allow. list of "individual", "literal", "aggregate". Default: ["individual", "literal"]


  (0, _createClass2["default"])(ExprValidator, [{
    key: "validateExpr",
    value: function validateExpr(expr) {
      var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
      var column, cse, enumValueIds, error, exprTable, i, j, key, len, len1, opItems, ref, ref1, ref2, ref3, ref4, ref5, ref6, subexpr, value, variable;

      _.defaults(options, {
        aggrStatuses: ["individual", "literal"]
      });

      if (!expr) {
        return null;
      } // Allow {} placeholder


      if (_.isEmpty(expr)) {
        return null;
      } // Prevent infinite recursion


      if (options.depth > 100) {
        return "Circular reference";
      } // Check table if not literal


      if (options.table && expr.table && expr.table !== options.table) {
        return "Wrong table ".concat(expr.table, " (expected ").concat(options.table, ")");
      } // Literal is ok if right type


      switch (expr.type) {
        case "literal":
          if (options.types && (ref = expr.valueType, indexOf.call(options.types, ref) < 0)) {
            return "Wrong type";
          }

          if (options.idTable && expr.valueType === "id" && options.idTable !== expr.idTable) {
            return "Wrong table";
          }

          break;

        case "field":
          column = this.schema.getColumn(expr.table, expr.column);

          if (!column) {
            return "Missing column";
          } // Validate expression


          if (column.expr) {
            // Use depth to prevent infinite recursion
            error = this.validateExpr(column.expr, _.extend({}, options, {
              depth: (options.depth || 0) + 1
            }));

            if (error) {
              return error;
            }
          }

          break;

        case "op":
          ref1 = expr.exprs; // Validate exprs

          for (i = 0, len = ref1.length; i < len; i++) {
            subexpr = ref1[i];
            error = this.validateExpr(subexpr, _.omit(options, "types", "enumValueIds", "idTable"));

            if (error) {
              return error;
            }
          } // Find op


          opItems = this.exprUtils.findMatchingOpItems({
            op: expr.op,
            lhsExpr: expr.exprs[0],
            resultTypes: options.types
          });

          if (opItems.length === 0) {
            return "No matching op";
          }

          break;

        case "scalar":
          // Validate joins
          if (!this.exprUtils.areJoinsValid(expr.table, expr.joins)) {
            return "Invalid joins";
          }

          exprTable = this.exprUtils.followJoins(expr.table, expr.joins);
          error = this.validateExpr(expr.expr, _.extend({}, options, {
            table: exprTable
          }));

          if (error) {
            return error;
          }

          break;

        case "case":
          ref2 = expr.cases; // Validate cases

          for (j = 0, len1 = ref2.length; j < len1; j++) {
            cse = ref2[j];
            error = this.validateExpr(cse.when, _.extend({}, options, {
              types: ["boolean"]
            }));

            if (error) {
              return error;
            }

            error = this.validateExpr(cse.then, options);

            if (error) {
              return error;
            }
          }

          error = this.validateExpr(expr["else"], options);

          if (error) {
            return error;
          }

          break;

        case "score":
          error = this.validateExpr(expr.input, _.extend({}, options, {
            types: ["enum", "enumset"]
          }));

          if (error) {
            return error;
          }

          if (expr.input) {
            enumValueIds = _.pluck(this.exprUtils.getExprEnumValues(expr.input), "id");
          } else {
            enumValueIds = null;
          }

          ref3 = expr.scores;

          for (key in ref3) {
            value = ref3[key];

            if (enumValueIds && indexOf.call(enumValueIds, key) < 0) {
              return "Invalid score enum";
            }

            error = this.validateExpr(value, _.extend({}, options, {
              types: ["number"]
            }));

            if (error) {
              return error;
            }
          }

          break;

        case "build enumset":
          ref4 = expr.values;

          for (key in ref4) {
            value = ref4[key];

            if (options.enumValueIds && indexOf.call(options.enumValueIds, key) < 0) {
              return "Invalid score enum";
            }

            error = this.validateExpr(value, _.extend({}, options, {
              types: ["boolean"]
            }));

            if (error) {
              return error;
            }
          }

          break;

        case "variable":
          // Get variable
          variable = _.findWhere(this.variables, {
            id: expr.variableId
          });

          if (!variable) {
            return "Missing variable ".concat(expr.variableId);
          }

      } // Validate table


      if (options.idTable && this.exprUtils.getExprIdTable(expr) && this.exprUtils.getExprIdTable(expr) !== options.idTable) {
        return "Wrong idTable";
      } // Validate type if present


      if (options.types && this.exprUtils.getExprType(expr) && (ref5 = this.exprUtils.getExprType(expr), indexOf.call(options.types, ref5) < 0)) {
        return "Invalid type";
      } // Validate enums


      if (options.enumValueIds && ((ref6 = this.exprUtils.getExprType(expr)) === 'enum' || ref6 === 'enumset')) {
        if (_.difference(_.pluck(this.exprUtils.getExprEnumValues(expr), "id"), options.enumValueIds).length > 0) {
          return "Invalid enum";
        }
      }

      return null;
    }
  }]);
  return ExprValidator;
}();