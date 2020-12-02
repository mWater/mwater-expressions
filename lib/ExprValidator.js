"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var ExprUtils,
    ExprValidator,
    WeakCache,
    _,
    weakCache,
    indexOf = [].indexOf;

_ = require('lodash');
ExprUtils = require('./ExprUtils');
WeakCache = require('./WeakCache').WeakCache; // Weak cache is global to allow validator to be created and destroyed

weakCache = new WeakCache(); // Validates expressions. If an expression has been cleaned, it will always be valid

module.exports = ExprValidator = /*#__PURE__*/function () {
  function ExprValidator(schema) {
    (0, _classCallCheck2["default"])(this, ExprValidator);
    this.validateExprInternal = this.validateExprInternal.bind(this);
    this.schema = schema;
    this.exprUtils = new ExprUtils(schema);
  } // Validates an expression, returning null if it is valid, otherwise return an error string
  // NOTE: This uses global weak caching and assumes that expressions are never mutated after
  // having been validated!
  // options are:
  //   table: optional current table. expression must be related to this table or will be stripped
  //   types: optional types to limit to
  //   enumValueIds: ids of enum values that are valid if type is enum
  //   idTable: table that type of id must be from
  //   aggrStatuses: statuses of aggregation to allow. list of "individual", "literal", "aggregate". Default: ["individual", "literal"]


  (0, _createClass2["default"])(ExprValidator, [{
    key: "validateExpr",
    value: function validateExpr(expr) {
      var _this = this;

      var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};

      if (!expr) {
        return null;
      }

      return weakCache.cacheFunction([this.schema, expr], [options], function () {
        return _this.validateExprInternal(expr, options);
      });
    }
  }, {
    key: "validateExprInternal",
    value: function validateExprInternal(expr, options) {
      var aggrStatuses, column, cse, enumValueIds, error, exprTable, i, j, key, len, len1, opItems, ref, ref1, ref2, ref3, ref4, ref5, ref6, subexpr, value, variable;
      aggrStatuses = options.aggrStatuses || {
        aggrStatuses: ["individual", "literal"]
      };

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
            error = this.validateExprInternal(column.expr, _.extend({}, options, {
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
            error = this.validateExprInternal(subexpr, _.omit(options, "types", "enumValueIds", "idTable"));

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
          error = this.validateExprInternal(expr.expr, _.extend({}, options, {
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
            error = this.validateExprInternal(cse.when, _.extend({}, options, {
              types: ["boolean"]
            }));

            if (error) {
              return error;
            }

            error = this.validateExprInternal(cse.then, options);

            if (error) {
              return error;
            }
          }

          error = this.validateExprInternal(expr["else"], options);

          if (error) {
            return error;
          }

          break;

        case "score":
          error = this.validateExprInternal(expr.input, _.extend({}, options, {
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

            error = this.validateExprInternal(value, _.extend({}, options, {
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

            error = this.validateExprInternal(value, _.extend({}, options, {
              types: ["boolean"]
            }));

            if (error) {
              return error;
            }
          }

          break;

        case "variable":
          // Get variable
          variable = _.findWhere(this.schema.getVariables(), {
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