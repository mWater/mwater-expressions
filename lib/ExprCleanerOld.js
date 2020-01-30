"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var ExprCleaner,
    ExprUtils,
    ExprValidator,
    _,
    indexOf = [].indexOf;

_ = require('lodash');
ExprUtils = require('./ExprUtils');
ExprValidator = require('./ExprValidator'); // Cleans expressions. Cleaning means nulling invalid (not just incomplete) expressions if they cannot be auto-fixed.

module.exports = ExprCleaner =
/*#__PURE__*/
function () {
  function ExprCleaner(schema) {
    var variables = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : [];
    (0, _classCallCheck2["default"])(this, ExprCleaner);
    this.cleanComparisonExpr = this.cleanComparisonExpr.bind(this);
    this.cleanLogicalExpr = this.cleanLogicalExpr.bind(this);
    this.cleanCountExpr = this.cleanCountExpr.bind(this);
    this.cleanVariableExpr = this.cleanVariableExpr.bind(this);
    this.schema = schema;
    this.variables = variables;
    this.exprUtils = new ExprUtils(schema, variables);
    this.exprValidator = new ExprValidator(this.schema);
  } // Clean an expression, returning null if completely invalid, otherwise removing
  // invalid parts. Attempts to correct invalid types by wrapping in other expressions.
  // e.g. if an enum is chosen when a boolean is required, it will be wrapped in "= any" op
  // options are:
  //   table: optional current table. expression must be related to this table or will be stripped
  //   types: optional types to limit to
  //   enumValueIds: ids of enum values that are valid if type is enum
  //   idTable: table that type of id must be from
  //   aggrStatuses: statuses of aggregation to allow. list of "individual", "literal", "aggregate". Default: ["individual", "literal"]


  (0, _createClass2["default"])(ExprCleaner, [{
    key: "cleanExpr",
    value: function cleanExpr(expr) {
      var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
      var aggrOpItems, aggrStatus, args, booleanOnly, ex, i, k, l, opItem, ref, ref1, ref2, type;

      _.defaults(options, {
        aggrStatuses: ["individual", "literal"]
      });

      if (!expr) {
        return null;
      } // Allow {} placeholder


      if (_.isEmpty(expr)) {
        return expr;
      } // Handle upgrades from old version


      if (expr.type === "comparison") {
        return this.cleanComparisonExpr(expr, options);
      }

      if (expr.type === "logical") {
        return this.cleanLogicalExpr(expr, options);
      }

      if (expr.type === "count") {
        return this.cleanCountExpr(expr, options);
      }

      if (expr.type === "literal" && expr.valueType === "enum[]") {
        expr = {
          type: "literal",
          valueType: "enumset",
          value: expr.value
        };
      } // Strip if wrong table 


      if (options.table && expr.table && expr.table !== options.table) {
        return null;
      } // Strip if no table


      if (!expr.table && expr.type === "field") {
        return null;
      } // Strip if non-existent table


      if (expr.table && !this.schema.getTable(expr.table)) {
        return null;
      } // Handle infinite recursion that can occur when cleaning field expressions that self-reference


      aggrStatus = null;

      try {
        aggrStatus = this.exprUtils.getExprAggrStatus(expr);
      } catch (error) {
        ex = error;

        if (ex.message === 'Infinite recursion') {
          return null;
        }

        throw ex;
      } // Default aggregation if needed and not aggregated


      if (aggrStatus === "individual" && indexOf.call(options.aggrStatuses, "individual") < 0 && indexOf.call(options.aggrStatuses, "aggregate") >= 0) {
        aggrOpItems = this.exprUtils.findMatchingOpItems({
          resultTypes: options.types,
          lhsExpr: expr,
          aggr: true,
          ordered: ((ref = this.schema.getTable(expr.table)) != null ? ref.ordering : void 0) != null
        }); // If aggr is required and there is at least one possible, use it

        if (aggrOpItems.length > 0) {
          expr = {
            type: "op",
            op: aggrOpItems[0].op,
            table: expr.table,
            exprs: [expr]
          };
          aggrStatus = "aggregate";
        }
      } // Default percent where + booleanization 


      if (aggrStatus === "individual" && indexOf.call(options.aggrStatuses, "individual") < 0 && indexOf.call(options.aggrStatuses, "aggregate") >= 0) {
        // Only if result types include number
        if (!options.types || indexOf.call(options.types, "number") >= 0) {
          // Find op item that matches
          opItem = this.exprUtils.findMatchingOpItems({
            resultTypes: ["boolean"],
            lhsExpr: expr
          })[0];

          if (opItem) {
            // Wrap in op to make it boolean
            expr = {
              type: "op",
              table: expr.table,
              op: opItem.op,
              exprs: [expr]
            }; // Determine number of arguments to append

            args = opItem.exprTypes.length - 1; // Add extra nulls for other arguments

            for (i = k = 1, ref1 = args; 1 <= ref1 ? k <= ref1 : k >= ref1; i = 1 <= ref1 ? ++k : --k) {
              expr.exprs.push(null);
            }

            expr = {
              type: "op",
              op: "percent where",
              table: expr.table,
              exprs: [expr]
            };
            aggrStatus = "aggregate";
          }
        }
      } // Strip if wrong aggregation status


      if (aggrStatus && indexOf.call(options.aggrStatuses, aggrStatus) < 0) {
        return null;
      } // Get type


      type = this.exprUtils.getExprType(expr); // Boolean-ize for easy building of filters
      // True if a boolean expression is required

      booleanOnly = options.types && options.types.length === 1 && options.types[0] === "boolean"; // If boolean and expr is not boolean, wrap with appropriate expression

      if (booleanOnly && type && type !== "boolean") {
        // Find op item that matches
        opItem = this.exprUtils.findMatchingOpItems({
          resultTypes: ["boolean"],
          lhsExpr: expr
        })[0];

        if (opItem) {
          // Wrap in op to make it boolean
          expr = {
            type: "op",
            table: expr.table,
            op: opItem.op,
            exprs: [expr]
          }; // Determine number of arguments to append

          args = opItem.exprTypes.length - 1; // Add extra nulls for other arguments

          for (i = l = 1, ref2 = args; 1 <= ref2 ? l <= ref2 : l >= ref2; i = 1 <= ref2 ? ++l : --l) {
            expr.exprs.push(null);
          }
        }
      } // Get type again in case changed  


      type = this.exprUtils.getExprType(expr); // Strip if wrong type

      if (type && options.types && indexOf.call(options.types, type) < 0) {
        // case statements should be preserved as they are a variable type and they will have their then clauses cleaned
        if (expr.type !== "case") {
          return null;
        }
      }

      switch (expr.type) {
        case "field":
          return this.cleanFieldExpr(expr, options);

        case "scalar":
          return this.cleanScalarExpr(expr, options);

        case "op":
          return this.cleanOpExpr(expr, options);

        case "literal":
          return this.cleanLiteralExpr(expr, options);

        case "case":
          return this.cleanCaseExpr(expr, options);

        case "id":
          return this.cleanIdExpr(expr, options);

        case "score":
          return this.cleanScoreExpr(expr, options);

        case "build enumset":
          return this.cleanBuildEnumsetExpr(expr, options);

        case "variable":
          return this.cleanVariableExpr(expr, options);

        default:
          throw new Error("Unknown expression type ".concat(expr.type));
      }
    } // Removes references to non-existent tables

  }, {
    key: "cleanFieldExpr",
    value: function cleanFieldExpr(expr, options) {
      var column; // Empty expression

      if (!expr.column || !expr.table) {
        return null;
      } // Missing table


      if (!this.schema.getTable(expr.table)) {
        return null;
      } // Missing column


      column = this.schema.getColumn(expr.table, expr.column);

      if (!column) {
        return null;
      } // Invalid expr


      if (column.expr) {
        if (this.exprValidator.validateExpr(column.expr, options)) {
          return null;
        }
      } // Invalid enums


      if (options.enumValueIds && column.type === "enum") {
        if (_.difference(_.pluck(column.enumValues, "id"), options.enumValueIds).length > 0) {
          return null;
        }
      }

      if (options.enumValueIds && column.expr) {
        if (this.exprUtils.getExprType(column.expr) === "enum") {
          if (_.difference(_.pluck(this.exprUtils.getExprEnumValues(column.expr), "id"), options.enumValueIds).length > 0) {
            return null;
          }
        }
      }

      return expr;
    }
  }, {
    key: "cleanOpExpr",
    value: function cleanOpExpr(expr, options) {
      var _this = this;

      var aggr, enumValueIds, enumValues, exprs, innerAggrStatuses, lhsExpr, lhsTypes, opIsAggr, opItem, opItems, ref;

      switch (expr.op) {
        case "and":
        case "or":
          expr = _.extend({}, expr, {
            exprs: _.map(expr.exprs, function (e) {
              return _this.cleanExpr(e, {
                types: ["boolean"],
                table: expr.table
              });
            })
          }); // Simplify

          if (expr.exprs.length === 1) {
            return expr.exprs[0];
          }

          if (expr.exprs.length === 0) {
            return null;
          }

          return expr;

        case "+":
        case "*":
          expr = _.extend({}, expr, {
            exprs: _.map(expr.exprs, function (e) {
              return _this.cleanExpr(e, {
                types: ["number"],
                table: expr.table,
                aggrStatuses: options.aggrStatuses
              });
            })
          }); // Simplify

          if (expr.exprs.length === 1) {
            return expr.exprs[0];
          }

          if (expr.exprs.length === 0) {
            return null;
          }

          return expr;

        default:
          // Count always takes zero parameters and is valid if number type is valid
          if (expr.op === "count" && (!options.types || indexOf.call(options.types, "number") >= 0) && indexOf.call(options.aggrStatuses, "aggregate") >= 0) {
            return {
              type: "op",
              op: "count",
              table: expr.table,
              exprs: []
            };
          } // Determine aggregate type of op


          opIsAggr = ExprUtils.isOpAggr(expr.op); // Aggregate ops are never allowed if aggregates are not allowed

          if (opIsAggr && indexOf.call(options.aggrStatuses, "aggregate") < 0) {
            return null;
          } // Determine aggr setting. Prevent aggr for non-aggr output


          aggr = null;

          if (indexOf.call(options.aggrStatuses, "aggregate") < 0 && indexOf.call(options.aggrStatuses, "individual") >= 0) {
            aggr = false;
          } // Determine innerAggrStatuses (same as outer, unless aggregate expression, in which case always aggregate)


          if (opIsAggr) {
            innerAggrStatuses = ["literal", "individual"];
          } else {
            innerAggrStatuses = options.aggrStatuses;
          } // First do a loose cleaning of LHS to remove obviously invalid values


          lhsExpr = this.cleanExpr(expr.exprs[0], {
            table: expr.table,
            aggrStatuses: innerAggrStatuses
          }); // Now attempt to clean it restricting to the types the op allows as lhs

          if (lhsExpr) {
            lhsTypes = _.uniq(_.compact(_.map(this.exprUtils.findMatchingOpItems({
              op: expr.op
            }), function (opItem) {
              return opItem.exprTypes[0];
            })));
            lhsExpr = this.cleanExpr(expr.exprs[0], {
              table: expr.table,
              aggrStatuses: innerAggrStatuses,
              types: lhsTypes
            }); // If this nulls it, don't keep as we can switch ops to preseve it

            if (lhsExpr == null) {
              lhsExpr = this.cleanExpr(expr.exprs[0], {
                table: expr.table,
                aggrStatuses: innerAggrStatuses
              });
            }
          } // Need LHS for a normal op that is not a prefix. If it is a prefix op, allow the op to stand alone without params. Allow null type (ones being built out) to stand too


          if (!lhsExpr && !ExprUtils.isOpPrefix(expr.op)) {
            return null;
          } // Get opItem


          opItems = this.exprUtils.findMatchingOpItems({
            op: expr.op,
            lhsExpr: lhsExpr,
            resultTypes: options.types,
            aggr: aggr,
            ordered: ((ref = this.schema.getTable(expr.table)) != null ? ref.ordering : void 0) != null
          }); // If ambiguous, just clean subexprs and return

          if (opItems.length > 1) {
            return _.extend({}, expr, {
              exprs: _.map(expr.exprs, function (e, i) {
                var types; // Determine all possible types (union of all op items types)

                types = _.uniq(_.compact(_.flatten(_.map(opItems, function (opItem) {
                  return opItem.exprTypes[i];
                }))));
                return _this.cleanExpr(e, {
                  table: expr.table,
                  aggrStatuses: innerAggrStatuses,
                  types: types.length > 0 ? types : void 0
                });
              })
            });
          } // If not found, default opItem


          if (!opItems[0]) {
            opItem = this.exprUtils.findMatchingOpItems({
              lhsExpr: lhsExpr,
              resultTypes: options.types,
              aggr: aggr
            })[0];

            if (!opItem) {
              return null;
            }

            expr = {
              type: "op",
              table: expr.table,
              op: opItem.op,
              exprs: [lhsExpr || null]
            };
          } else {
            opItem = opItems[0];
          } // Pad or trim number of expressions


          while (expr.exprs.length < opItem.exprTypes.length) {
            exprs = expr.exprs.slice();
            exprs.push(null);
            expr = _.extend({}, expr, {
              exprs: exprs
            });
          }

          if (expr.exprs.length > opItem.exprTypes.length) {
            expr = _.extend({}, expr, {
              exprs: _.take(expr.exprs, opItem.exprTypes.length)
            });
          } // Clean all sub expressions


          if (lhsExpr) {
            enumValues = this.exprUtils.getExprEnumValues(lhsExpr);

            if (enumValues) {
              enumValueIds = _.pluck(enumValues, "id");
            }
          }

          expr = _.extend({}, expr, {
            exprs: _.map(expr.exprs, function (e, i) {
              return _this.cleanExpr(e, {
                table: expr.table,
                types: opItem.exprTypes[i] ? [opItem.exprTypes[i]] : void 0,
                enumValueIds: enumValueIds,
                idTable: _this.exprUtils.getExprIdTable(expr.exprs[0]),
                aggrStatuses: innerAggrStatuses
              });
            })
          });
          return expr;
      }
    } // Strips/defaults invalid aggr and where of a scalar expression

  }, {
    key: "cleanScalarExpr",
    value: function cleanScalarExpr(expr, options) {
      var aggrStatuses, innerTable, isMultiple, joins, ref;

      if (expr.joins.length === 0) {
        return this.cleanExpr(expr.expr, options);
      } // Fix legacy entity joins (accidentally had entities.<tablename>. prepended)


      joins = _.map(expr.joins, function (j) {
        if (j.match(/^entities\.[a-z_0-9]+\./)) {
          return j.split(".")[2];
        }

        return j;
      });
      expr = _.extend({}, expr, {
        joins: joins
      });

      if (!this.exprUtils.areJoinsValid(expr.table, expr.joins)) {
        return null;
      }

      innerTable = this.exprUtils.followJoins(expr.table, expr.joins); // Move aggr to inner expression

      if (expr.aggr) {
        expr = _.extend({}, _.omit(expr, "aggr"), {
          expr: {
            type: "op",
            table: innerTable,
            op: expr.aggr,
            exprs: [expr.expr]
          }
        });
      } // Clean where


      if (expr.where) {
        expr.where = this.cleanExpr(expr.where, {
          table: innerTable
        });
      } // Simplify to join column


      if (!expr.where && expr.joins.length === 1 && ((ref = expr.expr) != null ? ref.type : void 0) === "id") {
        return {
          type: "field",
          table: expr.table,
          column: expr.joins[0]
        };
      } // Get inner expression type (must match unless is count which can count anything)


      if (expr.expr) {
        isMultiple = this.exprUtils.isMultipleJoins(expr.table, expr.joins);
        aggrStatuses = isMultiple ? ["literal", "aggregate"] : ["literal", "individual"];
        expr = _.extend({}, expr, {
          expr: this.cleanExpr(expr.expr, _.extend({}, options, {
            table: innerTable,
            aggrStatuses: aggrStatuses
          }))
        });
      }

      return expr;
    }
  }, {
    key: "cleanLiteralExpr",
    value: function cleanLiteralExpr(expr, options) {
      var ref, ref1; // Convert old types

      if ((ref = expr.valueType) === 'decimal' || ref === 'integer') {
        expr = _.extend({}, expr, {
          valueType: "number"
        });
      } // TODO strip if no value?
      // Remove if enum type is wrong


      if (expr.valueType === "enum" && options.enumValueIds && expr.value && (ref1 = expr.value, indexOf.call(options.enumValueIds, ref1) < 0)) {
        return null;
      } // Remove invalid enum types


      if (expr.valueType === "enumset" && options.enumValueIds && expr.value) {
        expr = _.extend({}, expr, {
          value: _.intersection(options.enumValueIds, expr.value)
        });
      } // Null if wrong table


      if (expr.valueType === "id" && options.idTable && expr.idTable !== options.idTable) {
        return null;
      }

      return expr;
    }
  }, {
    key: "cleanCaseExpr",
    value: function cleanCaseExpr(expr, options) {
      var _this2 = this;

      // Simplify if no cases
      if (expr.cases.length === 0) {
        return expr["else"] || null;
      } // Clean whens as boolean


      expr = _.extend({}, expr, {
        cases: _.map(expr.cases, function (c) {
          return _.extend({}, c, {
            when: _this2.cleanExpr(c.when, {
              types: ["boolean"],
              table: expr.table
            }),
            then: _this2.cleanExpr(c.then, options)
          });
        }),
        "else": this.cleanExpr(expr["else"], options)
      });
      return expr;
    }
  }, {
    key: "cleanIdExpr",
    value: function cleanIdExpr(expr, options) {
      // Null if wrong table
      if (options.idTable && expr.table !== options.idTable) {
        return null;
      }

      return expr;
    }
  }, {
    key: "cleanScoreExpr",
    value: function cleanScoreExpr(expr, options) {
      var _this3 = this;

      var enumValues; // Clean input

      expr = _.extend({}, expr, {
        input: this.cleanExpr(expr.input, {
          types: ['enum', 'enumset']
        })
      }); // Remove scores if no input

      if (!expr.input) {
        expr = _.extend({}, expr, {
          scores: {}
        });
      } // Clean score values


      expr = _.extend({}, expr, {
        scores: _.mapValues(expr.scores, function (scoreExpr) {
          return _this3.cleanExpr(scoreExpr, {
            table: expr.table,
            types: ['number']
          });
        })
      }); // Remove unknown enum values 

      if (expr.input) {
        enumValues = this.exprUtils.getExprEnumValues(expr.input);
        expr = _.extend({}, expr, {
          scores: _.pick(expr.scores, function (value, key) {
            return _.findWhere(enumValues, {
              id: key
            }) && value != null;
          })
        });
      }

      return expr;
    }
  }, {
    key: "cleanBuildEnumsetExpr",
    value: function cleanBuildEnumsetExpr(expr, options) {
      var _this4 = this;

      // Clean values
      expr = _.extend({}, expr, {
        values: _.mapValues(expr.values, function (valueExpr) {
          return _this4.cleanExpr(valueExpr, {
            table: expr.table,
            types: ['boolean']
          });
        })
      }); // Remove unknown enum values 

      if (options.enumValueIds) {
        expr = _.extend({}, expr, {
          values: _.pick(expr.values, function (value, key) {
            return indexOf.call(options.enumValueIds, key) >= 0;
          })
        });
      } // Remove blank values


      expr = _.extend({}, expr, {
        values: _.pick(expr.values, function (value, key) {
          return value != null;
        })
      });
      return expr;
    }
  }, {
    key: "cleanComparisonExpr",
    value: function cleanComparisonExpr(expr, options) {
      var _this5 = this;

      var newExpr; // Upgrade to op

      newExpr = {
        type: "op",
        table: expr.table,
        op: expr.op,
        exprs: [expr.lhs]
      };

      if (expr.rhs) {
        newExpr.exprs.push(expr.rhs);
      } // Clean sub-expressions to handle legacy literals


      newExpr.exprs = _.map(newExpr.exprs, function (e) {
        return _this5.cleanExpr(e);
      }); // If = true

      if (expr.op === "= true") {
        newExpr = expr.lhs;
      }

      if (expr.op === "= false") {
        newExpr = {
          type: "op",
          op: "not",
          table: expr.table,
          exprs: [expr.lhs]
        };
      }

      if (expr.op === "between" && expr.rhs && expr.rhs.type === "literal" && expr.rhs.valueType === "daterange") {
        newExpr.exprs = [expr.lhs, {
          type: "literal",
          valueType: "date",
          value: expr.rhs.value[0]
        }, {
          type: "literal",
          valueType: "date",
          value: expr.rhs.value[1]
        }];
      }

      if (expr.op === "between" && expr.rhs && expr.rhs.type === "literal" && expr.rhs.valueType === "datetimerange") {
        // If date, convert datetime to date
        if (this.exprUtils.getExprType(expr.lhs) === "date") {
          newExpr.exprs = [expr.lhs, {
            type: "literal",
            valueType: "date",
            value: expr.rhs.value[0].substr(0, 10)
          }, {
            type: "literal",
            valueType: "date",
            value: expr.rhs.value[1].substr(0, 10)
          }];
        } else {
          newExpr.exprs = [expr.lhs, {
            type: "literal",
            valueType: "datetime",
            value: expr.rhs.value[0]
          }, {
            type: "literal",
            valueType: "datetime",
            value: expr.rhs.value[1]
          }];
        }
      }

      return this.cleanExpr(newExpr, options);
    }
  }, {
    key: "cleanLogicalExpr",
    value: function cleanLogicalExpr(expr, options) {
      var newExpr;
      newExpr = {
        type: "op",
        op: expr.op,
        table: expr.table,
        exprs: expr.exprs
      };
      return this.cleanExpr(newExpr, options);
    }
  }, {
    key: "cleanCountExpr",
    value: function cleanCountExpr(expr, options) {
      var newExpr;
      newExpr = {
        type: "id",
        table: expr.table
      };
      return this.cleanExpr(newExpr, options);
    }
  }, {
    key: "cleanVariableExpr",
    value: function cleanVariableExpr(expr, options) {
      var variable; // Get variable

      variable = _.findWhere(this.variables, {
        id: expr.variableId
      });

      if (!variable) {
        return null;
      } // Check id table


      if (options.idTable && variable.type === "id" && variable.idTable !== options.idTable) {
        return null;
      }

      return expr;
    }
  }]);
  return ExprCleaner;
}();