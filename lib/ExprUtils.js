"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var ExprUtils,
    WeakCache,
    _,
    addOpItem,
    aggrOpItems,
    exprAggrStatusWeakCache,
    groupedOpItems,
    i,
    k,
    l,
    len,
    len1,
    len2,
    len3,
    len4,
    len5,
    len6,
    len7,
    len8,
    m,
    moment,
    n,
    o,
    op,
    opItems,
    p,
    q,
    r,
    ref,
    ref1,
    ref2,
    ref3,
    ref4,
    ref5,
    ref6,
    ref7,
    relativeDateOp,
    relativeDateOps,
    type,
    type1,
    type2,
    indexOf = [].indexOf;

_ = require('lodash');
moment = require('moment');
WeakCache = require('./WeakCache').WeakCache; // exprAggrStatus Weak cache is global to allow validator to be created and destroyed

exprAggrStatusWeakCache = new WeakCache();

module.exports = ExprUtils = /*#__PURE__*/function () {
  function ExprUtils(schema) {
    var variables = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : [];
    (0, _classCallCheck2["default"])(this, ExprUtils);
    // Replace variables with literal values.
    this.inlineVariableValues = this.inlineVariableValues.bind(this);
    this.schema = schema;
    this.variables = variables;
  } // Search can contain resultTypes, lhsExpr, op, aggr. lhsExpr is actual expression of lhs. resultTypes is optional array of result types
  // If search ordered is not true, excludes ordered ones
  // If prefix, only prefix
  // Results are array of opItems.


  (0, _createClass2["default"])(ExprUtils, [{
    key: "findMatchingOpItems",
    value: function findMatchingOpItems(search) {
      var _this = this;

      var items; // Narrow list if op specified

      if (search.op) {
        items = groupedOpItems[search.op] || [];
      } else {
        items = opItems;
      }

      return _.filter(items, function (opItem) {
        var lhsType, ref;

        if (search.resultTypes) {
          if (ref = opItem.resultType, indexOf.call(search.resultTypes, ref) < 0) {
            return false;
          }
        }

        if (search.aggr != null && opItem.aggr !== search.aggr) {
          return false;
        }

        if (search.ordered === false && opItem.ordered) {
          return false;
        }

        if (search.prefix != null && opItem.prefix !== search.prefix) {
          return false;
        } // Handle list of specified types


        if (search.lhsExpr) {
          lhsType = _this.getExprType(search.lhsExpr);

          if (lhsType && (opItem.exprTypes[0] != null && opItem.exprTypes[0] !== lhsType || opItem.moreExprType != null && opItem.exprTypes[0] == null && opItem.moreExprType !== lhsType)) {
            return false;
          }
        } // Check lhsCond


        if (search.lhsExpr && opItem.lhsCond && !opItem.lhsCond(search.lhsExpr, _this)) {
          return false;
        }

        return true;
      });
    } // Determine if op is aggregate

  }, {
    key: "followJoins",
    // Follows a list of joins to determine final table
    value: function followJoins(startTable, joins) {
      var i, j, joinCol, len, t;
      t = startTable;

      for (i = 0, len = joins.length; i < len; i++) {
        j = joins[i];
        joinCol = this.schema.getColumn(t, j);

        if (joinCol.type === "join") {
          t = joinCol.join.toTable;
        } else {
          t = joinCol.idTable;
        }
      }

      return t;
    } // Determines if an set of joins contains a multiple

  }, {
    key: "isMultipleJoins",
    value: function isMultipleJoins(table, joins) {
      var i, j, joinCol, len, ref, t;
      t = table;

      for (i = 0, len = joins.length; i < len; i++) {
        j = joins[i];
        joinCol = this.schema.getColumn(t, j);

        if (joinCol.type === "join") {
          if ((ref = joinCol.join.type) === '1-n' || ref === 'n-n') {
            return true;
          }

          t = joinCol.join.toTable;
        } else if (joinCol.type === "id") {
          t = joinCol.idTable;
        } else if (joinCol.type === "id[]") {
          return true;
        } else {
          throw new Error("Unsupported join type ".concat(joinCol.type));
        }
      }

      return false;
    } // Return array of { id: <enum value>, name: <localized label of enum value> }

  }, {
    key: "getExprEnumValues",
    value: function getExprEnumValues(expr) {
      var column, cse, enumValues, i, len, ref, ref1, ref2;

      if (!expr) {
        return;
      }

      if (expr.type === "field") {
        column = this.schema.getColumn(expr.table, expr.column);

        if (!column) {
          return null;
        } // Prefer returning specified enumValues as expr might not cover all possibilities if it's an if/then, etc.        


        if (column.enumValues) {
          return column.enumValues;
        } // DEPRECATED. Remove July 2017


        if (column.type === "expr") {
          return this.getExprEnumValues(column.expr);
        }

        return null;
      }

      if (expr.type === "scalar") {
        if (expr.expr) {
          return this.getExprEnumValues(expr.expr);
        }
      } // "last", "last where" and "previous" are only ops to pass through enum values


      if (expr.type === "op" && ((ref = expr.op) === "last" || ref === "last where" || ref === "previous") && expr.exprs[0]) {
        return this.getExprEnumValues(expr.exprs[0]);
      } // Weeks of month has predefined values (1-5 as text)


      if (expr.type === "op" && expr.op === "weekofmonth") {
        return [{
          id: "1",
          name: {
            en: "1"
          }
        }, {
          id: "2",
          name: {
            en: "2"
          }
        }, {
          id: "3",
          name: {
            en: "3"
          }
        }, {
          id: "4",
          name: {
            en: "4"
          }
        }, {
          id: "5",
          name: {
            en: "5"
          }
        }];
      } // Days of month has predefined values (01-31 as text)


      if (expr.type === "op" && expr.op === "dayofmonth") {
        return [{
          id: "01",
          name: {
            en: "01"
          }
        }, {
          id: "02",
          name: {
            en: "02"
          }
        }, {
          id: "03",
          name: {
            en: "03"
          }
        }, {
          id: "04",
          name: {
            en: "04"
          }
        }, {
          id: "05",
          name: {
            en: "05"
          }
        }, {
          id: "06",
          name: {
            en: "06"
          }
        }, {
          id: "07",
          name: {
            en: "07"
          }
        }, {
          id: "08",
          name: {
            en: "08"
          }
        }, {
          id: "09",
          name: {
            en: "09"
          }
        }, {
          id: "10",
          name: {
            en: "10"
          }
        }, {
          id: "11",
          name: {
            en: "11"
          }
        }, {
          id: "12",
          name: {
            en: "12"
          }
        }, {
          id: "13",
          name: {
            en: "13"
          }
        }, {
          id: "14",
          name: {
            en: "14"
          }
        }, {
          id: "15",
          name: {
            en: "15"
          }
        }, {
          id: "16",
          name: {
            en: "16"
          }
        }, {
          id: "17",
          name: {
            en: "17"
          }
        }, {
          id: "18",
          name: {
            en: "18"
          }
        }, {
          id: "19",
          name: {
            en: "19"
          }
        }, {
          id: "20",
          name: {
            en: "20"
          }
        }, {
          id: "21",
          name: {
            en: "21"
          }
        }, {
          id: "22",
          name: {
            en: "22"
          }
        }, {
          id: "23",
          name: {
            en: "23"
          }
        }, {
          id: "24",
          name: {
            en: "24"
          }
        }, {
          id: "25",
          name: {
            en: "25"
          }
        }, {
          id: "26",
          name: {
            en: "26"
          }
        }, {
          id: "27",
          name: {
            en: "27"
          }
        }, {
          id: "28",
          name: {
            en: "28"
          }
        }, {
          id: "29",
          name: {
            en: "29"
          }
        }, {
          id: "30",
          name: {
            en: "30"
          }
        }, {
          id: "31",
          name: {
            en: "31"
          }
        }];
      } // Month has predefined values


      if (expr.type === "op" && expr.op === "month") {
        return [{
          id: "01",
          name: {
            en: "January"
          }
        }, {
          id: "02",
          name: {
            en: "February"
          }
        }, {
          id: "03",
          name: {
            en: "March"
          }
        }, {
          id: "04",
          name: {
            en: "April"
          }
        }, {
          id: "05",
          name: {
            en: "May"
          }
        }, {
          id: "06",
          name: {
            en: "June"
          }
        }, {
          id: "07",
          name: {
            en: "July"
          }
        }, {
          id: "08",
          name: {
            en: "August"
          }
        }, {
          id: "09",
          name: {
            en: "September"
          }
        }, {
          id: "10",
          name: {
            en: "October"
          }
        }, {
          id: "11",
          name: {
            en: "November"
          }
        }, {
          id: "12",
          name: {
            en: "December"
          }
        }];
      } // Week of year has predefined values (01-53 as text)


      if (expr.type === "op" && expr.op === "weekofyear") {
        return [{
          id: "01",
          name: {
            en: "01"
          }
        }, {
          id: "02",
          name: {
            en: "02"
          }
        }, {
          id: "03",
          name: {
            en: "03"
          }
        }, {
          id: "04",
          name: {
            en: "04"
          }
        }, {
          id: "05",
          name: {
            en: "05"
          }
        }, {
          id: "06",
          name: {
            en: "06"
          }
        }, {
          id: "07",
          name: {
            en: "07"
          }
        }, {
          id: "08",
          name: {
            en: "08"
          }
        }, {
          id: "09",
          name: {
            en: "09"
          }
        }, {
          id: "10",
          name: {
            en: "10"
          }
        }, {
          id: "11",
          name: {
            en: "11"
          }
        }, {
          id: "12",
          name: {
            en: "12"
          }
        }, {
          id: "13",
          name: {
            en: "13"
          }
        }, {
          id: "14",
          name: {
            en: "14"
          }
        }, {
          id: "15",
          name: {
            en: "15"
          }
        }, {
          id: "16",
          name: {
            en: "16"
          }
        }, {
          id: "17",
          name: {
            en: "17"
          }
        }, {
          id: "18",
          name: {
            en: "18"
          }
        }, {
          id: "19",
          name: {
            en: "19"
          }
        }, {
          id: "20",
          name: {
            en: "20"
          }
        }, {
          id: "21",
          name: {
            en: "21"
          }
        }, {
          id: "22",
          name: {
            en: "22"
          }
        }, {
          id: "23",
          name: {
            en: "23"
          }
        }, {
          id: "24",
          name: {
            en: "24"
          }
        }, {
          id: "25",
          name: {
            en: "25"
          }
        }, {
          id: "26",
          name: {
            en: "26"
          }
        }, {
          id: "27",
          name: {
            en: "27"
          }
        }, {
          id: "28",
          name: {
            en: "28"
          }
        }, {
          id: "29",
          name: {
            en: "29"
          }
        }, {
          id: "30",
          name: {
            en: "30"
          }
        }, {
          id: "31",
          name: {
            en: "31"
          }
        }, {
          id: "32",
          name: {
            en: "32"
          }
        }, {
          id: "33",
          name: {
            en: "33"
          }
        }, {
          id: "34",
          name: {
            en: "34"
          }
        }, {
          id: "35",
          name: {
            en: "35"
          }
        }, {
          id: "36",
          name: {
            en: "36"
          }
        }, {
          id: "37",
          name: {
            en: "37"
          }
        }, {
          id: "38",
          name: {
            en: "38"
          }
        }, {
          id: "39",
          name: {
            en: "39"
          }
        }, {
          id: "40",
          name: {
            en: "40"
          }
        }, {
          id: "41",
          name: {
            en: "41"
          }
        }, {
          id: "42",
          name: {
            en: "42"
          }
        }, {
          id: "43",
          name: {
            en: "43"
          }
        }, {
          id: "44",
          name: {
            en: "44"
          }
        }, {
          id: "45",
          name: {
            en: "45"
          }
        }, {
          id: "46",
          name: {
            en: "46"
          }
        }, {
          id: "47",
          name: {
            en: "47"
          }
        }, {
          id: "48",
          name: {
            en: "48"
          }
        }, {
          id: "49",
          name: {
            en: "49"
          }
        }, {
          id: "50",
          name: {
            en: "50"
          }
        }, {
          id: "51",
          name: {
            en: "51"
          }
        }, {
          id: "52",
          name: {
            en: "52"
          }
        }, {
          id: "53",
          name: {
            en: "53"
          }
        }];
      } // Case statements search for possible values


      if (expr.type === "case") {
        ref1 = expr.cases;

        for (i = 0, len = ref1.length; i < len; i++) {
          cse = ref1[i];
          enumValues = this.getExprEnumValues(cse.then);

          if (enumValues) {
            return enumValues;
          }
        }

        return this.getExprEnumValues(expr["else"]);
      }

      if (expr.type === "variable") {
        return (ref2 = _.findWhere(this.variables, {
          id: expr.variableId
        })) != null ? ref2.enumValues : void 0;
      }
    } // gets the id table of an expression of type id

  }, {
    key: "getExprIdTable",
    value: function getExprIdTable(expr) {
      var column, ref, ref1, ref2;

      if (!expr) {
        return null;
      }

      if (expr.type === "literal" && ((ref = expr.valueType) === "id" || ref === "id[]")) {
        return expr.idTable;
      }

      if (expr.type === "id") {
        return expr.table;
      }

      if (expr.type === "scalar") {
        return this.getExprIdTable(expr.expr);
      } // Handle fields


      if (expr.type === "field") {
        column = this.schema.getColumn(expr.table, expr.column);

        if ((column != null ? column.type : void 0) === "join") {
          return column.join.toTable;
        } // DEPRECATED. Remove July 2017


        if ((column != null ? column.type : void 0) === "expr") {
          return this.getExprIdTable(column.expr);
        }

        if ((ref1 = column != null ? column.type : void 0) === "id" || ref1 === "id[]") {
          return column.idTable;
        }

        return null;
      }

      if (expr.type === "variable") {
        return (ref2 = _.findWhere(this.variables, {
          id: expr.variableId
        })) != null ? ref2.idTable : void 0;
      }
    } // Gets the type of an expression

  }, {
    key: "getExprType",
    value: function getExprType(expr) {
      var column, cse, i, len, matchingOpItems, ref, ref1, resultTypes, type, variable;

      if (expr == null || !expr.type) {
        return null;
      }

      switch (expr.type) {
        case "field":
          column = this.schema.getColumn(expr.table, expr.column);

          if (column) {
            if (column.type === "join") {
              if ((ref = column.join.type) === '1-1' || ref === 'n-1') {
                return "id";
              } else {
                return "id[]";
              } // DEPRECATED. Remove July 2017

            } else if (column.type === "expr") {
              return this.getExprType(column.expr);
            }

            return column.type;
          }

          return null;

        case "id":
          return "id";

        case "scalar":
          // Legacy support:
          if (expr.aggr) {
            return this.getExprType({
              type: "op",
              op: expr.aggr,
              table: expr.table,
              exprs: [expr.expr]
            });
          }

          return this.getExprType(expr.expr);

        case "op":
          // Check for single-type ops
          matchingOpItems = this.findMatchingOpItems({
            op: expr.op
          });
          resultTypes = _.uniq(_.compact(_.pluck(matchingOpItems, "resultType")));

          if (resultTypes.length === 1) {
            return resultTypes[0];
          } // Get possible ops


          matchingOpItems = this.findMatchingOpItems({
            op: expr.op,
            lhsExpr: expr.exprs[0]
          }); // Get unique resultTypes

          resultTypes = _.uniq(_.compact(_.pluck(matchingOpItems, "resultType")));

          if (resultTypes.length === 1) {
            return resultTypes[0];
          }

          return null;

        case "literal":
          return expr.valueType;

        case "case":
          ref1 = expr.cases; // Use type of first then that has value

          for (i = 0, len = ref1.length; i < len; i++) {
            cse = ref1[i];
            type = this.getExprType(cse.then);

            if (type) {
              return type;
            }
          }

          return this.getExprType(expr["else"]);

        case "build enumset":
          return "enumset";

        case "score":
          return "number";

        case "count":
          // Deprecated
          return "count";

        case "variable":
          variable = _.findWhere(this.variables, {
            id: expr.variableId
          });

          if (!variable) {
            return null;
          }

          return variable.type;

        default:
          throw new Error("Not implemented for ".concat(expr.type));
      }
    } // Determines the aggregation status of an expression. This is whether the expression is
    // aggregate (like sum, avg, etc) or individual (a regular field-containing expression) or 
    // literal (which is neither, just a number or text). 
    // Invisible second parameter is depth to prevent infinite recursion

  }, {
    key: "getExprAggrStatus",
    value: function getExprAggrStatus(expr) {
      var _this2 = this;

      var column, depth, exprs, getListAggrStatus, variable;

      if (expr == null || !expr.type) {
        return null;
      }

      depth = arguments[1] || 0;

      if (depth > 100) {
        throw new Error("Infinite recursion");
      } // Gets the aggregation status of a series of expressions (takes highest always)


      getListAggrStatus = function getListAggrStatus(exprs) {
        var aggrStatuses; // If has no expressions, is literal

        if (exprs.length === 0) {
          return "literal";
        } // Get highest type


        aggrStatuses = _.map(exprs, function (subExpr) {
          return _this2.getExprAggrStatus(subExpr, depth + 1);
        });

        if (indexOf.call(aggrStatuses, "aggregate") >= 0) {
          return "aggregate";
        }

        if (indexOf.call(aggrStatuses, "individual") >= 0) {
          return "individual";
        }

        if (indexOf.call(aggrStatuses, "literal") >= 0) {
          return "literal";
        }

        return null;
      };

      switch (expr.type) {
        case "id":
        case "scalar":
          return "individual";

        case "field":
          column = this.schema.getColumn(expr.table, expr.column);

          if (column != null ? column.expr : void 0) {
            // This is a slow operation for complex columns. Use weak cache
            // to cache column expression aggregate status
            return exprAggrStatusWeakCache.cacheFunction([this.schema, column.expr], [this.variables], function () {
              return _this2.getExprAggrStatus(column.expr, depth + 1);
            });
          }

          return "individual";

        case "op":
          // If aggregate op
          if (ExprUtils.isOpAggr(expr.op)) {
            return "aggregate";
          }

          return getListAggrStatus(expr.exprs);

        case "literal":
          return "literal";

        case "case":
          // Gather all exprs
          exprs = [expr.input, expr["else"]];
          exprs = exprs.concat(_.map(expr.cases, function (cs) {
            return cs.when;
          }));
          exprs = exprs.concat(_.map(expr.cases, function (cs) {
            return cs.then;
          }));
          return getListAggrStatus(exprs);

        case "score":
          return this.getExprAggrStatus(expr.input, depth + 1);

        case "build enumset":
          // Gather all exprs
          exprs = _.values(expr.values);
          return getListAggrStatus(exprs);

        case "count":
        case "comparison":
        case "logical":
          // Deprecated
          return "individual";

        case "variable":
          variable = _.findWhere(this.variables, {
            id: expr.variableId
          });

          if (!variable) {
            return "literal"; // To prevent crash in cleaning, return something
          }

          if (variable.table) {
            return "individual";
          }

          return "literal";

        default:
          throw new Error("Not implemented for ".concat(expr.type));
      }
    } // Determines if an set of joins are valid

  }, {
    key: "areJoinsValid",
    value: function areJoinsValid(table, joins) {
      var i, j, joinCol, len, ref, t;
      t = table;

      for (i = 0, len = joins.length; i < len; i++) {
        j = joins[i];
        joinCol = this.schema.getColumn(t, j);

        if (!joinCol) {
          return false;
        }

        if ((ref = joinCol.type) === "id" || ref === "id[]") {
          t = joinCol.idTable;
        } else if (joinCol.type === "join") {
          t = joinCol.join.toTable;
        } else {
          return false;
        }
      }

      return true;
    } // Gets the expression table

  }, {
    key: "getExprTable",
    value: function getExprTable(expr) {
      if (!expr) {
        return null;
      }

      return expr.table;
    } // Gets the types that can be formed by aggregating an expression

  }, {
    key: "getAggrTypes",
    value: function getAggrTypes(expr) {
      var aggrOpItems, ref;
      aggrOpItems = this.findMatchingOpItems({
        lhsExpr: expr,
        aggr: true,
        ordered: ((ref = this.schema.getTable(expr.table)) != null ? ref.ordering : void 0) != null
      });
      return _.uniq(_.pluck(aggrOpItems, "resultType"));
    }
  }, {
    key: "localizeString",
    value: function localizeString(name, locale) {
      return ExprUtils.localizeString(name, locale);
    } // Localize a string that is { en: "english word", etc. }. Works with null and plain strings too.

  }, {
    key: "summarizeExpr",
    // Summarizes expression as text
    value: function summarizeExpr(expr, locale) {
      var _this3 = this;

      var opItem, ref, ref1, ref10, ref11, ref2, ref3, ref4, ref5, ref6, ref7, ref8, ref9, variable;

      if (!expr) {
        return "None"; // TODO localize
      }

      switch (expr.type) {
        case "scalar":
          return this.summarizeScalarExpr(expr, locale);

        case "field":
          return this.localizeString((ref = this.schema.getColumn(expr.table, expr.column)) != null ? ref.name : void 0, locale);

        case "id":
          return this.localizeString((ref1 = this.schema.getTable(expr.table)) != null ? ref1.name : void 0, locale);

        case "op":
          // Special case for contains/intersects with literal RHS
          if (expr.op === "contains" && ((ref2 = expr.exprs[1]) != null ? ref2.type : void 0) === "literal" && ((ref3 = expr.exprs[1]) != null ? ref3.valueType : void 0) === "enumset") {
            return this.summarizeExpr(expr.exprs[0], locale) + " includes all of " + this.stringifyLiteralValue("enumset", expr.exprs[1].value, locale, this.getExprEnumValues(expr.exprs[0]));
          }

          if (expr.op === "intersects" && ((ref4 = expr.exprs[1]) != null ? ref4.type : void 0) === "literal" && ((ref5 = expr.exprs[1]) != null ? ref5.valueType : void 0) === "enumset") {
            return this.summarizeExpr(expr.exprs[0], locale) + " includes any of " + this.stringifyLiteralValue("enumset", expr.exprs[1].value, locale, this.getExprEnumValues(expr.exprs[0]));
          } // Special case for = any with literal RHS


          if (expr.op === "= any" && ((ref6 = expr.exprs[1]) != null ? ref6.type : void 0) === "literal" && ((ref7 = expr.exprs[1]) != null ? ref7.valueType : void 0) === "enumset") {
            return this.summarizeExpr(expr.exprs[0], locale) + " is any of " + this.stringifyLiteralValue("enumset", expr.exprs[1].value, locale, this.getExprEnumValues(expr.exprs[0]));
          } // Special case for = with literal RHS


          if (expr.op === "=" && ((ref8 = expr.exprs[1]) != null ? ref8.type : void 0) === "literal" && ((ref9 = expr.exprs[1]) != null ? ref9.valueType : void 0) === "enum") {
            return this.summarizeExpr(expr.exprs[0], locale) + " is " + this.stringifyLiteralValue("enum", expr.exprs[1].value, locale, this.getExprEnumValues(expr.exprs[0]));
          } // Special case for <> with literal RHS


          if (expr.op === "<>" && ((ref10 = expr.exprs[1]) != null ? ref10.type : void 0) === "literal" && ((ref11 = expr.exprs[1]) != null ? ref11.valueType : void 0) === "enum") {
            return this.summarizeExpr(expr.exprs[0], locale) + " is not " + this.stringifyLiteralValue("enum", expr.exprs[1].value, locale, this.getExprEnumValues(expr.exprs[0]));
          } // Special case for count


          if (expr.op === "count") {
            return "Number of " + this.localizeString(this.schema.getTable(expr.table).name, locale);
          }

          opItem = this.findMatchingOpItems({
            op: expr.op
          })[0];

          if (opItem) {
            if (opItem.prefix) {
              return (opItem.prefixLabel || opItem.name) + " " + _.map(expr.exprs, function (e, index) {
                // Only use rhs placeholder if > 0
                if (index === 0) {
                  if (e) {
                    return _this3.summarizeExpr(e, locale);
                  } else {
                    return opItem.lhsPlaceholder || "None";
                  }
                } else {
                  if (e) {
                    return _this3.summarizeExpr(e, locale);
                  } else {
                    return opItem.rhsPlaceholder || "None";
                  }
                }
              }).join(opItem.joiner ? " ".concat(opItem.joiner, " ") : ", ");
            }

            if (expr.exprs.length === 1) {
              return this.summarizeExpr(expr.exprs[0], locale) + " " + opItem.name;
            }

            return _.map(expr.exprs, function (e) {
              return _this3.summarizeExpr(e, locale);
            }).join(" " + opItem.name + " ");
          } else {
            return "";
          }

          break;

        case "case":
          return this.summarizeCaseExpr(expr, locale);

        case "literal":
          return expr.value + "";

        case "score":
          return "Score of " + this.summarizeExpr(expr.input, locale);

        case "build enumset":
          return "Build Enumset";

        case "count":
          return "Count";
        // Deprecated

        case "variable":
          variable = _.findWhere(this.variables, {
            id: expr.variableId
          });
          return this.localizeString(variable != null ? variable.name : void 0, locale);

        default:
          throw new Error("Unsupported type ".concat(expr.type));
      }
    }
  }, {
    key: "summarizeScalarExpr",
    value: function summarizeScalarExpr(expr, locale) {
      var exprType, i, innerExpr, join, joinCol, len, ref, ref1, ref2, ref3, str, t;
      exprType = this.getExprType(expr.expr);
      str = ""; // Add joins

      t = expr.table;
      ref = expr.joins;

      for (i = 0, len = ref.length; i < len; i++) {
        join = ref[i];
        joinCol = this.schema.getColumn(t, join);

        if (joinCol) {
          str += this.localizeString(joinCol.name, locale) + " > ";
        } else {
          str += "NOT FOUND > ";
          break;
        }

        if (joinCol.type === "join") {
          t = joinCol.join.toTable;
        } else if ((ref1 = joinCol.type) === "id" || ref1 === "id[]") {
          t = joinCol.idTable;
        } else {
          str += "INVALID >";
          break;
        }
      } // Special case for id type to be rendered as {last join name}


      if (((ref2 = expr.expr) != null ? ref2.type : void 0) === "id" && !expr.aggr) {
        str = str.substring(0, str.length - 3);
      } else {
        innerExpr = expr.expr; // Handle legacy

        if (expr.aggr) {
          innerExpr = {
            type: "op",
            op: expr.aggr,
            table: (ref3 = expr.expr) != null ? ref3.table : void 0,
            exprs: [expr.expr]
          };
        }

        str += this.summarizeExpr(innerExpr, locale);
      }

      return str;
    }
  }, {
    key: "summarizeCaseExpr",
    value: function summarizeCaseExpr(expr, locale) {
      var c, i, len, ref, str;
      str = "If";
      ref = expr.cases;

      for (i = 0, len = ref.length; i < len; i++) {
        c = ref[i];
        str += " " + this.summarizeExpr(c.when);
        str += " Then " + this.summarizeExpr(c.then);
      }

      if (expr["else"]) {
        str += " Else " + this.summarizeExpr(expr["else"]);
      }

      return str;
    } // Converts a literal value related to an expression to a string, using name of enums. preferEnumCodes tries to use code over name

  }, {
    key: "stringifyExprLiteral",
    value: function stringifyExprLiteral(expr, literal, locale) {
      var preferEnumCodes = arguments.length > 3 && arguments[3] !== undefined ? arguments[3] : false;
      return this.stringifyLiteralValue(this.getExprType(expr), literal, locale, this.getExprEnumValues(expr), preferEnumCodes);
    } // Stringify a literal value of a certain type
    // type is "text", "number", etc.
    // Does not have intelligence to properly handle type id and id[], so just puts in raw id

  }, {
    key: "stringifyLiteralValue",
    value: function stringifyLiteralValue(type, value, locale, enumValues) {
      var preferEnumCodes = arguments.length > 4 && arguments[4] !== undefined ? arguments[4] : false;
      var item;

      if (value == null) {
        return "None"; // TODO localize
      }

      switch (type) {
        case "text":
          return value;

        case "number":
          return "" + value;

        case "enum":
          // Get enumValues
          item = _.findWhere(enumValues, {
            id: value
          });

          if (item) {
            if (preferEnumCodes && item.code) {
              return item.code;
            }

            return ExprUtils.localizeString(item.name, locale);
          }

          return "???";

        case "enumset":
          return _.map(value, function (val) {
            item = _.findWhere(enumValues, {
              id: val
            });

            if (item) {
              if (preferEnumCodes && item.code) {
                return item.code;
              }

              return ExprUtils.localizeString(item.name, locale);
            }

            return "???";
          }).join(', ');

        case "text[]":
          // Parse if string
          if (_.isString(value)) {
            value = JSON.parse(value || "[]");
          }

          return value.join(', ');

        case "date":
          return moment(value, moment.ISO_8601).format("ll");

        case "datetime":
          return moment(value, moment.ISO_8601).format("lll");
      }

      if (value === true) {
        return "True";
      }

      if (value === false) {
        return "False";
      }

      return "".concat(value);
    } // Get all comparison ops (id and name) for a given left hand side type DEPRECATED

  }, {
    key: "getComparisonOps",
    value: function getComparisonOps(lhsType) {
      var ops;
      ops = [];

      switch (lhsType) {
        case "number":
          ops.push({
            id: "=",
            name: "equals"
          });
          ops.push({
            id: ">",
            name: "is greater than"
          });
          ops.push({
            id: ">=",
            name: "is greater or equal to"
          });
          ops.push({
            id: "<",
            name: "is less than"
          });
          ops.push({
            id: "<=",
            name: "is less than or equal to"
          });
          break;

        case "text":
          ops.push({
            id: "= any",
            name: "is one of"
          });
          ops.push({
            id: "=",
            name: "is"
          });
          ops.push({
            id: "~*",
            name: "matches"
          });
          break;

        case "date":
        case "datetime":
          ops.push({
            id: "between",
            name: "between"
          });
          ops.push({
            id: ">",
            name: "after"
          });
          ops.push({
            id: "<",
            name: "before"
          });
          break;

        case "enum":
          ops.push({
            id: "= any",
            name: "is one of"
          });
          ops.push({
            id: "=",
            name: "is"
          });
          break;

        case "boolean":
          ops.push({
            id: "= true",
            name: "is true"
          });
          ops.push({
            id: "= false",
            name: "is false"
          });
      }

      ops.push({
        id: "is null",
        name: "has no value"
      });
      ops.push({
        id: "is not null",
        name: "has a value"
      });
      return ops;
    } // Get the right hand side type for a comparison DEPRECATED

  }, {
    key: "getComparisonRhsType",
    value: function getComparisonRhsType(lhsType, op) {
      if (op === '= true' || op === '= false' || op === 'is null' || op === 'is not null') {
        return null;
      }

      if (op === '= any') {
        if (lhsType === "enum") {
          return 'enum[]';
        } else if (lhsType === "text") {
          return "text[]";
        } else {
          throw new Error("Invalid lhs type for op = any");
        }
      }

      if (op === "between") {
        if (lhsType === "date") {
          return 'daterange';
        }

        if (lhsType === "datetime") {
          return 'datetimerange';
        } else {
          throw new Error("Invalid lhs type for op between");
        }
      }

      return lhsType;
    } // Get a list of fields that are referenced in a an expression
    // Useful to know which fields and joins are used. Includes joins as fields

  }, {
    key: "getReferencedFields",
    value: function getReferencedFields(expr) {
      var cols, column, i, join, k, l, len, len1, len2, len3, len4, m, n, ref, ref1, ref2, ref3, ref4, ref5, subcase, subexpr, table, value;
      cols = [];

      if (!expr) {
        return cols;
      }

      switch (expr.type) {
        case "field":
          cols.push(expr);
          column = this.schema.getColumn(expr.table, expr.column);

          if (column != null ? column.expr : void 0) {
            cols = cols.concat(this.getReferencedFields(column.expr));
          }

          break;

        case "op":
          ref = expr.exprs;

          for (i = 0, len = ref.length; i < len; i++) {
            subexpr = ref[i];
            cols = cols.concat(this.getReferencedFields(subexpr));
          }

          break;

        case "case":
          ref1 = expr.cases;

          for (k = 0, len1 = ref1.length; k < len1; k++) {
            subcase = ref1[k];
            cols = cols.concat(this.getReferencedFields(subcase.when));
            cols = cols.concat(this.getReferencedFields(subcase.then));
          }

          cols = cols.concat(this.getReferencedFields(expr["else"]));
          break;

        case "scalar":
          table = expr.table;
          ref2 = expr.joins;

          for (l = 0, len2 = ref2.length; l < len2; l++) {
            join = ref2[l];
            cols.push({
              type: "field",
              table: table,
              column: join
            });
            column = this.schema.getColumn(table, join); // Handle gracefully

            if (!column) {
              break;
            }

            if (column.type === "join") {
              table = column.join.toTable;
            } else if ((ref3 = column.type) === 'id' || ref3 === 'id[]') {
              table = column.idTable;
            } else {
              break;
            }
          }

          cols = cols.concat(this.getReferencedFields(expr.expr));
          break;

        case "score":
          cols = cols.concat(this.getReferencedFields(expr.input));
          ref4 = _.values(expr.scores);

          for (m = 0, len3 = ref4.length; m < len3; m++) {
            value = ref4[m];
            cols = cols.concat(this.getReferencedFields(value));
          }

          break;

        case "build enumset":
          ref5 = _.values(expr.values);

          for (n = 0, len4 = ref5.length; n < len4; n++) {
            value = ref5[n];
            cols = cols.concat(this.getReferencedFields(value));
          }

      }

      return _.uniq(cols, function (col) {
        return col.table + "/" + col.column;
      });
    }
  }, {
    key: "inlineVariableValues",
    value: function inlineVariableValues(expr, variableValues) {
      var _this4 = this;

      var mapObject, _replacer; // Replace every part of an object, including array members


      mapObject = function mapObject(obj, replacer) {
        if (!obj) {
          return obj;
        }

        if (_.isArray(obj)) {
          return _.map(obj, replacer);
        }

        if (_.isObject(obj)) {
          return _.mapValues(obj, replacer);
        }

        return obj;
      };

      _replacer = function replacer(part) {
        var variable;
        part = mapObject(part, _replacer);

        if (part && part.type === "variable") {
          // Find variable
          variable = _.findWhere(_this4.variables, {
            id: part.variableId
          });

          if (!variable) {
            throw new Error("Variable ".concat(part.variableId, " not found"));
          }

          if (variable.table) {
            return variableValues[variable.id] || null;
          }

          if (variableValues[variable.id] != null) {
            return {
              type: "literal",
              valueType: variable.type,
              value: variableValues[variable.id]
            };
          } else {
            return null;
          }
        }

        return part;
      };

      return mapObject(expr, _replacer);
    }
  }], [{
    key: "isOpAggr",
    value: function isOpAggr(op) {
      return aggrOpItems[op] || false;
    } // Determine if op is prefix

  }, {
    key: "isOpPrefix",
    value: function isOpPrefix(op) {
      return _.findWhere(opItems, {
        op: op,
        prefix: true
      }) != null;
    }
  }, {
    key: "localizeString",
    value: function localizeString(name, locale) {
      if (!name) {
        return name;
      } // Simple string


      if (typeof name === "string") {
        return name;
      }

      if (locale && name[locale] != null) {
        return name[locale];
      }

      if (name._base && name[name._base] != null) {
        return name[name._base];
      } // Fall back to English


      if (name.en != null) {
        return name.en;
      }

      return null;
    } // Combine n expressions together by and

  }, {
    key: "andExprs",
    value: function andExprs(table) {
      for (var _len = arguments.length, exprs = new Array(_len > 1 ? _len - 1 : 0), _key = 1; _key < _len; _key++) {
        exprs[_key - 1] = arguments[_key];
      }

      exprs = _.map(exprs, function (expr) {
        if ((expr != null ? expr.type : void 0) === "op" && expr.op === "and") {
          return expr.exprs;
        } else {
          return expr;
        }
      });
      exprs = _.compact(_.flatten(exprs));

      if (exprs.length === 0) {
        return null;
      }

      if (exprs.length === 1) {
        return exprs[0];
      }

      return {
        type: "op",
        op: "and",
        table: table,
        exprs: exprs
      };
    }
  }]);
  return ExprUtils;
}(); // # Get a list of column ids of expression table that are referenced in a an expression
// # Useful to know which fields and joins are used. Does not follow joins, beyond including 
// # the first join (which is a column in the start table).
// # Function does not require a schema, so schema can be null/undefined in constructor
// getImmediateReferencedColumns: (expr) ->
//   cols = []
//   if not expr
//     return cols
//   switch expr.type
//     when "field"
//       cols.push(expr.column)
//     when "op"
//       for subexpr in expr.exprs
//         cols = cols.concat(@getImmediateReferencedColumns(subexpr))
//     when "case"
//       for subcase in expr.cases
//         cols = cols.concat(@getImmediateReferencedColumns(subcase.when))
//         cols = cols.concat(@getImmediateReferencedColumns(subcase.then))
//       cols = cols.concat(@getImmediateReferencedColumns(expr.else))
//   return _.uniq(cols)
// Setup op items
// opItems are a list of ops for various types:
// op: e.g. "="
// name: e.g. "is"
// resultType: resulting type from op. e.g. "boolean"
// exprTypes: array of types of expressions required for arguments
// moreExprType: type of n more expressions (like "and" that takes n arguments)
// prefix: true if name goes before LHS value
// prefixLabel: overrides name when displayed as prefix
// lhsCond: optional condition function on LHS expr that tests if applicable (for "within" which only applies to hierarchical tables)
// rhsLiteral: prefer rhs literal
// joiner: string to put between exprs when prefix type
// aggr: true if aggregating (e.g. sum)
// ordered: for aggr = true if table must be have ordering
// lhsPlaceholder: placeholder for lhs expression
// rhsPlaceholder: placeholder for rhs expression


opItems = []; // Which op items are aggregate (key = op, value = true)

aggrOpItems = {}; // opItems grouped by op

groupedOpItems = {}; // Adds an op item (particular combination of operands types with an operator)
// exprTypes is a list of types for expressions. moreExprType is the type of further N expressions, if allowed

addOpItem = function addOpItem(item) {
  var list;
  opItems.push(_.defaults(item, {
    prefix: false,
    rhsLiteral: true,
    aggr: false,
    ordered: false
  }));

  if (item.aggr) {
    aggrOpItems[item.op] = true;
  }

  list = groupedOpItems[item.op] || [];
  list.push(item);
  return groupedOpItems[item.op] = list;
}; // TODO n?


addOpItem({
  op: "= any",
  name: "is any of",
  resultType: "boolean",
  exprTypes: ["text", "text[]"]
});
addOpItem({
  op: "= any",
  name: "is any of",
  resultType: "boolean",
  exprTypes: ["enum", "enumset"]
});
addOpItem({
  op: "contains",
  name: "includes all of",
  resultType: "boolean",
  exprTypes: ["enumset", "enumset"]
});
addOpItem({
  op: "intersects",
  name: "includes any of",
  resultType: "boolean",
  exprTypes: ["enumset", "enumset"]
});
addOpItem({
  op: "contains",
  name: "includes all of",
  resultType: "boolean",
  exprTypes: ["text[]", "text[]"]
});
addOpItem({
  op: "intersects",
  name: "includes any of",
  resultType: "boolean",
  exprTypes: ["text[]", "text[]"]
}); // Add relative dates

relativeDateOps = [['thisyear', 'is this year'], ['lastyear', 'is last year'], ['thismonth', 'is this month'], ['lastmonth', 'is last month'], ['today', 'is today'], ['yesterday', 'is yesterday'], ['last24hours', 'is in last 24 hours'], ['last7days', 'is in last 7 days'], ['last30days', 'is in last 30 days'], ['last365days', 'is in last 365 days'], ['last3months', 'is in last 3 months'], ['last6months', 'is in last 6 months'], ['last12months', 'is in last 12 months'], ['future', 'is in the future'], ['notfuture', 'is not in the future']];

for (i = 0, len = relativeDateOps.length; i < len; i++) {
  relativeDateOp = relativeDateOps[i];
  addOpItem({
    op: relativeDateOp[0],
    name: relativeDateOp[1],
    resultType: "boolean",
    exprTypes: ['date']
  });
  addOpItem({
    op: relativeDateOp[0],
    name: relativeDateOp[1],
    resultType: "boolean",
    exprTypes: ['datetime']
  });
} // Add in ranges


addOpItem({
  op: "between",
  name: "is between",
  resultType: "boolean",
  exprTypes: ["date", "date", "date"]
});
addOpItem({
  op: "between",
  name: "is between",
  resultType: "boolean",
  exprTypes: ["datetime", "datetime", "datetime"]
});
addOpItem({
  op: "=",
  name: "is",
  resultType: "boolean",
  exprTypes: ["number", "number"]
});
addOpItem({
  op: "=",
  name: "is",
  resultType: "boolean",
  exprTypes: ["text", "text"]
});
addOpItem({
  op: "=",
  name: "is",
  resultType: "boolean",
  exprTypes: ["enum", "enum"]
});
addOpItem({
  op: "=",
  name: "is",
  resultType: "boolean",
  exprTypes: ["date", "date"]
});
addOpItem({
  op: "=",
  name: "is",
  resultType: "boolean",
  exprTypes: ["datetime", "datetime"]
});
addOpItem({
  op: "=",
  name: "is",
  resultType: "boolean",
  exprTypes: ["boolean", "boolean"]
});
addOpItem({
  op: "<>",
  name: "is not",
  resultType: "boolean",
  exprTypes: ["text", "text"]
});
addOpItem({
  op: "<>",
  name: "is not",
  resultType: "boolean",
  exprTypes: ["enum", "enum"]
});
addOpItem({
  op: "<>",
  name: "is not",
  resultType: "boolean",
  exprTypes: ["date", "date"]
});
addOpItem({
  op: "<>",
  name: "is not",
  resultType: "boolean",
  exprTypes: ["datetime", "datetime"]
});
addOpItem({
  op: "<>",
  name: "is not",
  resultType: "boolean",
  exprTypes: ["boolean", "boolean"]
});
addOpItem({
  op: "<>",
  name: "is not",
  resultType: "boolean",
  exprTypes: ["number", "number"]
});
addOpItem({
  op: ">",
  name: "is greater than",
  resultType: "boolean",
  exprTypes: ["number", "number"]
});
addOpItem({
  op: "<",
  name: "is less than",
  resultType: "boolean",
  exprTypes: ["number", "number"]
});
addOpItem({
  op: ">=",
  name: "is greater than or equal to",
  resultType: "boolean",
  exprTypes: ["number", "number"]
});
addOpItem({
  op: "<=",
  name: "is less than or equal to",
  resultType: "boolean",
  exprTypes: ["number", "number"]
});
ref = ['date', 'datetime'];

for (k = 0, len1 = ref.length; k < len1; k++) {
  type1 = ref[k];
  ref1 = ['date', 'datetime'];

  for (l = 0, len2 = ref1.length; l < len2; l++) {
    type2 = ref1[l];
    addOpItem({
      op: ">",
      name: "is after",
      resultType: "boolean",
      exprTypes: [type1, type2]
    });
    addOpItem({
      op: "<",
      name: "is before",
      resultType: "boolean",
      exprTypes: [type1, type2]
    });
    addOpItem({
      op: ">=",
      name: "is after or same as",
      resultType: "boolean",
      exprTypes: [type1, type2]
    });
    addOpItem({
      op: "<=",
      name: "is before or same as",
      resultType: "boolean",
      exprTypes: [type1, type2]
    });
  }
}

addOpItem({
  op: "between",
  name: "is between",
  resultType: "boolean",
  exprTypes: ["number", "number", "number"]
});
addOpItem({
  op: "round",
  name: "Round",
  desc: "Round a number to closest whole number",
  resultType: "number",
  exprTypes: ["number"],
  prefix: true
});
addOpItem({
  op: "floor",
  name: "Floor",
  desc: "Round a number down",
  resultType: "number",
  exprTypes: ["number"],
  prefix: true
});
addOpItem({
  op: "ceiling",
  name: "Ceiling",
  desc: "Round a number up",
  resultType: "number",
  exprTypes: ["number"],
  prefix: true
});
addOpItem({
  op: "latitude",
  name: "Latitude of",
  desc: "Get latitude in degrees of a location",
  resultType: "number",
  exprTypes: ["geometry"],
  prefix: true
});
addOpItem({
  op: "longitude",
  name: "Longitude of",
  desc: "Get longitude in degrees of a location",
  resultType: "number",
  exprTypes: ["geometry"],
  prefix: true
});
addOpItem({
  op: "distance",
  name: "Distance between",
  desc: "Get distance in meters between two locations",
  resultType: "number",
  exprTypes: ["geometry", "geometry"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
}); // And/or is a list of booleans

addOpItem({
  op: "and",
  name: "and",
  resultType: "boolean",
  exprTypes: [],
  moreExprType: "boolean"
});
addOpItem({
  op: "or",
  name: "or",
  resultType: "boolean",
  exprTypes: [],
  moreExprType: "boolean"
});
ref2 = ['+', '*'];

for (m = 0, len3 = ref2.length; m < len3; m++) {
  op = ref2[m];
  addOpItem({
    op: op,
    name: op,
    resultType: "number",
    exprTypes: [],
    moreExprType: "number"
  });
}

addOpItem({
  op: "-",
  name: "-",
  resultType: "number",
  exprTypes: ["number", "number"]
});
addOpItem({
  op: "/",
  name: "/",
  resultType: "number",
  exprTypes: ["number", "number"]
}); // Date subtraction

addOpItem({
  op: "days difference",
  name: "Days between",
  desc: "Get the number of days between two dates",
  resultType: "number",
  exprTypes: ["date", "date"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
});
addOpItem({
  op: "days difference",
  name: "Days between",
  desc: "Get the number of days between two dates",
  resultType: "number",
  exprTypes: ["datetime", "datetime"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
});
addOpItem({
  op: "days difference",
  name: "Days between",
  desc: "Get the number of days between two dates",
  resultType: "number",
  exprTypes: ["date", "datetime"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
});
addOpItem({
  op: "days difference",
  name: "Days between",
  desc: "Get the number of days between two dates",
  resultType: "number",
  exprTypes: ["datetime", "date"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
});
addOpItem({
  op: "months difference",
  name: "Months between",
  desc: "Get the number of months between two dates",
  resultType: "number",
  exprTypes: ["date", "date"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
});
addOpItem({
  op: "months difference",
  name: "Months between",
  desc: "Get the number of months between two dates",
  resultType: "number",
  exprTypes: ["datetime", "datetime"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
});
addOpItem({
  op: "months difference",
  name: "Months between",
  desc: "Get the number of months between two dates",
  resultType: "number",
  exprTypes: ["date", "datetime"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
});
addOpItem({
  op: "months difference",
  name: "Months between",
  desc: "Get the number of months between two dates",
  resultType: "number",
  exprTypes: ["datetime", "date"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
});
addOpItem({
  op: "years difference",
  name: "Years between",
  desc: "Get the number of years between two dates",
  resultType: "number",
  exprTypes: ["date", "date"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
});
addOpItem({
  op: "years difference",
  name: "Years between",
  desc: "Get the number of years between two dates",
  resultType: "number",
  exprTypes: ["datetime", "datetime"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
});
addOpItem({
  op: "years difference",
  name: "Years between",
  desc: "Get the number of years between two dates",
  resultType: "number",
  exprTypes: ["date", "datetime"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
});
addOpItem({
  op: "years difference",
  name: "Years between",
  desc: "Get the number of years between two dates",
  resultType: "number",
  exprTypes: ["datetime", "date"],
  prefix: true,
  rhsLiteral: false,
  joiner: "and"
});
addOpItem({
  op: "days since",
  name: "Days since",
  desc: "Get number of days from a date to the present",
  resultType: "number",
  exprTypes: ["date"],
  prefix: true,
  rhsLiteral: false
});
addOpItem({
  op: "days since",
  name: "Days since",
  desc: "Get number of days from a date to the present",
  resultType: "number",
  exprTypes: ["datetime"],
  prefix: true,
  rhsLiteral: false
});
addOpItem({
  op: "month",
  name: "Month",
  desc: "Month of year",
  resultType: "enum",
  exprTypes: ["date"],
  prefix: true,
  rhsLiteral: false
});
addOpItem({
  op: "month",
  name: "Month",
  desc: "Month of year",
  resultType: "enum",
  exprTypes: ["datetime"],
  prefix: true,
  rhsLiteral: false
});
addOpItem({
  op: "yearmonth",
  name: "Year and Month",
  desc: "Date of start of month",
  resultType: "date",
  exprTypes: ["date"],
  prefix: true,
  rhsLiteral: false
});
addOpItem({
  op: "yearmonth",
  name: "Year and Month",
  desc: "Date of start of month",
  resultType: "date",
  exprTypes: ["datetime"],
  prefix: true,
  rhsLiteral: false
});
addOpItem({
  op: "year",
  name: "Year",
  desc: "Date of start of year",
  resultType: "date",
  exprTypes: ["date"],
  prefix: true,
  rhsLiteral: false
});
addOpItem({
  op: "year",
  name: "Year",
  desc: "Date of start of year",
  resultType: "date",
  exprTypes: ["datetime"],
  prefix: true,
  rhsLiteral: false
});
addOpItem({
  op: "weekofmonth",
  name: "Week of month",
  desc: "Week within the month",
  resultType: "enum",
  exprTypes: ["date"],
  prefix: true,
  rhsLiteral: false
});
addOpItem({
  op: "weekofmonth",
  name: "Week of month",
  desc: "Week within the month",
  resultType: "enum",
  exprTypes: ["datetime"],
  prefix: true,
  rhsLiteral: false
});
addOpItem({
  op: "dayofmonth",
  name: "Day of month",
  desc: "Day within the month (1-31)",
  resultType: "enum",
  exprTypes: ["date"],
  prefix: true,
  rhsLiteral: false
});
addOpItem({
  op: "dayofmonth",
  name: "Day of month",
  desc: "Day within the month (1-31)",
  resultType: "enum",
  exprTypes: ["datetime"],
  prefix: true,
  rhsLiteral: false
});
addOpItem({
  op: "yearquarter",
  name: "Year/Quarter",
  desc: "Year and quarter of a date",
  resultType: "enum",
  exprTypes: ["date"],
  prefix: true,
  rhsLiteral: false
});
addOpItem({
  op: "yearquarter",
  name: "Year/Quarter",
  desc: "Year and quarter of a date",
  resultType: "enum",
  exprTypes: ["datetime"],
  prefix: true,
  rhsLiteral: false
});
addOpItem({
  op: "yearweek",
  name: "Year/Week",
  desc: "Year and week of a date",
  resultType: "enum",
  exprTypes: ["date"],
  prefix: true,
  rhsLiteral: false
});
addOpItem({
  op: "yearweek",
  name: "Year/Week",
  desc: "Year and week of a date",
  resultType: "enum",
  exprTypes: ["datetime"],
  prefix: true,
  rhsLiteral: false
});
addOpItem({
  op: "weekofyear",
  name: "Week",
  desc: "Week of a date",
  resultType: "enum",
  exprTypes: ["date"],
  prefix: true,
  rhsLiteral: false
});
addOpItem({
  op: "weekofyear",
  name: "Week",
  desc: "Week of a date",
  resultType: "enum",
  exprTypes: ["datetime"],
  prefix: true,
  rhsLiteral: false
});
addOpItem({
  op: "within",
  name: "is within",
  resultType: "boolean",
  exprTypes: ["id", "id"],
  lhsCond: function lhsCond(lhsExpr, exprUtils) {
    var lhsIdTable;
    lhsIdTable = exprUtils.getExprIdTable(lhsExpr);

    if (lhsIdTable) {
      return exprUtils.schema.getTable(lhsIdTable).ancestry != null || exprUtils.schema.getTable(lhsIdTable).ancestryTable != null;
    }

    return false;
  }
});
addOpItem({
  op: "=",
  name: "is",
  resultType: "boolean",
  exprTypes: ["id", "id"]
});
addOpItem({
  op: "<>",
  name: "is not",
  resultType: "boolean",
  exprTypes: ["id", "id"]
});
addOpItem({
  op: "= any",
  name: "is any of",
  resultType: "boolean",
  exprTypes: ["id", "id[]"]
});
ref3 = ['text', 'number', 'enum', 'enumset', 'boolean', 'date', 'datetime', 'geometry', 'id'];

for (n = 0, len4 = ref3.length; n < len4; n++) {
  type = ref3[n];
  addOpItem({
    op: "last",
    name: "Latest",
    desc: "Get latest value when there are multiple",
    resultType: type,
    exprTypes: [type],
    prefix: true,
    aggr: true,
    ordered: true
  });
  addOpItem({
    op: "last where",
    name: "Latest where",
    desc: "Get latest value that matches a condition",
    resultType: type,
    exprTypes: [type, "boolean"],
    prefix: true,
    prefixLabel: "Latest",
    aggr: true,
    ordered: true,
    rhsLiteral: false,
    joiner: "where",
    rhsPlaceholder: "All"
  });
  addOpItem({
    op: "previous",
    name: "Previous",
    desc: "Get 2nd latest value when there are multiple",
    resultType: type,
    exprTypes: [type],
    prefix: true,
    aggr: true,
    ordered: true
  });
}

addOpItem({
  op: "sum",
  name: "Total",
  desc: "Add all values together",
  resultType: "number",
  exprTypes: ["number"],
  prefix: true,
  aggr: true
});
addOpItem({
  op: "avg",
  name: "Average",
  desc: "Average all values together",
  resultType: "number",
  exprTypes: ["number"],
  prefix: true,
  aggr: true
});
ref4 = ['number', 'date', 'datetime'];

for (o = 0, len5 = ref4.length; o < len5; o++) {
  type = ref4[o];
  addOpItem({
    op: "min",
    name: "Minimum",
    desc: "Get smallest value",
    resultType: type,
    exprTypes: [type],
    prefix: true,
    aggr: true
  });
  addOpItem({
    op: "min where",
    name: "Minimum where",
    desc: "Get smallest value that matches a condition",
    resultType: type,
    exprTypes: [type, "boolean"],
    prefix: true,
    prefixLabel: "Minimum",
    aggr: true,
    rhsLiteral: false,
    joiner: "of",
    rhsPlaceholder: "All"
  });
  addOpItem({
    op: "max",
    name: "Maximum",
    desc: "Get largest value",
    resultType: type,
    exprTypes: [type],
    prefix: true,
    aggr: true
  });
  addOpItem({
    op: "max where",
    name: "Maximum where",
    desc: "Get largest value that matches a condition",
    resultType: type,
    exprTypes: [type, "boolean"],
    prefix: true,
    prefixLabel: "Maximum",
    aggr: true,
    rhsLiteral: false,
    joiner: "of",
    rhsPlaceholder: "All"
  });
}

addOpItem({
  op: "percent where",
  name: "Percent where",
  desc: "Get percent of items that match a condition",
  resultType: "number",
  exprTypes: ["boolean", "boolean"],
  prefix: true,
  aggr: true,
  rhsLiteral: false,
  joiner: "of",
  rhsPlaceholder: "All"
});
addOpItem({
  op: "count where",
  name: "Number where",
  desc: "Get number of items that match a condition",
  resultType: "number",
  exprTypes: ["boolean"],
  prefix: true,
  aggr: true
});
addOpItem({
  op: "sum where",
  name: "Total where",
  desc: "Add together only values that match a condition",
  resultType: "number",
  exprTypes: ["number", "boolean"],
  prefix: true,
  prefixLabel: "Total",
  aggr: true,
  rhsLiteral: false,
  joiner: "where",
  rhsPlaceholder: "All"
});
addOpItem({
  op: "within any",
  name: "is within any of",
  resultType: "boolean",
  exprTypes: ["id", "id[]"],
  lhsCond: function lhsCond(lhsExpr, exprUtils) {
    var lhsIdTable;
    lhsIdTable = exprUtils.getExprIdTable(lhsExpr);

    if (lhsIdTable) {
      return exprUtils.schema.getTable(lhsIdTable).ancestry != null || exprUtils.schema.getTable(lhsIdTable).ancestryTable != null;
    }

    return false;
  }
});
addOpItem({
  op: "array_agg",
  name: "Make list of",
  desc: "Aggregates results into a list",
  resultType: "text[]",
  exprTypes: ["text"],
  prefix: true,
  aggr: true
});
addOpItem({
  op: "contains",
  name: "includes all of",
  resultType: "boolean",
  exprTypes: ["id[]", "id[]"]
});
addOpItem({
  op: "intersects",
  name: "includes any of",
  resultType: "boolean",
  exprTypes: ["id[]", "id[]"]
});
addOpItem({
  op: "count",
  name: "Total Number",
  desc: "Get total number of items",
  resultType: "number",
  exprTypes: [],
  prefix: true,
  aggr: true
});
addOpItem({
  op: "percent",
  name: "Percent of Total",
  desc: "Percent of all items",
  resultType: "number",
  exprTypes: [],
  prefix: true,
  aggr: true
});
addOpItem({
  op: "~*",
  name: "matches",
  resultType: "boolean",
  exprTypes: ["text", "text"]
});
addOpItem({
  op: "not",
  name: "Not",
  desc: "Opposite of a value",
  resultType: "boolean",
  exprTypes: ["boolean"],
  prefix: true
});
ref5 = ['text', 'number', 'enum', 'enumset', 'boolean', 'date', 'datetime', 'geometry', 'image', 'imagelist', 'id', 'json', 'dataurl'];

for (p = 0, len6 = ref5.length; p < len6; p++) {
  type = ref5[p];
  addOpItem({
    op: "is null",
    name: "is blank",
    resultType: "boolean",
    exprTypes: [type]
  });
  addOpItem({
    op: "is not null",
    name: "is not blank",
    resultType: "boolean",
    exprTypes: [type]
  });
}

ref6 = ['id', 'text', 'date'];

for (q = 0, len7 = ref6.length; q < len7; q++) {
  type = ref6[q];
  addOpItem({
    op: "count distinct",
    name: "Number of unique",
    desc: "Count number of unique values",
    resultType: "number",
    exprTypes: [type],
    prefix: true,
    aggr: true
  });
}

addOpItem({
  op: "length",
  name: "Number of values in",
  desc: "Advanced: number of values selected in a multi-choice field",
  resultType: "number",
  exprTypes: ["enumset"],
  prefix: true
});
addOpItem({
  op: "length",
  name: "Number of values in",
  desc: "Advanced: number of images present",
  resultType: "number",
  exprTypes: ["imagelist"],
  prefix: true
});
addOpItem({
  op: "length",
  name: "Number of values in",
  desc: "Advanced: number of items present in a text list",
  resultType: "number",
  exprTypes: ["text[]"],
  prefix: true
});
addOpItem({
  op: "line length",
  name: "Length of line",
  desc: "Length of a line shape in meters",
  resultType: "number",
  exprTypes: ["geometry"],
  prefix: true
});
ref7 = ['id'];

for (r = 0, len8 = ref7.length; r < len8; r++) {
  type = ref7[r];
  addOpItem({
    op: "is latest",
    name: "Is latest for each",
    desc: "Only include latest item for each of something",
    resultType: "boolean",
    exprTypes: [type, "boolean"],
    prefix: true,
    ordered: true,
    aggr: false,
    rhsLiteral: false,
    joiner: "where",
    rhsPlaceholder: "All"
  });
}

addOpItem({
  op: "current date",
  name: "Today",
  desc: "Advanced: current date. Do not use in comparisons",
  resultType: "date",
  exprTypes: [],
  prefix: true
});
addOpItem({
  op: "current datetime",
  name: "Now",
  desc: "Advanced: current datetime. Do not use in comparisons",
  resultType: "datetime",
  exprTypes: [],
  prefix: true
});
addOpItem({
  op: "to text",
  name: "Convert to text",
  desc: "Advanced: convert a choice or number type to a text value",
  resultType: "text",
  exprTypes: ["enum"],
  prefix: true
});
addOpItem({
  op: "to text",
  name: "Convert to text",
  desc: "Advanced: convert a choice or number type to a text value",
  resultType: "text",
  exprTypes: ["number"],
  prefix: true
});
addOpItem({
  op: "to date",
  name: "Convert to date",
  desc: "Convert a datetime to a date",
  resultType: "date",
  exprTypes: ["datetime"],
  prefix: true
});
addOpItem({
  op: "least",
  name: "Least of",
  desc: "Takes the smallest of several numbers",
  resultType: "number",
  exprTypes: ["number", "number"],
  moreExprType: "number",
  prefix: true,
  joiner: ", "
});
addOpItem({
  op: "greatest",
  name: "Greatest of",
  desc: "Takes the largest of several numbers",
  resultType: "number",
  exprTypes: ["number", "number"],
  moreExprType: "number",
  prefix: true,
  joiner: ", "
});