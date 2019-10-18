"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var ExprEvaluator,
    ExprUtils,
    _,
    async,
    deg2rad,
    getDistanceFromLatLngInM,
    moment,
    indexOf = [].indexOf;

_ = require('lodash');
moment = require('moment');
async = require('async');
ExprUtils = require('./ExprUtils'); // Evaluates an expression given a context.
// context is a plain object that contains:
// row: current row (see below)
// rows: array of rows (for aggregate expressions. See below for row definition)
// a row is a plain object that has the following functions as properties:
//  getPrimaryKey(callback) : gets primary key of row. callback is called with (error, value)
//  getField(columnId, callback) : gets the value of a column. callback is called with (error, value)
// For joins, getField will get array of rows for 1-n and n-n joins and a row for n-1 and 1-1 joins

module.exports = ExprEvaluator =
/*#__PURE__*/
function () {
  // Schema is optional and used for ordering, "to text" function and expression columns
  function ExprEvaluator(schema, locale) {
    var variables = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : [];
    var variableValues = arguments.length > 3 && arguments[3] !== undefined ? arguments[3] : {};
    (0, _classCallCheck2["default"])(this, ExprEvaluator);
    this.schema = schema;
    this.locale = locale;
    this.variables = variables;
    this.variableValues = variableValues;
  } // Evaluate an expression


  (0, _createClass2["default"])(ExprEvaluator, [{
    key: "evaluate",
    value: function evaluate(expr, context, callback) {
      var _this = this;

      var ref; // Handle promise case

      if (!callback) {
        return new Promise(function (resolve, reject) {
          return _this.evaluate(expr, context, function (error, result) {
            if (error) {
              return reject(error);
            } else {
              return resolve(result);
            }
          });
        });
      }

      if (expr == null) {
        return callback(null, null);
      }

      switch (expr.type) {
        case "field":
          // If schema is present and column is an expression column, use that
          if (this.schema && ((ref = this.schema.getColumn(expr.table, expr.column)) != null ? ref.expr : void 0)) {
            return this.evaluate(this.schema.getColumn(expr.table, expr.column).expr, context, callback);
          }

          return context.row.getField(expr.column, function (error, value) {
            if (error) {
              return callback(error);
            } // Handle row case


            if (value && value.getPrimaryKey) {
              return value.getPrimaryKey(callback);
            } // Handle rows case


            if (value && _.isArray(value) && value.length > 0 && value[0].getPrimaryKey) {
              // Map to id
              async.map(value, function (item, cb) {
                return item.getPrimaryKey(cb);
              }, function (error, ids) {
                return callback(error, ids);
              });
              return;
            }

            return callback(null, value);
          });

        case "literal":
          return callback(null, expr.value);

        case "op":
          return this.evaluateOp(expr.table, expr.op, expr.exprs, context, callback);

        case "id":
          return context.row.getPrimaryKey(callback);

        case "case":
          return this.evaluateCase(expr, context, callback);

        case "scalar":
          return this.evaluateScalar(expr, context, callback);

        case "score":
          return this.evaluateScore(expr, context, callback);

        case "build enumset":
          return this.evaluateBuildEnumset(expr, context, callback);

        case "variable":
          return this.evaluateVariable(expr, context, callback);

        default:
          throw new Error("Unsupported expression type ".concat(expr.type));
      }
    }
  }, {
    key: "evaluateOp",
    value: function evaluateOp(table, op, exprs, context, callback) {
      var _this2 = this;

      // If aggregate op
      if (ExprUtils.isOpAggr(op)) {
        this.evaluteAggrOp(table, op, exprs, context, callback);
        return;
      } // is latest is special case for window-like function


      if (op === "is latest") {
        this.evaluateIsLatest(table, exprs, context, callback);
        return;
      } // Evaluate exprs


      return async.map(exprs, function (expr, cb) {
        return _this2.evaluate(expr, context, cb);
      }, function (error, values) {
        var result;

        if (error) {
          return callback(error);
        }

        try {
          result = _this2.evaluateOpValues(op, exprs, values);
          return callback(null, result);
        } catch (error1) {
          error = error1;
          return callback(error);
        }
      });
    } // Synchronous evaluation

  }, {
    key: "evaluateOpValues",
    value: function evaluateOpValues(op, exprs, values) {
      var coords, date, exprUtils, hasNull, i, j, point, point1, point2, ref, total; // Check if has null argument

      hasNull = _.any(values, function (v) {
        return v == null;
      });

      switch (op) {
        case "+":
          return _.reduce(values, function (acc, value) {
            return acc + (value != null ? value : 0);
          });

        case "*":
          if (hasNull) {
            return null;
          }

          return _.reduce(values, function (acc, value) {
            return acc * value;
          });

        case "-":
          if (hasNull) {
            return null;
          }

          return values[0] - values[1];

        case "/":
          if (hasNull) {
            return null;
          }

          if (values[1] === 0) {
            return null;
          }

          return values[0] / values[1];

        case "and":
          if (values.length === 0) {
            return null;
          }

          return _.reduce(values, function (acc, value) {
            return acc && value;
          });

        case "or":
          if (values.length === 0) {
            return null;
          }

          return _.reduce(values, function (acc, value) {
            return acc || value;
          });

        case "not":
          if (hasNull) {
            return true;
          }

          return !values[0];

        case "=":
          if (hasNull) {
            return null;
          }

          return values[0] === values[1];

        case "<>":
          if (hasNull) {
            return null;
          }

          return values[0] !== values[1];

        case ">":
          if (hasNull) {
            return null;
          }

          return values[0] > values[1];

        case ">=":
          if (hasNull) {
            return null;
          }

          return values[0] >= values[1];

        case "<":
          if (hasNull) {
            return null;
          }

          return values[0] < values[1];

        case "<=":
          if (hasNull) {
            return null;
          }

          return values[0] <= values[1];

        case "= false":
          if (hasNull) {
            return null;
          }

          return values[0] === false;

        case "is null":
          return values[0] == null;

        case "is not null":
          return values[0] != null;

        case "~*":
          if (hasNull) {
            return null;
          }

          return values[0].match(new RegExp(values[1], "i")) != null;

        case "= any":
          if (hasNull) {
            return null;
          }

          return _.contains(values[1], values[0]);

        case "contains":
          if (hasNull) {
            return null;
          }

          return _.difference(values[1], values[0]).length === 0;

        case "intersects":
          if (hasNull) {
            return null;
          }

          return _.intersection(values[0], values[1]).length > 0;

        case "length":
          if (hasNull) {
            return 0;
          }

          return values[0].length;

        case "between":
          if (hasNull) {
            return null;
          }

          return values[0] >= values[1] && values[0] <= values[2];

        case "round":
          if (hasNull) {
            return null;
          }

          return Math.round(values[0]);

        case "floor":
          if (hasNull) {
            return null;
          }

          return Math.floor(values[0]);

        case "ceiling":
          if (hasNull) {
            return null;
          }

          return Math.ceil(values[0]);

        case "days difference":
          if (hasNull) {
            return null;
          }

          return moment(values[0], moment.ISO_8601).diff(moment(values[1], moment.ISO_8601)) / 24 / 3600 / 1000;

        case "months difference":
          if (hasNull) {
            return null;
          }

          return moment(values[0], moment.ISO_8601).diff(moment(values[1], moment.ISO_8601)) / 24 / 3600 / 1000 / 30.5;

        case "years difference":
          if (hasNull) {
            return null;
          }

          return moment(values[0], moment.ISO_8601).diff(moment(values[1], moment.ISO_8601)) / 24 / 3600 / 1000 / 365;

        case "days since":
          if (hasNull) {
            return null;
          }

          return moment().diff(moment(values[0], moment.ISO_8601)) / 24 / 3600 / 1000;

        case "weekofmonth":
          if (hasNull) {
            return null;
          }

          return Math.floor((moment(values[0], moment.ISO_8601).date() - 1) / 7) + 1 + "";
        // Make string

        case "dayofmonth":
          if (hasNull) {
            return null;
          }

          return moment(values[0], moment.ISO_8601).format("DD");

        case "month":
          if (hasNull) {
            return null;
          }

          return values[0].substr(5, 2);

        case "yearmonth":
          if (hasNull) {
            return null;
          }

          return values[0].substr(0, 7) + "-01";

        case "year":
          if (hasNull) {
            return null;
          }

          return values[0].substr(0, 4) + "-01-01";

        case "today":
          if (hasNull) {
            return null;
          }

          date = values[0];
          return moment(date, moment.ISO_8601).format("YYYY-MM-DD") === moment().format("YYYY-MM-DD");

        case "yesterday":
          if (hasNull) {
            return null;
          }

          date = values[0];
          return moment(date, moment.ISO_8601).add(1, "days").format("YYYY-MM-DD") === moment().format("YYYY-MM-DD");

        case "thismonth":
          if (hasNull) {
            return null;
          }

          date = values[0];
          return moment(date, moment.ISO_8601).format("YYYY-MM") === moment().format("YYYY-MM");

        case "lastmonth":
          if (hasNull) {
            return null;
          }

          date = values[0];
          return moment(date, moment.ISO_8601).add(1, "months").format("YYYY-MM") === moment().format("YYYY-MM");

        case "thisyear":
          if (hasNull) {
            return null;
          }

          date = values[0];
          return moment(date, moment.ISO_8601).format("YYYY") === moment().format("YYYY");

        case "lastyear":
          if (hasNull) {
            return null;
          }

          date = values[0];
          return moment(date, moment.ISO_8601).add(1, "years").format("YYYY") === moment().format("YYYY");

        case "last24hours":
          if (hasNull) {
            return null;
          }

          date = values[0];
          return moment(date, moment.ISO_8601).isSameOrBefore(moment()) && moment(date, moment.ISO_8601).isAfter(moment().subtract(24, "hours"));

        case "last7days":
          if (hasNull) {
            return null;
          }

          date = values[0];
          return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) && moment(date, moment.ISO_8601).isAfter(moment().subtract(7, "days"));

        case "last30days":
          if (hasNull) {
            return null;
          }

          date = values[0];
          return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) && moment(date, moment.ISO_8601).isAfter(moment().subtract(30, "days"));

        case "last365days":
          if (hasNull) {
            return null;
          }

          date = values[0];
          return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) && moment(date, moment.ISO_8601).isAfter(moment().subtract(365, "days"));

        case "last12months":
          if (hasNull) {
            return null;
          }

          date = values[0];
          return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) && moment(date, moment.ISO_8601).isAfter(moment().subtract(11, "months").startOf('month'));

        case "last6months":
          if (hasNull) {
            return null;
          }

          date = values[0];
          return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) && moment(date, moment.ISO_8601).isAfter(moment().subtract(5, "months").startOf('month'));

        case "last3months":
          if (hasNull) {
            return null;
          }

          date = values[0];
          return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) && moment(date, moment.ISO_8601).isAfter(moment().subtract(2, "months").startOf('month'));

        case "future":
          if (hasNull) {
            return null;
          }

          date = values[0];
          return moment(date, moment.ISO_8601).isAfter(moment());

        case "notfuture":
          if (hasNull) {
            return null;
          }

          date = values[0];
          return !moment(date, moment.ISO_8601).isAfter(moment());

        case "current date":
          return moment().format("YYYY-MM-DD");

        case "current datetime":
          return moment().toISOString();

        case "latitude":
          if (hasNull) {
            return null;
          }

          point = values[0];

          if ((point != null ? point.type : void 0) === "Point") {
            return point.coordinates[1];
          }

          break;

        case "longitude":
          if (hasNull) {
            return null;
          }

          point = values[0];

          if ((point != null ? point.type : void 0) === "Point") {
            return point.coordinates[0];
          }

          break;

        case "distance":
          if (hasNull) {
            return null;
          }

          point1 = values[0];
          point2 = values[1];

          if ((point1 != null ? point1.type : void 0) === "Point" && (point2 != null ? point2.type : void 0) === "Point") {
            return getDistanceFromLatLngInM(point1.coordinates[1], point1.coordinates[0], point2.coordinates[1], point2.coordinates[0]);
          }

          break;

        case "line length":
          if (hasNull) {
            return null;
          }

          if (values[0].type !== "LineString") {
            return 0;
          }

          total = 0;
          coords = values[0].coordinates;

          for (i = j = 0, ref = coords.length - 1; 0 <= ref ? j < ref : j > ref; i = 0 <= ref ? ++j : --j) {
            total += getDistanceFromLatLngInM(coords[i][1], coords[i][0], coords[i + 1][1], coords[i + 1][0]);
          }

          return total;

        case "to text":
          if (hasNull) {
            return null;
          }

          if (this.schema) {
            exprUtils = new ExprUtils(this.schema);
            return exprUtils.stringifyExprLiteral(exprs[0], values[0], this.locale);
          } else {
            return values[0] + "";
          }

          break;

        default:
          throw new Error("Unknown op ".concat(op));
      }
    }
  }, {
    key: "evaluteAggrOp",
    value: function evaluteAggrOp(table, op, exprs, context, callback) {
      var _this3 = this;

      switch (op) {
        case "count":
          return callback(null, context.rows.length);

        case "sum":
          // Evaluate all rows
          return async.map(context.rows, function (row, cb) {
            return _this3.evaluate(exprs[0], {
              row: row
            }, cb);
          }, function (error, values) {
            if (error) {
              return callback(error);
            }

            return callback(null, _.sum(values));
          });

        case "avg":
          // Evaluate all rows
          return async.map(context.rows, function (row, cb) {
            return _this3.evaluate(exprs[0], {
              row: row
            }, cb);
          }, function (error, values) {
            if (error) {
              return callback(error);
            }

            return callback(null, _.sum(values) / values.length);
          });
        // TODO. Uses window functions, so returning 100 for now

        case "percent":
          return callback(null, 100);

        case "min":
          // Evaluate all rows
          return async.map(context.rows, function (row, cb) {
            return _this3.evaluate(exprs[0], {
              row: row
            }, cb);
          }, function (error, values) {
            if (error) {
              return callback(error);
            }

            return callback(null, _.min(values));
          });

        case "max":
          // Evaluate all rows
          return async.map(context.rows, function (row, cb) {
            return _this3.evaluate(exprs[0], {
              row: row
            }, cb);
          }, function (error, values) {
            if (error) {
              return callback(error);
            }

            return callback(null, _.max(values));
          });

        case "last":
          // Fail quietly if no ordering or no schema
          if (!this.schema || !this.schema.getTable(table).ordering) {
            console.warn("last does not work without schema and ordering");
            return callback(null, null);
          } // Evaluate all rows by ordering


          return async.map(context.rows, function (row, cb) {
            return row.getField(_this3.schema.getTable(table).ordering, cb);
          }, function (error, orderValues) {
            if (error) {
              return callback(error);
            } // Evaluate all rows


            return async.map(context.rows, function (row, cb) {
              return _this3.evaluate(exprs[0], {
                row: row
              }, cb);
            }, function (error, values) {
              var j, len, value, zipped;

              if (error) {
                return callback(error);
              }

              zipped = _.zip(values, orderValues); // Sort by ordering reverse

              zipped = _.sortByOrder(zipped, [function (entry) {
                return entry[1];
              }], ["desc"]);
              values = _.map(zipped, function (entry) {
                return entry[0];
              }); // Take first non-null

              for (j = 0, len = values.length; j < len; j++) {
                value = values[j];

                if (value != null) {
                  callback(null, value);
                  return;
                }
              }

              return callback(null, null);
            });
          });

        case "last where":
          // Fail quietly if no ordering or no schema
          if (!this.schema || !this.schema.getTable(table).ordering) {
            console.warn("last where does not work without schema and ordering");
            return callback(null, null);
          } // Evaluate all rows by ordering


          return async.map(context.rows, function (row, cb) {
            return row.getField(_this3.schema.getTable(table).ordering, cb);
          }, function (error, orderValues) {
            if (error) {
              return callback(error);
            } // Evaluate all rows by where


            return async.map(context.rows, function (row, cb) {
              return _this3.evaluate(exprs[1], {
                row: row
              }, cb);
            }, function (error, wheres) {
              if (error) {
                return callback(error);
              } // Evaluate all rows


              return async.map(context.rows, function (row, cb) {
                return _this3.evaluate(exprs[0], {
                  row: row
                }, cb);
              }, function (error, values) {
                var i, index, j, largest, len, ref, row;

                if (error) {
                  return callback(error);
                } // Find largest


                if (orderValues.length === 0) {
                  return callback(null, null);
                }

                index = -1;
                largest = null;
                ref = context.rows;

                for (i = j = 0, len = ref.length; j < len; i = ++j) {
                  row = ref[i];

                  if ((wheres[i] || !exprs[1]) && (index === -1 || orderValues[i] > largest) && values[i] != null) {
                    index = i;
                    largest = orderValues[i];
                  }
                }

                if (index >= 0) {
                  return callback(null, values[index]);
                } else {
                  return callback(null, null);
                }
              });
            });
          });

        case "previous":
          // Fail quietly if no ordering or no schema
          if (!this.schema || !this.schema.getTable(table).ordering) {
            console.warn("last does not work without schema and ordering");
            return callback(null, null);
          } // Evaluate all rows by ordering


          return async.map(context.rows, function (row, cb) {
            return row.getField(_this3.schema.getTable(table).ordering, cb);
          }, function (error, orderValues) {
            if (error) {
              return callback(error);
            } // Evaluate all rows


            return async.map(context.rows, function (row, cb) {
              return _this3.evaluate(exprs[0], {
                row: row
              }, cb);
            }, function (error, values) {
              var zipped;

              if (error) {
                return callback(error);
              }

              zipped = _.zip(values, orderValues); // Sort by ordering reverse

              zipped = _.sortByOrder(zipped, [function (entry) {
                return entry[1];
              }], ["desc"]);
              values = _.map(zipped, function (entry) {
                return entry[0];
              }); // Take second non-null

              values = _.filter(values, function (v) {
                return v != null;
              });

              if (values[1] != null) {
                callback(null, values[1]);
                return;
              }

              return callback(null, null);
            });
          });

        case "count where":
          // Evaluate all rows by where
          return async.map(context.rows, function (row, cb) {
            return _this3.evaluate(exprs[0], {
              row: row
            }, cb);
          }, function (error, wheres) {
            var count, j, len, where;

            if (error) {
              return callback(error);
            }

            count = 0;

            for (j = 0, len = wheres.length; j < len; j++) {
              where = wheres[j];

              if (where === true) {
                count += 1;
              }
            }

            return callback(null, count);
          });

        case "sum where":
          // Evaluate all rows
          return async.map(context.rows, function (row, cb) {
            return _this3.evaluate(exprs[0], {
              row: row
            }, cb);
          }, function (error, values) {
            if (error) {
              return callback(error);
            } // Evaluate all rows by where


            return async.map(context.rows, function (row, cb) {
              return _this3.evaluate(exprs[1], {
                row: row
              }, cb);
            }, function (error, wheres) {
              var i, j, len, ref, row, sum;

              if (error) {
                return callback(error);
              }

              sum = 0;
              ref = context.rows;

              for (i = j = 0, len = ref.length; j < len; i = ++j) {
                row = ref[i];

                if (wheres[i] === true) {
                  sum += values[i];
                }
              }

              return callback(null, sum);
            });
          });

        case "percent where":
          // Evaluate all rows
          return async.map(context.rows, function (row, cb) {
            return _this3.evaluate(exprs[0], {
              row: row
            }, cb);
          }, function (error, wheres) {
            if (error) {
              return callback(error);
            } // Evaluate all rows by of


            return async.map(context.rows, function (row, cb) {
              return _this3.evaluate(exprs[1], {
                row: row
              }, cb);
            }, function (error, ofs) {
              var count, i, j, len, ref, row, sum;

              if (error) {
                return callback(error);
              }

              sum = 0;
              count = 0;
              ref = context.rows;

              for (i = j = 0, len = ref.length; j < len; i = ++j) {
                row = ref[i];

                if (wheres[i] === true && (!exprs[1] || ofs[i] === true)) {
                  sum += 1;
                }

                if (!exprs[1] || ofs[i] === true) {
                  count += 1;
                }
              }

              if (count === 0) {
                return callback(null, null);
              } else {
                return callback(null, sum / count * 100);
              }
            });
          });

        case "min where":
          // Evaluate all rows
          return async.map(context.rows, function (row, cb) {
            return _this3.evaluate(exprs[0], {
              row: row
            }, cb);
          }, function (error, values) {
            if (error) {
              return callback(error);
            } // Evaluate all rows by where


            return async.map(context.rows, function (row, cb) {
              return _this3.evaluate(exprs[1], {
                row: row
              }, cb);
            }, function (error, wheres) {
              var i, items, j, len, ref, row, value;

              if (error) {
                return callback(error);
              }

              items = [];
              ref = context.rows;

              for (i = j = 0, len = ref.length; j < len; i = ++j) {
                row = ref[i];

                if (wheres[i] === true) {
                  items.push(values[i]);
                }
              }

              value = _.min(items);
              return callback(null, value != null ? value : null);
            });
          });

        case "max where":
          // Evaluate all rows
          return async.map(context.rows, function (row, cb) {
            return _this3.evaluate(exprs[0], {
              row: row
            }, cb);
          }, function (error, values) {
            if (error) {
              return callback(error);
            } // Evaluate all rows by where


            return async.map(context.rows, function (row, cb) {
              return _this3.evaluate(exprs[1], {
                row: row
              }, cb);
            }, function (error, wheres) {
              var i, items, j, len, ref, row, value;

              if (error) {
                return callback(error);
              }

              items = [];
              ref = context.rows;

              for (i = j = 0, len = ref.length; j < len; i = ++j) {
                row = ref[i];

                if (wheres[i] === true) {
                  items.push(values[i]);
                }
              }

              value = _.max(items);
              return callback(null, value != null ? value : null);
            });
          });

        case "count distinct":
          // Evaluate all rows 
          return async.map(context.rows, function (row, cb) {
            return _this3.evaluate(exprs[0], {
              row: row
            }, cb);
          }, function (error, values) {
            var count;

            if (error) {
              return callback(error);
            }

            count = _.uniq(values).length;
            return callback(null, count);
          });

        case "array_agg":
          // Evaluate all rows
          return async.map(context.rows, function (row, cb) {
            return _this3.evaluate(exprs[0], {
              row: row
            }, cb);
          }, function (error, values) {
            if (error) {
              return callback(error);
            }

            return callback(null, values);
          });

        default:
          return callback(new Error("Unknown op ".concat(op)));
      }
    }
  }, {
    key: "evaluateCase",
    value: function evaluateCase(expr, context, callback) {
      var _this4 = this;

      // Evaluate case whens and thens
      return async.map(expr.cases, function (acase, cb) {
        return _this4.evaluate(acase.when, context, cb);
      }, function (error, whens) {
        if (error) {
          return callback(error);
        }

        return async.map(expr.cases, function (acase, cb) {
          return _this4.evaluate(acase.then, context, cb);
        }, function (error, thens) {
          if (error) {
            return callback(error);
          }

          return _this4.evaluate(expr["else"], context, function (error, aelse) {
            var acase, i, j, len, ref;

            if (error) {
              return callback(error);
            }

            ref = expr.cases;

            for (i = j = 0, len = ref.length; j < len; i = ++j) {
              acase = ref[i];

              if (whens[i]) {
                return callback(null, thens[i]);
              }
            }

            return callback(null, aelse);
          });
        });
      });
    }
  }, {
    key: "evaluateScalar",
    value: function evaluateScalar(expr, context, callback) {
      var _this5 = this;

      // Null expression is null
      if (!expr.expr) {
        return callback(null, null);
      } // Follow joins


      return async.reduce(expr.joins, {
        row: context.row
      }, function (memo, join, cb) {
        // Memo is the context to apply the join to 
        // If multiple rows, get join for each and flatten
        if (memo.rows) {
          return async.map(memo.rows, function (row, cb2) {
            return row.getField(join, cb2);
          }, function (error, results) {
            if (error) {
              return cb(error);
            }

            if (results) {
              return cb(null, {
                rows: _.compact(_.flatten(results))
              });
            }
          });
        } else if (memo.row) {
          // Single row
          return memo.row.getField(join, function (error, result) {
            if (error) {
              return cb(error);
            }

            if (_.isArray(result)) {
              return cb(null, {
                rows: _.compact(result)
              });
            } else {
              return cb(null, {
                row: result
              });
            }
          });
        } else {
          return cb(null, {
            row: null
          });
        }
      }, function (error, exprContext) {
        if (error) {
          return callback(error);
        } // Null row and null/empty rows is null


        if (exprContext.row == null && (exprContext.rows == null || exprContext.rows.length === 0)) {
          return callback(null, null);
        }

        return _this5.evaluate(expr.expr, exprContext, callback);
      });
    }
  }, {
    key: "evaluateScore",
    value: function evaluateScore(expr, context, callback) {
      var _this6 = this;

      var sum; // Get input value

      if (!expr.input) {
        return callback(null, null);
      }

      sum = 0;
      return this.evaluate(expr.input, context, function (error, input) {
        var scorePairs;

        if (error) {
          return callback(error);
        }

        scorePairs = _.pairs(expr.scores);
        return async.map(scorePairs, function (scorePair, cb) {
          return _this6.evaluate(scorePair[1], context, cb);
        }, function (error, values) {
          var i, j, k, len, len1, scorePair, scoreValues, val;

          if (error) {
            return callback(error);
          }

          scoreValues = {};

          for (i = j = 0, len = scorePairs.length; j < len; i = ++j) {
            scorePair = scorePairs[i];
            scoreValues[scorePair[0]] = values[i];
          }

          if (_.isArray(input)) {
            for (k = 0, len1 = input.length; k < len1; k++) {
              val = input[k];

              if (scoreValues[val]) {
                sum += scoreValues[val];
              }
            }
          } else if (input) {
            if (scoreValues[input]) {
              sum += scoreValues[input];
            }
          }

          return callback(null, sum);
        });
      });
    }
  }, {
    key: "evaluateBuildEnumset",
    value: function evaluateBuildEnumset(expr, context, callback) {
      var _this7 = this;

      var valuePairs; // Evaluate each boolean

      valuePairs = _.pairs(expr.values);
      return async.map(valuePairs, function (valuePair, cb) {
        return _this7.evaluate(valuePair[1], context, cb);
      }, function (error, values) {
        var i, j, len, result, valuePair;

        if (error) {
          return callback(error);
        }

        result = [];

        for (i = j = 0, len = valuePairs.length; j < len; i = ++j) {
          valuePair = valuePairs[i];

          if (values[i]) {
            result.push(valuePair[0]);
          }
        }

        return callback(null, result);
      });
    }
  }, {
    key: "evaluateIsLatest",
    value: function evaluateIsLatest(table, exprs, context, callback) {
      var _this8 = this;

      // Fail quietly if no ordering or no schema
      if (!this.schema || !this.schema.getTable(table).ordering) {
        console.warn("evaluateIsLatest does not work without schema and ordering");
        return callback(null, false);
      } // Fail quietly if no rows


      if (!context.rows) {
        console.warn("evaluateIsLatest does not work without rows context");
        return callback(null, false);
      } // Evaluate lhs (value to group by) for all rows


      return async.map(context.rows, function (row, cb) {
        return _this8.evaluate(exprs[0], {
          row: row
        }, cb);
      }, function (error, lhss) {
        if (error) {
          return callback(error);
        } // Evaluate pk for all rows


        return async.map(context.rows, function (row, cb) {
          return row.getPrimaryKey(cb);
        }, function (error, pks) {
          if (error) {
            return callback(error);
          } // Evaluate ordering for all rows


          return async.map(context.rows, function (row, cb) {
            return row.getField(_this8.schema.getTable(table).ordering, cb);
          }, function (error, orderValues) {
            if (error) {
              return callback(error);
            } // Evaluate filter value for all rows if present


            return async.map(context.rows, function (row, cb) {
              return _this8.evaluate(exprs[1], {
                row: row
              }, cb);
            }, function (error, filters) {
              var groups, items, latests, lhs;

              if (error) {
                return callback(error);
              }

              items = _.map(lhss, function (lhs, index) {
                return {
                  lhs: lhs,
                  pk: pks[index],
                  ordering: orderValues[index],
                  filter: filters[index]
                };
              }); // Filter

              if (exprs[1]) {
                items = _.filter(items, function (item) {
                  return item.filter;
                });
              } // Group by lhs


              groups = _.groupBy(items, "lhs"); // Keep latest of each group

              latests = [];

              for (lhs in groups) {
                items = groups[lhs];
                latests.push(_.max(items, "ordering"));
              } // Get pk of row


              return context.row.getPrimaryKey(function (error, pk) {
                if (error) {
                  return callback(error);
                } // See if match


                return callback(null, indexOf.call(_.pluck(latests, "pk"), pk) >= 0);
              });
            });
          });
        });
      });
    }
  }, {
    key: "evaluateVariable",
    value: function evaluateVariable(expr, context, callback) {
      var value, variable;
      console.log(this.variables); // Get variable

      variable = _.findWhere(this.variables, {
        id: expr.variableId
      });

      if (!variable) {
        throw new Error("Variable ".concat(expr.variableId, " not found"));
      } // Get value


      value = this.variableValues[variable.id];

      if (value === void 0) {
        throw new Error("Variable ".concat(expr.variableId, " has no value"));
      } // If expression, compile


      if (variable.table) {
        return this.evaluate(value, context, callback);
      } else {
        return callback(null, value);
      }
    }
  }]);
  return ExprEvaluator;
}(); // From http://www.movable-type.co.uk/scripts/latlong.html


getDistanceFromLatLngInM = function getDistanceFromLatLngInM(lat1, lng1, lat2, lng2) {
  var R, a, c, d, dLat, dLng;
  R = 6370986; // Radius of the earth in m

  dLat = deg2rad(lat2 - lat1); // deg2rad below

  dLng = deg2rad(lng2 - lng1);
  a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
  c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  d = R * c; // Distance in m

  return d;
};

deg2rad = function deg2rad(deg) {
  return deg * (Math.PI / 180);
};