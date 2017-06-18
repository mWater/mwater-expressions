var ExprEvaluator, ExprUtils, _, async, deg2rad, getDistanceFromLatLngInM, moment;

_ = require('lodash');

moment = require('moment');

async = require('async');

ExprUtils = require('./ExprUtils');

module.exports = ExprEvaluator = (function() {
  function ExprEvaluator() {}

  ExprEvaluator.prototype.evaluate = function(expr, context, callback) {
    if (expr == null) {
      return callback(null, null);
    }
    switch (expr.type) {
      case "field":
        return context.row.getField(expr.column, (function(_this) {
          return function(error, value) {
            if (error) {
              return callback(error);
            }
            if (value && value.getPrimaryKey) {
              return value.getPrimaryKey(callback);
            }
            if (value && _.isArray(value) && value.length > 0 && value[0].getPrimaryKey) {
              async.map(value, (function(item, cb) {
                return item.getPrimaryKey(cb);
              }), function(error, ids) {
                return callback(error, ids);
              });
              return;
            }
            return callback(null, value);
          };
        })(this));
      case "literal":
        return callback(null, expr.value);
      case "op":
        return this.evaluateOp(expr.op, expr.exprs, context, callback);
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
      default:
        throw new Error("Unsupported expression type " + expr.type);
    }
  };

  ExprEvaluator.prototype.evaluateOp = function(op, exprs, context, callback) {
    if (ExprUtils.isOpAggr(op)) {
      return this.evaluteAggrOp(op, exprs, context, callback);
    }
    return async.map(exprs, ((function(_this) {
      return function(expr, cb) {
        return _this.evaluate(expr, context, cb);
      };
    })(this)), (function(_this) {
      return function(error, values) {
        var result;
        if (error) {
          return callback(error);
        }
        try {
          result = _this.evaluateOpValues(op, values);
          return callback(null, result);
        } catch (error1) {
          error = error1;
          return callback(error);
        }
      };
    })(this));
  };

  ExprEvaluator.prototype.evaluateOpValues = function(op, values) {
    var date, hasNull, point, point1, point2;
    hasNull = _.any(values, function(v) {
      return v == null;
    });
    switch (op) {
      case "+":
        if (hasNull) {
          return null;
        }
        return _.reduce(values, function(acc, value) {
          return acc + value;
        });
      case "*":
        if (hasNull) {
          return null;
        }
        return _.reduce(values, function(acc, value) {
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
        return _.reduce(values, function(acc, value) {
          return acc && value;
        });
      case "or":
        return _.reduce(values, function(acc, value) {
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
          return null;
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
      case "days since":
        if (hasNull) {
          return null;
        }
        return moment().diff(moment(values[0], moment.ISO_8601)) / 24 / 3600 / 1000;
      case "weekofmonth":
        if (hasNull) {
          return null;
        }
        return (Math.floor((moment(values[0], moment.ISO_8601).date() - 1) / 7) + 1) + "";
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
      case "to text":
        if (hasNull) {
          return null;
        }
        return values[0] + "";
      default:
        throw new Error("Unknown op " + op);
    }
  };

  ExprEvaluator.prototype.evaluteAggrOp = function(op, exprs, context, callback) {
    switch (op) {
      case "count":
        return callback(null, context.rows.length);
      case "sum":
        return async.map(context.rows, ((function(_this) {
          return function(row, cb) {
            return _this.evaluate(exprs[0], {
              row: row
            }, cb);
          };
        })(this)), (function(_this) {
          return function(error, values) {
            if (error) {
              return callback(error);
            }
            return callback(null, _.sum(values));
          };
        })(this));
      case "avg":
        return async.map(context.rows, ((function(_this) {
          return function(row, cb) {
            return _this.evaluate(exprs[0], {
              row: row
            }, cb);
          };
        })(this)), (function(_this) {
          return function(error, values) {
            if (error) {
              return callback(error);
            }
            return callback(null, _.sum(values) / values.length);
          };
        })(this));
      case "min":
        return async.map(context.rows, ((function(_this) {
          return function(row, cb) {
            return _this.evaluate(exprs[0], {
              row: row
            }, cb);
          };
        })(this)), (function(_this) {
          return function(error, values) {
            if (error) {
              return callback(error);
            }
            return callback(null, _.min(values));
          };
        })(this));
      case "max":
        return async.map(context.rows, ((function(_this) {
          return function(row, cb) {
            return _this.evaluate(exprs[0], {
              row: row
            }, cb);
          };
        })(this)), (function(_this) {
          return function(error, values) {
            if (error) {
              return callback(error);
            }
            return callback(null, _.max(values));
          };
        })(this));
      case "last":
        return async.map(context.rows, ((function(_this) {
          return function(row, cb) {
            return row.getOrdering(cb);
          };
        })(this)), (function(_this) {
          return function(error, orderValues) {
            if (error) {
              return callback(error);
            }
            return async.map(context.rows, (function(row, cb) {
              return _this.evaluate(exprs[0], {
                row: row
              }, cb);
            }), function(error, values) {
              var j, len, value, zipped;
              if (error) {
                return callback(error);
              }
              zipped = _.zip(values, orderValues);
              zipped = _.sortByOrder(zipped, [
                function(entry) {
                  return entry[1];
                }
              ], ["desc"]);
              values = _.map(zipped, function(entry) {
                return entry[0];
              });
              for (j = 0, len = values.length; j < len; j++) {
                value = values[j];
                if (value != null) {
                  callback(null, value);
                  return;
                }
              }
              return callback(null, null);
            });
          };
        })(this));
      case "last where":
        return async.map(context.rows, ((function(_this) {
          return function(row, cb) {
            return row.getOrdering(cb);
          };
        })(this)), (function(_this) {
          return function(error, ordering) {
            if (error) {
              return callback(error);
            }
            return async.map(context.rows, (function(row, cb) {
              return _this.evaluate(exprs[1], {
                row: row
              }, cb);
            }), function(error, wheres) {
              if (error) {
                return callback(error);
              }
              return async.map(context.rows, (function(row, cb) {
                return _this.evaluate(exprs[0], {
                  row: row
                }, cb);
              }), function(error, values) {
                var i, index, j, largest, len, ref, row;
                if (error) {
                  return callback(error);
                }
                if (ordering.length === 0) {
                  return callback(null, null);
                }
                index = -1;
                largest = null;
                ref = context.rows;
                for (i = j = 0, len = ref.length; j < len; i = ++j) {
                  row = ref[i];
                  if ((wheres[i] || !exprs[1]) && (index === -1 || ordering[i] > largest) && (values[i] != null)) {
                    index = i;
                    largest = ordering[i];
                  }
                }
                if (index >= 0) {
                  return callback(null, values[index]);
                } else {
                  return callback(null, null);
                }
              });
            });
          };
        })(this));
      case "count where":
        return async.map(context.rows, ((function(_this) {
          return function(row, cb) {
            return _this.evaluate(exprs[0], {
              row: row
            }, cb);
          };
        })(this)), (function(_this) {
          return function(error, wheres) {
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
          };
        })(this));
      case "sum where":
        return async.map(context.rows, ((function(_this) {
          return function(row, cb) {
            return _this.evaluate(exprs[0], {
              row: row
            }, cb);
          };
        })(this)), (function(_this) {
          return function(error, values) {
            if (error) {
              return callback(error);
            }
            return async.map(context.rows, (function(row, cb) {
              return _this.evaluate(exprs[1], {
                row: row
              }, cb);
            }), function(error, wheres) {
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
          };
        })(this));
      case "percent where":
        return async.map(context.rows, ((function(_this) {
          return function(row, cb) {
            return _this.evaluate(exprs[0], {
              row: row
            }, cb);
          };
        })(this)), (function(_this) {
          return function(error, wheres) {
            if (error) {
              return callback(error);
            }
            return async.map(context.rows, (function(row, cb) {
              return _this.evaluate(exprs[1], {
                row: row
              }, cb);
            }), function(error, ofs) {
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
          };
        })(this));
      case "count distinct":
        return async.map(context.rows, ((function(_this) {
          return function(row, cb) {
            return _this.evaluate(exprs[0], {
              row: row
            }, cb);
          };
        })(this)), (function(_this) {
          return function(error, values) {
            var count;
            if (error) {
              return callback(error);
            }
            count = _.uniq(values).length;
            return callback(null, count);
          };
        })(this));
      default:
        return callback(new Error("Unknown op " + op));
    }
  };

  ExprEvaluator.prototype.evaluateCase = function(expr, context, callback) {
    return async.map(expr.cases, ((function(_this) {
      return function(acase, cb) {
        return _this.evaluate(acase.when, context, cb);
      };
    })(this)), (function(_this) {
      return function(error, whens) {
        if (error) {
          return callback(error);
        }
        return async.map(expr.cases, (function(acase, cb) {
          return _this.evaluate(acase.then, context, cb);
        }), function(error, thens) {
          if (error) {
            return callback(error);
          }
          return _this.evaluate(expr["else"], context, function(error, aelse) {
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
      };
    })(this));
  };

  ExprEvaluator.prototype.evaluateScalar = function(expr, context, callback) {
    if (!expr.expr) {
      return callback(null, null);
    }
    return async.reduce(expr.joins, context, (function(_this) {
      return function(memo, join, cb) {
        if (memo.rows) {
          return async.map(memo.rows, (function(row, cb2) {
            return row.getField(join, cb2);
          }), function(error, results) {
            if (error) {
              return cb(error);
            }
            if (results) {
              return cb(null, {
                rows: _.flatten(results)
              });
            }
          });
        } else if (memo.row) {
          return memo.row.getField(join, function(error, result) {
            if (error) {
              return cb(error);
            }
            if (_.isArray(result)) {
              return cb(null, {
                rows: result
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
      };
    })(this), (function(_this) {
      return function(error, exprContext) {
        if (error) {
          return callback(error);
        }
        if ((exprContext.row == null) && ((exprContext.rows == null) || exprContext.rows.length === 0)) {
          return callback(null, null);
        }
        return _this.evaluate(expr.expr, exprContext, callback);
      };
    })(this));
  };

  ExprEvaluator.prototype.evaluateScore = function(expr, context, callback) {
    var sum;
    if (!expr.input) {
      return callback(null, null);
    }
    sum = 0;
    return this.evaluate(expr.input, context, (function(_this) {
      return function(error, input) {
        var scorePairs;
        if (error) {
          return callback(error);
        }
        scorePairs = _.pairs(expr.scores);
        return async.map(scorePairs, (function(scorePair, cb) {
          return _this.evaluate(scorePair[1], context, cb);
        }), function(error, values) {
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
      };
    })(this));
  };

  ExprEvaluator.prototype.evaluateBuildEnumset = function(expr, context, callback) {
    var valuePairs;
    valuePairs = _.pairs(expr.values);
    return async.map(valuePairs, ((function(_this) {
      return function(valuePair, cb) {
        return _this.evaluate(valuePair[1], context, cb);
      };
    })(this)), (function(_this) {
      return function(error, values) {
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
      };
    })(this));
  };

  return ExprEvaluator;

})();

getDistanceFromLatLngInM = function(lat1, lng1, lat2, lng2) {
  var R, a, c, d, dLat, dLng;
  R = 6370986;
  dLat = deg2rad(lat2 - lat1);
  dLng = deg2rad(lng2 - lng1);
  a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
  c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  d = R * c;
  return d;
};

deg2rad = function(deg) {
  return deg * (Math.PI / 180);
};
