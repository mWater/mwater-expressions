var ExprEvaluator, _, deg2rad, getDistanceFromLatLngInM, moment;

_ = require('lodash');

moment = require('moment');

module.exports = ExprEvaluator = (function() {
  function ExprEvaluator() {}

  ExprEvaluator.prototype.evaluate = function(expr, row) {
    if (expr == null) {
      return null;
    }
    switch (expr.type) {
      case "field":
        return row.getField(expr.column);
      case "literal":
        return expr.value;
      case "op":
        return this.evaluateOp(expr.op, expr.exprs, row);
      case "id":
        return row.getPrimaryKey();
      case "case":
        return this.evaluateCase(expr, row);
      default:
        throw new Error("Unsupported expression type " + expr.type);
    }
  };

  ExprEvaluator.prototype.evaluateOp = function(op, exprs, row) {
    var date, point, point1, point2;
    switch (op) {
      case "and":
        return this.evaluate(exprs[0], row) && this.evaluate(exprs[1], row);
      case "or":
        return this.evaluate(exprs[0], row) || this.evaluate(exprs[1], row);
      case "=":
        return this.evaluate(exprs[0], row) === this.evaluate(exprs[1], row);
      case "<>":
        return this.evaluate(exprs[0], row) !== this.evaluate(exprs[1], row);
      case ">":
        return this.evaluate(exprs[0], row) > this.evaluate(exprs[1], row);
      case ">=":
        return this.evaluate(exprs[0], row) >= this.evaluate(exprs[1], row);
      case "<":
        return this.evaluate(exprs[0], row) < this.evaluate(exprs[1], row);
      case "<=":
        return this.evaluate(exprs[0], row) <= this.evaluate(exprs[1], row);
      case "= false":
        return this.evaluate(exprs[0], row) === false;
      case "is null":
        return this.evaluate(exprs[0], row) == null;
      case "is not null":
        return this.evaluate(exprs[0], row) != null;
      case "~*":
        return this.evaluate(exprs[0], row).match(new RegExp(this.evaluate(exprs[1], row), "i")) != null;
      case "= any":
        return _.contains(this.evaluate(exprs[1], row), this.evaluate(exprs[0], row));
      case "between":
        return this.evaluate(exprs[0], row) >= this.evaluate(exprs[1], row) && this.evaluate(exprs[0], row) <= this.evaluate(exprs[2], row);
      case "today":
        date = this.evaluate(exprs[0], row);
        if (!date) {
          return false;
        }
        return moment(date, moment.ISO_8601).format("YYYY-MM-DD") === moment().format("YYYY-MM-DD");
      case "yesterday":
        date = this.evaluate(exprs[0], row);
        if (!date) {
          return false;
        }
        return moment(date, moment.ISO_8601).add(1, "days").format("YYYY-MM-DD") === moment().format("YYYY-MM-DD");
      case "thismonth":
        date = this.evaluate(exprs[0], row);
        if (!date) {
          return false;
        }
        return moment(date, moment.ISO_8601).format("YYYY-MM") === moment().format("YYYY-MM");
      case "lastmonth":
        date = this.evaluate(exprs[0], row);
        if (!date) {
          return false;
        }
        return moment(date, moment.ISO_8601).add(1, "months").format("YYYY-MM") === moment().format("YYYY-MM");
      case "thisyear":
        date = this.evaluate(exprs[0], row);
        if (!date) {
          return false;
        }
        return moment(date, moment.ISO_8601).format("YYYY") === moment().format("YYYY");
      case "lastyear":
        date = this.evaluate(exprs[0], row);
        if (!date) {
          return false;
        }
        return moment(date, moment.ISO_8601).add(1, "years").format("YYYY") === moment().format("YYYY");
      case "last7days":
        date = this.evaluate(exprs[0], row);
        if (!date) {
          return false;
        }
        return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) && moment(date, moment.ISO_8601).isAfter(moment().subtract(7, "days"));
      case "last30days":
        date = this.evaluate(exprs[0], row);
        if (!date) {
          return false;
        }
        return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) && moment(date, moment.ISO_8601).isAfter(moment().subtract(30, "days"));
      case "last365days":
        date = this.evaluate(exprs[0], row);
        if (!date) {
          return false;
        }
        return moment(date, moment.ISO_8601).isBefore(moment().add(1, "days")) && moment(date, moment.ISO_8601).isAfter(moment().subtract(365, "days"));
      case "latitude":
        point = this.evaluate(exprs[0], row);
        if ((point != null ? point.type : void 0) === "Point") {
          return point.coordinates[1];
        }
        break;
      case "longitude":
        point = this.evaluate(exprs[0], row);
        if ((point != null ? point.type : void 0) === "Point") {
          return point.coordinates[0];
        }
        break;
      case "distance":
        point1 = this.evaluate(exprs[0], row);
        point2 = this.evaluate(exprs[1], row);
        if ((point1 != null ? point1.type : void 0) === "Point" && (point2 != null ? point2.type : void 0) === "Point") {
          return getDistanceFromLatLngInM(point1.coordinates[1], point1.coordinates[0], point2.coordinates[1], point2.coordinates[0]);
        }
    }
  };

  ExprEvaluator.prototype.evaluateCase = function(expr, row) {
    var acase, i, len, ref;
    ref = expr.cases;
    for (i = 0, len = ref.length; i < len; i++) {
      acase = ref[i];
      if (this.evaluate(acase.when, row)) {
        return this.evaluate(acase.then, row);
      }
    }
    return this.evaluate(expr["else"], row);
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
