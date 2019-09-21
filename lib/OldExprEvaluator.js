"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var ExprEvaluator, _, deg2rad, getDistanceFromLatLngInM, moment;

_ = require('lodash');
moment = require('moment'); // Evaluates an expression given a row
// row is a plain object that has the following functions as properties:
//  getPrimaryKey() : returns primary key of row
//  getField(columnId) : gets the value of a column
// For joins, getField will return array of rows for 1-n and n-n joins and a row for n-1 and 1-1 joins

module.exports = ExprEvaluator =
/*#__PURE__*/
function () {
  function ExprEvaluator() {
    (0, _classCallCheck2["default"])(this, ExprEvaluator);
  }

  (0, _createClass2["default"])(ExprEvaluator, [{
    key: "evaluate",
    value: function evaluate(expr, row) {
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

        case "scalar":
          return this.evaluateScalar(expr, row);

        case "score":
          return this.evaluateScore(expr, row);

        default:
          throw new Error("Unsupported expression type ".concat(expr.type));
      }
    }
  }, {
    key: "evaluateOp",
    value: function evaluateOp(op, exprs, row) {
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

        case "round":
          return Math.round(this.evaluate(exprs[0], row));

        case "floor":
          return Math.floor(this.evaluate(exprs[0], row));

        case "ceiling":
          return Math.ceil(this.evaluate(exprs[0], row));

        case "days difference":
          return moment(this.evaluate(exprs[0], row), moment.ISO_8601).diff(moment(this.evaluate(exprs[1], row), moment.ISO_8601)) / 24 / 3600 / 1000;

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
    }
  }, {
    key: "evaluateCase",
    value: function evaluateCase(expr, row) {
      var acase, i, len, ref;
      ref = expr.cases;

      for (i = 0, len = ref.length; i < len; i++) {
        acase = ref[i];

        if (this.evaluate(acase.when, row)) {
          return this.evaluate(acase.then, row);
        }
      }

      return this.evaluate(expr["else"], row);
    }
  }, {
    key: "evaluateScalar",
    value: function evaluateScalar(expr, row) {
      var innerRow;

      if (expr.aggr) {
        throw new Error("Aggr not supported");
      }

      if (expr.joins.length > 1) {
        throw new Error("Multi-joins not supported");
      } // Get inner row


      innerRow = row.getField(expr.joins[0]);

      if (innerRow) {
        return this.evaluate(expr.expr, innerRow);
      } else {
        return null;
      }
    }
  }, {
    key: "evaluateScore",
    value: function evaluateScore(expr, row) {
      var i, input, len, sum, val; // Get input value

      if (!expr.input) {
        return null;
      }

      sum = 0;
      input = this.evaluate(expr.input, row);

      if (_.isArray(input)) {
        for (i = 0, len = input.length; i < len; i++) {
          val = input[i];

          if (expr.scores[val]) {
            sum += this.evaluate(expr.scores[val], row);
          }
        }
      } else if (input) {
        if (expr.scores[input]) {
          sum += this.evaluate(expr.scores[input], row);
        }
      }

      return sum;
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