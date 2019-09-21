"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var _possibleConstructorReturn2 = _interopRequireDefault(require("@babel/runtime/helpers/possibleConstructorReturn"));

var _getPrototypeOf2 = _interopRequireDefault(require("@babel/runtime/helpers/getPrototypeOf"));

var _inherits2 = _interopRequireDefault(require("@babel/runtime/helpers/inherits"));

var DataSource, NullDataSource;
DataSource = require('./DataSource'); // Data source which always returns empty queries

module.exports = NullDataSource =
/*#__PURE__*/
function (_DataSource) {
  (0, _inherits2["default"])(NullDataSource, _DataSource);

  function NullDataSource() {
    (0, _classCallCheck2["default"])(this, NullDataSource);
    return (0, _possibleConstructorReturn2["default"])(this, (0, _getPrototypeOf2["default"])(NullDataSource).apply(this, arguments));
  }

  (0, _createClass2["default"])(NullDataSource, [{
    key: "performQuery",
    // Performs a single query. Calls cb with rows
    value: function performQuery(query, cb) {
      return cb(null, []);
    }
  }]);
  return NullDataSource;
}(DataSource);