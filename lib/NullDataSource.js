"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _createClass2 = _interopRequireDefault(require("@babel/runtime/helpers/createClass"));

var _inherits2 = _interopRequireDefault(require("@babel/runtime/helpers/inherits"));

var _possibleConstructorReturn2 = _interopRequireDefault(require("@babel/runtime/helpers/possibleConstructorReturn"));

var _getPrototypeOf2 = _interopRequireDefault(require("@babel/runtime/helpers/getPrototypeOf"));

function _createSuper(Derived) { var hasNativeReflectConstruct = _isNativeReflectConstruct(); return function _createSuperInternal() { var Super = (0, _getPrototypeOf2["default"])(Derived), result; if (hasNativeReflectConstruct) { var NewTarget = (0, _getPrototypeOf2["default"])(this).constructor; result = Reflect.construct(Super, arguments, NewTarget); } else { result = Super.apply(this, arguments); } return (0, _possibleConstructorReturn2["default"])(this, result); }; }

function _isNativeReflectConstruct() { if (typeof Reflect === "undefined" || !Reflect.construct) return false; if (Reflect.construct.sham) return false; if (typeof Proxy === "function") return true; try { Date.prototype.toString.call(Reflect.construct(Date, [], function () {})); return true; } catch (e) { return false; } }

var DataSource, NullDataSource;
DataSource = require('./DataSource'); // Data source which always returns empty queries

module.exports = NullDataSource = /*#__PURE__*/function (_DataSource) {
  (0, _inherits2["default"])(NullDataSource, _DataSource);

  var _super = _createSuper(NullDataSource);

  function NullDataSource() {
    (0, _classCallCheck2["default"])(this, NullDataSource);
    return _super.apply(this, arguments);
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