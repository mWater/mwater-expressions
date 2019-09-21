"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _classCallCheck2 = _interopRequireDefault(require("@babel/runtime/helpers/classCallCheck"));

var _possibleConstructorReturn2 = _interopRequireDefault(require("@babel/runtime/helpers/possibleConstructorReturn"));

var _getPrototypeOf2 = _interopRequireDefault(require("@babel/runtime/helpers/getPrototypeOf"));

var _inherits2 = _interopRequireDefault(require("@babel/runtime/helpers/inherits"));

var _wrapNativeSuper2 = _interopRequireDefault(require("@babel/runtime/helpers/wrapNativeSuper"));

// Thrown when a column is not found when compiling an expression
var ColumnNotFoundException;

module.exports = ColumnNotFoundException = function () {
  var ColumnNotFoundException =
  /*#__PURE__*/
  function (_Error) {
    (0, _inherits2["default"])(ColumnNotFoundException, _Error);

    function ColumnNotFoundException(message) {
      var _this;

      (0, _classCallCheck2["default"])(this, ColumnNotFoundException);
      _this = (0, _possibleConstructorReturn2["default"])(this, (0, _getPrototypeOf2["default"])(ColumnNotFoundException).call(this, message));
      _this.name = _this.constructor.name;
      _this.message = message;
      _this.stack = new Error(message).stack;
      return _this;
    }

    return ColumnNotFoundException;
  }((0, _wrapNativeSuper2["default"])(Error));

  ;
  ColumnNotFoundException.prototype.constructor = ColumnNotFoundException;
  return ColumnNotFoundException;
}.call(void 0);