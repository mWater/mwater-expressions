"use strict";
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (Object.prototype.hasOwnProperty.call(b, p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
/** Thrown when a column is not found when compiling an expression */
var ColumnNotFoundException = /** @class */ (function (_super) {
    __extends(ColumnNotFoundException, _super);
    function ColumnNotFoundException(message) {
        var _this = _super.call(this, message) || this;
        _this.name = _this.constructor.name;
        _this.message = message;
        _this.stack = (new Error(message)).stack;
        return _this;
    }
    return ColumnNotFoundException;
}(Error));
exports.default = ColumnNotFoundException;
ColumnNotFoundException.prototype.constructor = ColumnNotFoundException;
