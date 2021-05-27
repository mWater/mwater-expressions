"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
/** Thrown when a column is not found when compiling an expression */
class ColumnNotFoundException extends Error {
    constructor(message) {
        super(message);
        this.name = this.constructor.name;
        this.message = message;
        this.stack = (new Error(message)).stack;
    }
}
exports.default = ColumnNotFoundException;
ColumnNotFoundException.prototype.constructor = ColumnNotFoundException;
