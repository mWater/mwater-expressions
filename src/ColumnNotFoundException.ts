/** Thrown when a column is not found when compiling an expression */
export default class ColumnNotFoundException extends Error {
  constructor(message: string) {
    super(message)
    this.name = this.constructor.name
    this.message = message
    this.stack = (new Error(message)).stack
  }
}

ColumnNotFoundException.prototype.constructor = ColumnNotFoundException;
