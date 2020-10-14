/** Thrown when a column is not found when compiling an expression */
export default class ColumnNotFoundException extends Error {
    constructor(message: string);
}
