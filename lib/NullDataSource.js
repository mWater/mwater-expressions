"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const DataSource_1 = __importDefault(require("./DataSource"));
/** Data source which always returns empty queries */
class NullDataSource extends DataSource_1.default {
    performQuery(query, cb) {
        if (cb) {
            cb(null, []);
            return;
        }
        else {
            return Promise.resolve([]);
        }
    }
}
exports.default = NullDataSource;
