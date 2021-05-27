"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.WeakCache = void 0;
const lodash_1 = __importDefault(require("lodash"));
/** Cache which can cache by an array of weak objects and an array of strong primitives and
 * objects.
 */
class WeakCache {
    constructor() {
        this.cache = new WeakMap();
    }
    /** Gets a cache entry. Pass array of objects to hold weakly and optional strong array.
     * Returns undefined if not found. Must always be called with same length in each array.
     */
    get(weak, strong) {
        // Follow weak
        let value = this.cache;
        for (const k of weak) {
            value = value.get(k);
            if (value === undefined) {
                return undefined;
            }
        }
        value = value.get(JSON.stringify(strong));
        return value;
    }
    /** Sets a cache entry. Pass array of objects to hold weakly and optional strong array.
     * Returns undefined if not found. Must always be called with same length in each array.
     */
    set(weak, strong, value) {
        // Follow weak
        let map = this.cache;
        for (const k of lodash_1.default.initial(weak)) {
            if (!map.has(k)) {
                map.set(k, new WeakMap());
            }
            map = map.get(k);
        }
        if (!map.has(lodash_1.default.last(weak))) {
            map.set(lodash_1.default.last(weak), new Map());
        }
        map = map.get(lodash_1.default.last(weak));
        map.set(JSON.stringify(strong), value);
    }
    /** Looks up cache, and if not found, runs function and stores result */
    cacheFunction(weak, strong, func) {
        let value = this.get(weak, strong);
        if (value !== undefined) {
            return value;
        }
        value = func();
        this.set(weak, strong, value);
        return value;
    }
}
exports.WeakCache = WeakCache;
