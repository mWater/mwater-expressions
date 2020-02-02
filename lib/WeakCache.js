"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
var lodash_1 = __importDefault(require("lodash"));
/** Cache which can cache by an array of weak objects and an array of strong primitives and
 * objects.
 */
var WeakCache = /** @class */ (function () {
    function WeakCache() {
        this.cache = new WeakMap();
    }
    /** Gets a cache entry. Pass array of objects to hold weakly and optional strong array.
     * Returns undefined if not found. Must always be called with same length in each array.
     */
    WeakCache.prototype.get = function (weak, strong) {
        // Follow weak
        var value = this.cache;
        for (var _i = 0, weak_1 = weak; _i < weak_1.length; _i++) {
            var k = weak_1[_i];
            value = value.get(k);
            if (value === undefined) {
                return undefined;
            }
        }
        value = value.get(JSON.stringify(strong));
        return value;
    };
    /** Sets a cache entry. Pass array of objects to hold weakly and optional strong array.
     * Returns undefined if not found. Must always be called with same length in each array.
     */
    WeakCache.prototype.set = function (weak, strong, value) {
        // Follow weak
        var map = this.cache;
        for (var _i = 0, _a = lodash_1.default.initial(weak); _i < _a.length; _i++) {
            var k = _a[_i];
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
    };
    /** Looks up cache, and if not found, runs function and stores result */
    WeakCache.prototype.cacheFunction = function (weak, strong, func) {
        var value = this.get(weak, strong);
        if (value !== undefined) {
            return value;
        }
        value = func();
        this.set(weak, strong, value);
        return value;
    };
    return WeakCache;
}());
exports.WeakCache = WeakCache;
