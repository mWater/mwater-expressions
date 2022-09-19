"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const lodash_1 = __importDefault(require("lodash"));
const DataSource_1 = __importDefault(require("./DataSource"));
const lru_cache_1 = __importDefault(require("lru-cache"));
const querystring_1 = __importDefault(require("querystring"));
const jquery_1 = __importDefault(require("jquery"));
/** Caching data source for mWater. Requires jQuery. require explicitly: require('mwater-expressions/lib/MWaterDataSource') */
class MWaterDataSource extends DataSource_1.default {
    /**
     * @param apiUrl
     * @param options serverCaching: allows server to send cached results. default true
     * localCaching allows local MRU cache. default true
     * imageApiUrl: overrides apiUrl for images
     */
    constructor(apiUrl, client, options = {}) {
        super();
        this.apiUrl = apiUrl;
        this.client = client;
        // cacheExpiry is time in ms from epoch that is oldest data that can be accepted. 0 = any (if serverCaching is true)
        this.cacheExpiry = 0;
        lodash_1.default.defaults(options, { serverCaching: true, localCaching: true });
        this.options = options;
        if (this.options.localCaching) {
            this.cache = new lru_cache_1.default({ max: 500, ttl: 1000 * 15 * 60 });
        }
    }
    performQuery(query, cb) {
        // If no callback, use promise
        let cacheKey, method;
        if (!cb) {
            return new Promise((resolve, reject) => {
                return this.performQuery(query, (error, rows) => {
                    if (error) {
                        return reject(error);
                    }
                    else {
                        return resolve(rows);
                    }
                });
            });
        }
        if (this.options.localCaching) {
            cacheKey = JSON.stringify(query);
            const cachedRows = this.cache.get(cacheKey);
            if (cachedRows) {
                cb(null, cachedRows);
                return;
            }
        }
        const queryParams = {};
        if (this.client) {
            queryParams.client = this.client;
        }
        const jsonqlStr = JSON.stringify(query);
        // Add as GET if short, POST otherwise
        if (jsonqlStr.length < 2000) {
            queryParams.jsonql = jsonqlStr;
            method = "GET";
        }
        else {
            method = "POST";
        }
        // Setup caching
        const headers = {};
        if (method === "GET") {
            if (!this.options.serverCaching) {
                // Using headers forces OPTIONS call, so use timestamp to disable caching
                // headers['Cache-Control'] = "no-cache"
                queryParams.ts = Date.now();
            }
            else if (this.cacheExpiry) {
                const seconds = Math.floor((new Date().getTime() - this.cacheExpiry) / 1000);
                headers["Cache-Control"] = `max-age=${seconds}`;
            }
        }
        // Create URL
        const url = this.apiUrl + "jsonql?" + querystring_1.default.stringify(queryParams);
        jquery_1.default.ajax({
            dataType: "json",
            method,
            url,
            headers,
            data: method === "POST" ? { jsonql: jsonqlStr } : undefined
        })
            .done((rows) => {
            if (this.options.localCaching) {
                // Cache rows
                this.cache.set(cacheKey, rows);
            }
            cb(null, rows);
        })
            .fail((xhr) => {
            cb(new Error(xhr.responseText));
        });
    }
    // Get the cache expiry time in ms from epoch. No cached items before this time will be used
    getCacheExpiry() {
        return this.cacheExpiry;
    }
    // Clears the local cache
    clearCache() {
        var _a;
        (_a = this.cache) === null || _a === void 0 ? void 0 : _a.reset();
        // Set new cache expiry
        this.cacheExpiry = new Date().getTime();
    }
    /** Get the url to download an image (by id from an image or imagelist column)
     * Height, if specified, is minimum height needed. May return larger image
     */
    getImageUrl(imageId, height) {
        const apiUrl = this.options.imageApiUrl || this.apiUrl;
        let url = apiUrl + `images/${imageId}`;
        const query = {};
        if (height) {
            query.h = height;
        }
        if (!lodash_1.default.isEmpty(query)) {
            url += "?" + querystring_1.default.stringify(query);
        }
        return url;
    }
    /** Get the url to upload an image (by id from an image or imagelist column)
      POST to upload
    */
    getImageUploadUrl(imageId) {
        const apiUrl = this.options.imageApiUrl || this.apiUrl;
        let url = apiUrl + `images/${imageId}`;
        const query = {};
        if (this.client) {
            query.client = this.client;
        }
        if (!lodash_1.default.isEmpty(query)) {
            url += "?" + querystring_1.default.stringify(query);
        }
        return url;
    }
}
exports.default = MWaterDataSource;
