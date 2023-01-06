"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
// Fetches data for queries
class DataSource {
    performQuery(query, cb) {
        throw new Error("Not implemented");
    }
    /** Get the url to download an image (by id from an image or imagelist column)
      Height, if specified, is minimum height needed. May return larger image
    */
    getImageUrl(imageId, height) {
        throw new Error("Not implemented");
    }
    /** Get the url to upload an image (by id from an image or imagelist column)
      POST to upload
    */
    getImageUploadUrl(imageId) {
        throw new Error("Not implemented");
    }
    /** Clears the cache if possible with this data source */
    clearCache() {
        throw new Error("Not implemented");
    }
    /** Get the cache expiry time in ms from epoch. No cached items before this time will be used. 0 for no cache limit.
     * Useful for knowing when cache has been cleared, as it will be set to time of clearing.
     */
    getCacheExpiry() {
        throw new Error("Not implemented");
    }
    /** Get the url to download a file (by id from an file or filelist column). filename optionally overrides
     * the downloaded filename. GET to download
     */
    getFileUrl(fileId, filename) {
        throw new Error("Not implemented");
    }
    /** Get the url to upload an file (by id from an file or filelist column)
      POST to upload
    */
    getFileUploadUrl(fileId) {
        throw new Error("Not implemented");
    }
}
exports.default = DataSource;
