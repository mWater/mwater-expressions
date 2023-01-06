import { JsonQLQuery } from "jsonql";
import { Row } from "./types";
export default class DataSource {
    /** Performs a single query. Calls cb with (error, rows) or uses promise if no callback */
    performQuery(query: JsonQLQuery): Promise<Row[]>;
    performQuery(query: JsonQLQuery, cb: (error: any, rows: Row[]) => void): void;
    /** Get the url to download an image (by id from an image or imagelist column)
      Height, if specified, is minimum height needed. May return larger image
    */
    getImageUrl(imageId: string, height?: number): string;
    /** Get the url to upload an image (by id from an image or imagelist column)
      POST to upload
    */
    getImageUploadUrl(imageId: string): string;
    /** Clears the cache if possible with this data source */
    clearCache(): void;
    /** Get the cache expiry time in ms from epoch. No cached items before this time will be used. 0 for no cache limit.
     * Useful for knowing when cache has been cleared, as it will be set to time of clearing.
     */
    getCacheExpiry(): number;
    /** Get the url to download a file (by id from an file or filelist column). filename optionally overrides
     * the downloaded filename. GET to download
     */
    getFileUrl(fileId: string, filename?: string): string;
    /** Get the url to upload an file (by id from an file or filelist column)
      POST to upload
    */
    getFileUploadUrl(fileId: string): string;
}
